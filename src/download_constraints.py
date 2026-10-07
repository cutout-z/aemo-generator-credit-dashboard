"""Download DISPATCHCONSTRAINT, GENCONDATA, and SPDCONNECTIONPOINTCONSTRAINT via NEMOSIS.

These tables allow mapping network constraints to specific generators,
showing which constraints most frequently curtail each generator.
"""

from __future__ import annotations

import io
import logging
import re
import time
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from nemosis import downloader as nemosis_downloader
from nemosis import dynamic_data_compiler
from nemosis.custom_errors import NoDataToReturn

from . import config

logger = logging.getLogger(__name__)

# GENCONDATA and SPDCONNECTIONPOINTCONSTRAINT change every month (each MMSDM
# archive carries that month's new constraints and versions). Their caches
# used to be reused forever, so constraints and connection points AEMO added
# after the first download were never seen (audit 2026-10-07, S2-5). The
# constraints lane re-pulls a cache older than this; it runs monthly, so in
# practice every run refreshes. NEMOSIS keeps the per-month raw files, so a
# refresh only downloads the archive months it does not already hold.
REFERENCE_MAX_AGE_DAYS = 25

# A pull, or a cache, whose newest EFFECTIVEDATE is this far behind today is
# missing monthly files: NEMOSIS logs a failed archive as a warning and
# compiles the rest. On 2026-10-06 the NAS cached a SPDCONNECTIONPOINTCONSTRAINT
# table ending 2018-11-30, and March 2026 mapped 64 of its 628 binding
# constraints. Such a table is never cached or used.
REFERENCE_MAX_LAG_DAYS = 183

# nemweb intermittently answers a run of archive requests with something that
# is not a zip; a burst of the same requests minutes later succeeds. Archive
# downloads for these tables retry: a 404 once (most are real: no archive
# before 2015, months to 2030 are requested), anything else up to three times.
DOWNLOAD_RETRY_DELAYS = (5, 20, 60)
_sleep = time.sleep
_FILE_MONTH = re.compile(r"(20\d{2})(\d{2})010000")


def _stale_reason(df: pd.DataFrame, table: str, today=None) -> str | None:
    """Why ``df`` cannot be a complete copy of ``table`` (None when it can)."""
    if df is None or df.empty or "EFFECTIVEDATE" not in df.columns:
        return f"{table}: no rows"
    newest = pd.to_datetime(df["EFFECTIVEDATE"]).max()
    today = pd.Timestamp(today or pd.Timestamp.now())
    if today.tzinfo is not None:
        today = today.tz_convert(None)
    cutoff = today.normalize() - pd.Timedelta(days=REFERENCE_MAX_LAG_DAYS)
    if pd.isna(newest) or newest < cutoff:
        return (f"{table}: newest EFFECTIVEDATE {newest:%Y-%m-%d} is more than "
                f"{REFERENCE_MAX_LAG_DAYS} days old; monthly files are missing")
    return None


def _download_unzip_csv_with_retry(url: str, down_load_to: str) -> None:
    """nemosis.downloader.download_unzip_csv, retried when nemweb sends no zip."""
    url = url.replace("#", "%23")
    m = _FILE_MONTH.search(url)
    future = bool(m) and (int(m.group(1)), int(m.group(2))) > (
        pd.Timestamp.now().year, pd.Timestamp.now().month)
    last = None
    for attempt in range(len(DOWNLOAD_RETRY_DELAYS) + 1):
        try:
            r = requests.get(url, headers=nemosis_downloader.USR_AGENT_HEADER, timeout=300)
            if r.status_code == 200 and r.content[:2] == b"PK":
                zipfile.ZipFile(io.BytesIO(r.content)).extractall(down_load_to)
                if attempt:
                    logger.info(f"Downloaded {url.rsplit('/', 1)[-1]} on retry {attempt}")
                return
            last = f"HTTP {r.status_code}, {r.headers.get('content-type', '?')}"
            not_found = r.status_code == 404
        except (requests.RequestException, zipfile.BadZipFile) as e:
            last, not_found = f"{type(e).__name__}: {e}", False
        if future or attempt >= len(DOWNLOAD_RETRY_DELAYS) or (not_found and attempt >= 1):
            break
        _sleep(DOWNLOAD_RETRY_DELAYS[attempt])
    raise zipfile.BadZipFile(f"{url.rsplit('/', 1)[-1]}: {last}")


@contextmanager
def _patient_downloads():
    """Route NEMOSIS's archive downloads through the retrying downloader."""
    original = nemosis_downloader.download_unzip_csv
    nemosis_downloader.download_unzip_csv = _download_unzip_csv_with_retry
    try:
        yield
    finally:
        nemosis_downloader.download_unzip_csv = original


def _compile(**kwargs) -> pd.DataFrame:
    with _patient_downloads():
        return dynamic_data_compiler(**kwargs)


GENCONDATA_CACHE = "gencondata.feather"
# All versions are kept (audit 2026-10, L8), so the cache has a new name: a
# spdcp_constraint.feather written by the old latest-pair dedupe is not
# read as if it carried version history.
SPDCP_CACHE = "spdcp_constraint_versions.feather"


def reference_cache_date(path: str | Path) -> str | None:
    """UTC date (YYYY-MM-DD) a reference cache file was last written, or None."""
    p = Path(path)
    if not p.exists():
        return None
    return datetime.fromtimestamp(p.stat().st_mtime, timezone.utc).strftime("%Y-%m-%d")


def _cache_age_days(path: Path, now: datetime | None = None) -> float:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    written = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    return (now - written).total_seconds() / 86400


def _reference_table(
    cache_path: Path,
    label: str,
    compile_fn,
    *,
    rebuild: bool,
    max_age_days: float | None,
    errors: list[str] | None,
    now: datetime | None = None,
) -> pd.DataFrame:
    """Cached reference table, re-pulled when missing, forced or older than ``max_age_days``.

    ``compile_fn()`` returns the processed frame (writing nothing). A failed or
    empty pull is never silent: the exception text goes into ``errors`` (the
    constraints lane's manifest error), and a stale cache, when one exists, is
    still used so the run keeps its mapping.
    """
    cached = cache_path.exists()
    stale = bool(cached and max_age_days is not None and _cache_age_days(cache_path, now) > max_age_days)
    incomplete = None
    if cached and not rebuild and not stale:
        frame = pd.read_feather(cache_path)
        incomplete = _stale_reason(frame, f"cached {label}", today=now)
        if incomplete is None:
            logger.info(f"Loading cached {label}")
            return frame
        logger.warning(f"{incomplete}; fetching it again")

    reason = ("forced" if rebuild and cached else "cache incomplete" if incomplete
              else "cache older than %s days" % max_age_days if stale else "no cache")
    logger.info(f"Fetching {label} ({reason})...")
    try:
        df = compile_fn()
        if df is None or df.empty:
            raise NoDataToReturn(f"{label} pull returned no rows")
        short = _stale_reason(df, f"{label} pull", today=now)
        if short:
            raise NoDataToReturn(short)
    except Exception as e:  # noqa: BLE001 — recorded, never swallowed
        msg = f"{label} fetch failed: {type(e).__name__}: {e}"
        logger.warning(msg)
        fallback = pd.DataFrame()
        if cached:
            fallback = pd.read_feather(cache_path)
            if _stale_reason(fallback, label, today=now):
                msg += " (the cache is incomplete too, so no mapping is used)"
                fallback = pd.DataFrame()
            else:
                msg += f" (using the cache written {reference_cache_date(cache_path)})"
        if errors is not None:
            errors.append(msg)
        return fallback
    df = df.reset_index(drop=True)
    df.to_feather(cache_path)
    return df


def fetch_binding_constraints_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download DISPATCHCONSTRAINT for a single month, filtered to binding only.

    Returns DataFrame with columns: SETTLEMENTDATE, CONSTRAINTID, MARGINALVALUE
    Pre-filtered to MARGINALVALUE > 0 (binding constraints) to reduce volume.

    NoDataToReturn (no archive for the month: unpublished, or NEMOSIS could not
    download it) is raised to the caller, which decides whether that month
    should have been published. Every other exception also propagates, so the
    caller records it in the lane error (audit 2026-10-07, S2-4) instead of a
    failed month reading as a quiet empty one.
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching DISPATCHCONSTRAINT for {year}-{month:02d}...")
    df = _compile(
        start_time=start_time,
        end_time=end_time,
        table_name="DISPATCHCONSTRAINT",
        raw_data_location=nemosis_cache,
        select_columns=[
            "SETTLEMENTDATE", "CONSTRAINTID", "MARGINALVALUE", "INTERVENTION",
        ],
        fformat="parquet",
        rebuild=rebuild,
    )

    if df is None or df.empty:
        return pd.DataFrame()

    # Filter to non-intervention, binding constraints only
    df["INTERVENTION"] = pd.to_numeric(df["INTERVENTION"], errors="coerce")
    df = df[df["INTERVENTION"] == 0].copy()
    df.drop(columns=["INTERVENTION"], inplace=True)

    df["MARGINALVALUE"] = pd.to_numeric(df["MARGINALVALUE"], errors="coerce")
    df = df[df["MARGINALVALUE"].abs() > 0].copy()

    df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"])

    logger.info(
        f"DISPATCHCONSTRAINT {year}-{month:02d}: {len(df):,} binding rows, "
        f"{df['CONSTRAINTID'].nunique()} unique constraints"
    )
    return df


def fetch_gencondata(
    cache_dir: str,
    rebuild: bool = False,
    *,
    max_age_days: float | None = None,
    errors: list[str] | None = None,
    now: datetime | None = None,
) -> pd.DataFrame:
    """GENCONDATA (constraint definitions with descriptions), latest version per constraint.

    Cached as feather. ``max_age_days`` re-pulls an older cache (the
    constraints lane passes REFERENCE_MAX_AGE_DAYS); a failed pull is
    appended to ``errors`` and falls back to the cache when there is one.
    """
    cache_path = Path(cache_dir) / GENCONDATA_CACHE
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")

    def compile_fn() -> pd.DataFrame:
        Path(nemosis_cache).mkdir(parents=True, exist_ok=True)
        df = _compile(
            start_time="2020/01/01 00:00:00",
            end_time="2030/01/01 00:00:00",
            table_name="GENCONDATA",
            raw_data_location=nemosis_cache,
            select_columns=[
                "GENCONID", "EFFECTIVEDATE", "VERSIONNO",
                "DESCRIPTION", "REASON", "LIMITTYPE",
            ],
            fformat="parquet",
            rebuild=rebuild,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        # Keep latest version per constraint
        df["EFFECTIVEDATE"] = pd.to_datetime(df["EFFECTIVEDATE"])
        df["VERSIONNO"] = pd.to_numeric(df["VERSIONNO"], errors="coerce")
        df = df.sort_values(["GENCONID", "EFFECTIVEDATE", "VERSIONNO"])
        df = df.drop_duplicates(subset=["GENCONID"], keep="last")
        logger.info(f"GENCONDATA: {len(df)} constraint definitions")
        return df

    return _reference_table(
        cache_path, "GENCONDATA", compile_fn,
        rebuild=rebuild, max_age_days=max_age_days, errors=errors, now=now,
    )


def spdcp_mapping_asof(spdcp: pd.DataFrame, asof) -> dict[str, set]:
    """{CONNECTIONPOINTID: {GENCONID}} for the constraint versions in force at ``asof``.

    For each constraint the version in force is its latest EFFECTIVEDATE on or
    before ``asof`` (highest VERSIONNO on that date); its connection points are
    the rows of that version only. A frame without version columns (an old
    cache) maps every row, as before.
    """
    if spdcp is None or spdcp.empty:
        return {}
    if not {"EFFECTIVEDATE", "VERSIONNO"} <= set(spdcp.columns):
        return spdcp.groupby("CONNECTIONPOINTID")["GENCONID"].apply(set).to_dict()
    df = spdcp.copy()
    df["EFFECTIVEDATE"] = pd.to_datetime(df["EFFECTIVEDATE"]).astype("datetime64[ns]")
    df = df[df["EFFECTIVEDATE"] <= pd.Timestamp(asof)]
    if df.empty:
        return {}
    df["VERSIONNO"] = pd.to_numeric(df["VERSIONNO"], errors="coerce")
    latest = (
        df[["GENCONID", "EFFECTIVEDATE", "VERSIONNO"]]
        .drop_duplicates()
        .sort_values(["GENCONID", "EFFECTIVEDATE", "VERSIONNO"])
        .drop_duplicates(subset=["GENCONID"], keep="last")
    )
    current = df.merge(latest, on=["GENCONID", "EFFECTIVEDATE", "VERSIONNO"], how="inner")
    return current.groupby("CONNECTIONPOINTID")["GENCONID"].apply(set).to_dict()


def fetch_spdconnectionpointconstraint(
    cache_dir: str,
    rebuild: bool = False,
    *,
    max_age_days: float | None = None,
    errors: list[str] | None = None,
    now: datetime | None = None,
) -> pd.DataFrame:
    """SPDCONNECTIONPOINTCONSTRAINT (constraint-to-connection-point mapping), every version.

    Same cache, refresh and error rules as :func:`fetch_gencondata`.
    """
    cache_path = Path(cache_dir) / SPDCP_CACHE
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")

    def compile_fn() -> pd.DataFrame:
        Path(nemosis_cache).mkdir(parents=True, exist_ok=True)
        df = _compile(
            start_time="2020/01/01 00:00:00",
            end_time="2030/01/01 00:00:00",
            table_name="SPDCONNECTIONPOINTCONSTRAINT",
            raw_data_location=nemosis_cache,
            select_columns=[
                "CONNECTIONPOINTID", "EFFECTIVEDATE", "VERSIONNO",
                "GENCONID", "FACTOR", "BIDTYPE",
            ],
            fformat="parquet",
            rebuild=rebuild,
        )
        if df is None or df.empty:
            return pd.DataFrame()

        # Filter to ENERGY bid type (most relevant for generation)
        df = df[df["BIDTYPE"].str.upper() == "ENERGY"].copy()

        # Keep EVERY version: which connection points a constraint covers is
        # resolved per month (spdcp_mapping_asof). The old dedupe kept the latest
        # row per (connection point, constraint) pair, so a connection point that a
        # later version dropped stayed mapped to that constraint forever.
        df["EFFECTIVEDATE"] = pd.to_datetime(df["EFFECTIVEDATE"])
        df["VERSIONNO"] = pd.to_numeric(df["VERSIONNO"], errors="coerce")
        df = df.drop_duplicates(
            subset=["CONNECTIONPOINTID", "GENCONID", "EFFECTIVEDATE", "VERSIONNO"], keep="last",
        ).sort_values(["GENCONID", "EFFECTIVEDATE", "VERSIONNO", "CONNECTIONPOINTID"])

        logger.info(
            f"SPDCONNECTIONPOINTCONSTRAINT: {len(df)} mappings, "
            f"{df['CONNECTIONPOINTID'].nunique()} connection points"
        )
        return df

    return _reference_table(
        cache_path, "SPDCONNECTIONPOINTCONSTRAINT", compile_fn,
        rebuild=rebuild, max_age_days=max_age_days, errors=errors, now=now,
    )
