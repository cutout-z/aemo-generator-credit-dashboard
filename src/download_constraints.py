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
from pathlib import Path

import pandas as pd
import requests
from nemosis import downloader as nemosis_downloader
from nemosis import dynamic_data_compiler
from nemosis.custom_errors import NoDataToReturn

from . import config

logger = logging.getLogger(__name__)


def fetch_binding_constraints_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download DISPATCHCONSTRAINT for a single month, filtered to binding only.

    Returns DataFrame with columns: SETTLEMENTDATE, CONSTRAINTID, MARGINALVALUE
    Pre-filtered to MARGINALVALUE > 0 (binding constraints) to reduce volume.
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching DISPATCHCONSTRAINT for {year}-{month:02d}...")
    try:
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
    except (NoDataToReturn, Exception) as e:
        logger.warning(f"No DISPATCHCONSTRAINT for {year}-{month:02d}: {e}")
        return pd.DataFrame()

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




# nemweb intermittently answers a run of archive requests with something that
# is not a zip. NEMOSIS logs each such file as "not downloaded" and compiles
# what it has, so the 2026-10-06/07 constraints runs got
# SPDCONNECTIONPOINTCONSTRAINT tables ending 2018-11 and 2022-08 while a fast
# burst of the same requests a minute later all succeeded. The constraint
# tables are therefore downloaded with retries: a 404 is retried once (these
# tables have no archive before 2015, and the run asks for months up to 2030,
# so most 404s are real), anything else up to three times with backoff.
DOWNLOAD_RETRY_DELAYS = (5, 20, 60)
_sleep = time.sleep
_FILE_MONTH = re.compile(r"(20\d{2})(\d{2})010000")


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

# A NEMOSIS compile that lost its later monthly files (nemweb throttles long
# runs of requests; each failed file is only a warning) still returns rows,
# just old ones: on 2026-10-06 the NAS cached a SPDCONNECTIONPOINTCONSTRAINT
# table whose newest EFFECTIVEDATE was 2018-11-30, so 564 of March 2026's 628
# binding constraints mapped to no connection point. Both reference tables
# change every month, so data this far behind today means files are missing.
REFERENCE_MAX_AGE_DAYS = 183


def _stale_reason(df: pd.DataFrame, table: str, today=None) -> str | None:
    """Why ``df`` cannot be a complete copy of ``table`` (None when it can)."""
    if df is None or df.empty or "EFFECTIVEDATE" not in df.columns:
        return f"{table}: no rows"
    newest = pd.to_datetime(df["EFFECTIVEDATE"]).max()
    cutoff = pd.Timestamp(today or pd.Timestamp.now()).normalize() - pd.Timedelta(days=REFERENCE_MAX_AGE_DAYS)
    if pd.isna(newest) or newest < cutoff:
        return (f"{table}: newest EFFECTIVEDATE {newest:%Y-%m-%d} is more than "
                f"{REFERENCE_MAX_AGE_DAYS} days old; monthly files are missing")
    return None

def fetch_gencondata(cache_dir: str, rebuild: bool = False) -> pd.DataFrame:
    """Download GENCONDATA (constraint definitions with descriptions).

    This is a slowly-changing reference table. Cache as feather.
    """
    cache_path = Path(cache_dir) / "gencondata.feather"
    if cache_path.exists() and not rebuild:
        cached = pd.read_feather(cache_path)
        reason = _stale_reason(cached, "cached GENCONDATA")
        if reason is None:
            logger.info("Loading cached GENCONDATA")
            return cached
        logger.warning(f"{reason}; fetching it again")

    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    logger.info("Fetching GENCONDATA...")
    try:
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
    except (NoDataToReturn, Exception) as e:
        logger.warning(f"Failed to fetch GENCONDATA: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # Keep latest version per constraint
    df["EFFECTIVEDATE"] = pd.to_datetime(df["EFFECTIVEDATE"])
    df["VERSIONNO"] = pd.to_numeric(df["VERSIONNO"], errors="coerce")
    df = df.sort_values(["GENCONID", "EFFECTIVEDATE", "VERSIONNO"])
    df = df.drop_duplicates(subset=["GENCONID"], keep="last")

    reason = _stale_reason(df, "GENCONDATA")
    if reason:
        logger.error(f"{reason}; not cached or used")
        return pd.DataFrame()
    logger.info(f"GENCONDATA: {len(df)} constraint definitions")
    df.to_feather(cache_path)
    return df


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
    cache_dir: str, rebuild: bool = False,
) -> pd.DataFrame:
    """Download SPDCONNECTIONPOINTCONSTRAINT (constraint-to-connection-point mapping).

    This maps connection points to the constraints that affect them.
    """
    # All versions are kept (audit 2026-10, L8), so the cache has a new name: a
    # spdcp_constraint.feather written by the old latest-pair dedupe is not
    # read as if it carried version history.
    cache_path = Path(cache_dir) / "spdcp_constraint_versions.feather"
    if cache_path.exists() and not rebuild:
        cached = pd.read_feather(cache_path)
        reason = _stale_reason(cached, "cached SPDCONNECTIONPOINTCONSTRAINT")
        if reason is None:
            logger.info("Loading cached SPDCONNECTIONPOINTCONSTRAINT")
            return cached
        logger.warning(f"{reason}; fetching it again")

    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    logger.info("Fetching SPDCONNECTIONPOINTCONSTRAINT...")
    try:
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
    except (NoDataToReturn, Exception) as e:
        logger.warning(f"Failed to fetch SPDCONNECTIONPOINTCONSTRAINT: {e}")
        return pd.DataFrame()

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

    reason = _stale_reason(df, "SPDCONNECTIONPOINTCONSTRAINT")
    if reason:
        logger.error(f"{reason}; not cached or used")
        return pd.DataFrame()
    logger.info(
        f"SPDCONNECTIONPOINTCONSTRAINT: {len(df)} mappings, "
        f"{df['CONNECTIONPOINTID'].nunique()} connection points"
    )
    df.to_feather(cache_path)
    return df
