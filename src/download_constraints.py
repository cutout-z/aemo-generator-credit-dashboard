"""Download DISPATCHCONSTRAINT, GENCONDATA, and SPDCONNECTIONPOINTCONSTRAINT via NEMOSIS.

These tables allow mapping network constraints to specific generators,
showing which constraints most frequently curtail each generator.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
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
    if cached and not rebuild and not stale:
        logger.info(f"Loading cached {label}")
        return pd.read_feather(cache_path)

    reason = "forced" if rebuild and cached else ("cache older than %s days" % max_age_days if stale else "no cache")
    logger.info(f"Fetching {label} ({reason})...")
    try:
        df = compile_fn()
        if df is None or df.empty:
            raise NoDataToReturn(f"{label} pull returned no rows")
    except Exception as e:  # noqa: BLE001 — recorded, never swallowed
        msg = f"{label} fetch failed: {type(e).__name__}: {e}"
        logger.warning(msg)
        if cached:
            msg += f" (using the cache written {reference_cache_date(cache_path)})"
        if errors is not None:
            errors.append(msg)
        return pd.read_feather(cache_path) if cached else pd.DataFrame()
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
    df = dynamic_data_compiler(
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
        df = dynamic_data_compiler(
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
        df = dynamic_data_compiler(
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
