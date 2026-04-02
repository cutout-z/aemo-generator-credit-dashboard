"""Download DISPATCHCONSTRAINT, GENCONDATA, and SPDCONNECTIONPOINTCONSTRAINT via NEMOSIS.

These tables allow mapping network constraints to specific generators,
showing which constraints most frequently curtail each generator.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
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


def fetch_gencondata(cache_dir: str, rebuild: bool = False) -> pd.DataFrame:
    """Download GENCONDATA (constraint definitions with descriptions).

    This is a slowly-changing reference table. Cache as feather.
    """
    cache_path = Path(cache_dir) / "gencondata.feather"
    if cache_path.exists() and not rebuild:
        logger.info("Loading cached GENCONDATA")
        return pd.read_feather(cache_path)

    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    logger.info("Fetching GENCONDATA...")
    try:
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

    logger.info(f"GENCONDATA: {len(df)} constraint definitions")
    df.to_feather(cache_path)
    return df


def fetch_spdconnectionpointconstraint(
    cache_dir: str, rebuild: bool = False,
) -> pd.DataFrame:
    """Download SPDCONNECTIONPOINTCONSTRAINT (constraint-to-connection-point mapping).

    This maps connection points to the constraints that affect them.
    """
    cache_path = Path(cache_dir) / "spdcp_constraint.feather"
    if cache_path.exists() and not rebuild:
        logger.info("Loading cached SPDCONNECTIONPOINTCONSTRAINT")
        return pd.read_feather(cache_path)

    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    logger.info("Fetching SPDCONNECTIONPOINTCONSTRAINT...")
    try:
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
    except (NoDataToReturn, Exception) as e:
        logger.warning(f"Failed to fetch SPDCONNECTIONPOINTCONSTRAINT: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # Filter to ENERGY bid type (most relevant for generation)
    df = df[df["BIDTYPE"].str.upper() == "ENERGY"].copy()

    # Keep latest version per (connection_point, constraint)
    df["EFFECTIVEDATE"] = pd.to_datetime(df["EFFECTIVEDATE"])
    df["VERSIONNO"] = pd.to_numeric(df["VERSIONNO"], errors="coerce")
    df = df.sort_values(["CONNECTIONPOINTID", "GENCONID", "EFFECTIVEDATE", "VERSIONNO"])
    df = df.drop_duplicates(subset=["CONNECTIONPOINTID", "GENCONID"], keep="last")

    logger.info(
        f"SPDCONNECTIONPOINTCONSTRAINT: {len(df)} mappings, "
        f"{df['CONNECTIONPOINTID'].nunique()} connection points"
    )
    df.to_feather(cache_path)
    return df
