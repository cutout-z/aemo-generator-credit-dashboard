"""Download DISPATCH_UNIT_SCADA and DISPATCHLOAD via NEMOSIS.

Downloads one month at a time to keep memory manageable. Returns raw
5-minute interval data for aggregation.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from nemosis import dynamic_data_compiler

from . import config

logger = logging.getLogger(__name__)


def fetch_scada_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download DISPATCH_UNIT_SCADA for a single month via NEMOSIS.

    Returns DataFrame with columns: SETTLEMENTDATE, DUID, SCADAVALUE
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching DISPATCH_UNIT_SCADA for {year}-{month:02d}...")
    scada = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="DISPATCH_UNIT_SCADA",
        raw_data_location=nemosis_cache,
        select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
        fformat="parquet",
        rebuild=rebuild,
    )

    if scada is None or scada.empty:
        logger.warning(f"No SCADA data for {year}-{month:02d}")
        return pd.DataFrame()

    scada["SETTLEMENTDATE"] = pd.to_datetime(scada["SETTLEMENTDATE"])
    scada["SCADAVALUE"] = pd.to_numeric(scada["SCADAVALUE"], errors="coerce")
    scada = scada.dropna(subset=["SCADAVALUE"])

    logger.info(f"SCADA {year}-{month:02d}: {len(scada):,} rows, "
                f"{scada['DUID'].nunique()} DUIDs")
    return scada


def fetch_dispatchload_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download DISPATCHLOAD (AVAILABILITY) for a single month via NEMOSIS.

    Returns DataFrame with columns: SETTLEMENTDATE, DUID, AVAILABILITY
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching DISPATCHLOAD for {year}-{month:02d}...")
    dispatch = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="DISPATCHLOAD",
        raw_data_location=nemosis_cache,
        select_columns=["SETTLEMENTDATE", "DUID", "AVAILABILITY", "INTERVENTION"],
        fformat="parquet",
        rebuild=rebuild,
    )

    if dispatch is None or dispatch.empty:
        logger.warning(f"No DISPATCHLOAD data for {year}-{month:02d}")
        return pd.DataFrame()

    dispatch["INTERVENTION"] = pd.to_numeric(dispatch["INTERVENTION"], errors="coerce")
    dispatch = dispatch[dispatch["INTERVENTION"] == 0].copy()
    dispatch.drop(columns=["INTERVENTION"], inplace=True)

    dispatch["SETTLEMENTDATE"] = pd.to_datetime(dispatch["SETTLEMENTDATE"])
    dispatch["AVAILABILITY"] = pd.to_numeric(dispatch["AVAILABILITY"], errors="coerce")
    dispatch = dispatch.dropna(subset=["AVAILABILITY"])

    logger.info(f"DISPATCHLOAD {year}-{month:02d}: {len(dispatch):,} rows, "
                f"{dispatch['DUID'].nunique()} DUIDs")
    return dispatch
