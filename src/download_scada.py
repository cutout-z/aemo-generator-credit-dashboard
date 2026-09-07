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
    """Download DISPATCHLOAD (AVAILABILITY, UIGF) for a single month via NEMOSIS.

    Returns DataFrame with columns: SETTLEMENTDATE, DUID, AVAILABILITY, UIGF.

    UIGF (unconstrained intermittent generation forecast) is requested for a
    future measured-curtailed-energy methodology (S3-04), not for the current
    shortfall proxy. Historical schema boundary: UIGF is populated only for
    intermittent (semi-scheduled) units — other unit types report no value —
    and older MMS archive vintages may not carry the column at all, so the
    output tolerates its absence (column is NaN-filled rather than fatal).
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching DISPATCHLOAD for {year}-{month:02d}...")
    uigf_requested = True
    try:
        dispatch = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="DISPATCHLOAD",
            raw_data_location=nemosis_cache,
            select_columns=["SETTLEMENTDATE", "DUID", "AVAILABILITY", "INTERVENTION", "UIGF"],
            fformat="parquet",
            rebuild=rebuild,
        )
    except Exception as exc:
        # Historical schema boundary: UIGF availability depends on the raw
        # archive vintage AND the installed nemosis version (its table spec
        # may not know the column). Retry without it rather than failing the
        # monthly lane — UIGF is a pass-through for the future S3-04
        # methodology, not required by the current proxy metric.
        logger.warning(
            f"DISPATCHLOAD fetch with UIGF failed for {year}-{month:02d} "
            f"({type(exc).__name__}: {exc}); retrying without UIGF"
        )
        uigf_requested = False
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

    # UIGF: informational pass-through (future methodology input). Absent on
    # historical archive vintages -> NaN column; only intermittent units
    # report values.
    if "UIGF" in dispatch.columns:
        dispatch["UIGF"] = pd.to_numeric(dispatch["UIGF"], errors="coerce")
        uigf_duids = dispatch.loc[dispatch["UIGF"].notna(), "DUID"].nunique()
    else:
        dispatch["UIGF"] = float("nan")
        uigf_duids = 0

    logger.info(f"DISPATCHLOAD {year}-{month:02d}: {len(dispatch):,} rows, "
                f"{dispatch['DUID'].nunique()} DUIDs, {uigf_duids} with UIGF"
                + ("" if uigf_requested else " (UIGF unavailable — schema boundary)"))
    return dispatch
