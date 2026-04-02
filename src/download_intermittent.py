"""Download INTERMITTENT_GEN_SCADA via NEMOSIS.

This table provides quality flags for intermittent generators (solar/wind)
that allow separating grid curtailment from mechanical downtime.

Available from AEMO NEMWeb "Current" reports from ~Aug 2024 onwards.
Data is published per-day (next-day basis) and rolls off after ~60 days.
We cache aggressively so incremental runs accumulate history over time.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from nemosis import dynamic_data_compiler
from nemosis.custom_errors import NoDataToReturn

from . import config

logger = logging.getLogger(__name__)


def fetch_intermittent_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download INTERMITTENT_GEN_SCADA for a single month via NEMOSIS.

    Returns DataFrame with columns: SETTLEMENTDATE, DUID, SCADA_TYPE,
    SCADA_VALUE, SCADA_QUALITY.

    Returns empty DataFrame for months before INTERMITTENT_SCADA_START
    or if data is unavailable.
    """
    if (year, month) < config.INTERMITTENT_SCADA_START:
        return pd.DataFrame()

    # Check for cached monthly feather first
    cache_path = Path(cache_dir) / f"intermittent_{year}_{month:02d}.feather"
    if cache_path.exists() and not rebuild:
        logger.info(f"Loading cached INTERMITTENT_GEN_SCADA for {year}-{month:02d}")
        return pd.read_feather(cache_path)

    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching INTERMITTENT_GEN_SCADA for {year}-{month:02d}...")
    try:
        df = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="INTERMITTENT_GEN_SCADA",
            raw_data_location=nemosis_cache,
            select_columns=[
                "RUN_DATETIME", "DUID", "SCADA_TYPE",
                "SCADA_VALUE", "SCADA_QUALITY",
            ],
            fformat="parquet",
            rebuild=rebuild,
        )
    except (NoDataToReturn, Exception) as e:
        logger.warning(f"No INTERMITTENT_GEN_SCADA for {year}-{month:02d}: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        logger.warning(f"No INTERMITTENT_GEN_SCADA data for {year}-{month:02d}")
        return pd.DataFrame()

    df["RUN_DATETIME"] = pd.to_datetime(df["RUN_DATETIME"])
    df["SCADA_VALUE"] = pd.to_numeric(df["SCADA_VALUE"], errors="coerce")
    df = df.rename(columns={"RUN_DATETIME": "SETTLEMENTDATE"})

    logger.info(
        f"INTERMITTENT_GEN_SCADA {year}-{month:02d}: {len(df):,} rows, "
        f"{df['DUID'].nunique()} DUIDs, "
        f"quality: {df['SCADA_QUALITY'].value_counts().to_dict()}"
    )

    # Cache as feather for future runs
    df.to_feather(cache_path)
    return df
