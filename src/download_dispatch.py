"""Download DISPATCHPRICE (regional RRP) via NEMOSIS.

Downloads one month at a time. Returns 5-minute regional spot prices
for revenue reconstruction and price capture calculation.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from nemosis import dynamic_data_compiler

from . import config

logger = logging.getLogger(__name__)


def fetch_dispatch_price_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download DISPATCHPRICE for a single month via NEMOSIS.

    Returns DataFrame with columns: SETTLEMENTDATE, REGIONID, RRP
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching DISPATCHPRICE for {year}-{month:02d}...")
    prices = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="DISPATCHPRICE",
        raw_data_location=nemosis_cache,
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP", "INTERVENTION"],
        fformat="parquet",
        rebuild=rebuild,
    )

    if prices is None or prices.empty:
        logger.warning(f"No DISPATCHPRICE data for {year}-{month:02d}")
        return pd.DataFrame()

    # Filter non-intervention pricing
    prices["INTERVENTION"] = pd.to_numeric(prices["INTERVENTION"], errors="coerce")
    prices = prices[prices["INTERVENTION"] == 0].copy()
    prices.drop(columns=["INTERVENTION"], inplace=True)

    prices["SETTLEMENTDATE"] = pd.to_datetime(prices["SETTLEMENTDATE"])
    prices["RRP"] = pd.to_numeric(prices["RRP"], errors="coerce")
    prices = prices.dropna(subset=["RRP"])

    # Deduplicate (NEMOSIS can return duplicates across file boundaries)
    prices = prices.drop_duplicates(subset=["SETTLEMENTDATE", "REGIONID"])

    logger.info(f"DISPATCHPRICE {year}-{month:02d}: {len(prices):,} rows")
    return prices
