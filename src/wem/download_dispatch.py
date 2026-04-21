"""Download WEM balancing price from pre-Reform annual CSV files.

Source: http://data.wa.aemo.com.au/datafiles/balancing-summary/
Format: annual CSVs with 30-minute interval prices (Interval Number 1-48 per day)
Key column: Final Price ($/MWh) — WEM equivalent of NEM's Regional Reference Price

Coverage: 2012–2023 (aligned with revenue-calculation scope of Jul 2012 – Sep 2023)

Annual files are small (~50-200KB each). We download the full year on first access
and cache it as a feather, then slice per-month during aggregation.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)


def fetch_wem_price_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Return balancing price data for one month.

    Returns DataFrame with columns:
      Trading Date (str), Interval Number (int), Final Price ($/MWh) (float)
    Returns empty DataFrame if outside available range or on error.
    """
    start_y, start_m = config.WEM_DATA_START
    end_y, end_m = config.WEM_DATA_END
    if (year, month) < (start_y, start_m) or (year, month) > (end_y, end_m):
        return pd.DataFrame()

    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    # Monthly cache path
    month_feather = cache_path / f"price_{year:04d}_{month:02d}.feather"
    if month_feather.exists() and not rebuild:
        return pd.read_feather(month_feather)

    # Fetch the full-year CSV (cached as feather)
    annual = _fetch_annual_prices(year, cache_path, rebuild)
    if annual.empty:
        return pd.DataFrame()

    # Slice to this month
    month_df = _slice_month(annual, year, month)
    if not month_df.empty:
        month_df.to_feather(month_feather)
    return month_df


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _fetch_annual_prices(year: int, cache_path: Path, rebuild: bool) -> pd.DataFrame:
    """Download and cache the annual balancing summary CSV for a given year."""
    annual_feather = cache_path / f"balancing_summary_{year:04d}.feather"

    if annual_feather.exists() and not rebuild:
        return pd.read_feather(annual_feather)

    url = config.BALANCING_URL_TEMPLATE.format(year=year)
    logger.info(f"Downloading WEM balancing summary {year}...")

    raw = _download_csv(url)
    if raw is None:
        return pd.DataFrame()

    df = _parse_balancing_summary(raw, year)
    if not df.empty:
        df.to_feather(annual_feather)
        logger.info(f"Balancing summary {year}: {len(df):,} rows cached")
    return df


def _parse_balancing_summary(raw: pd.DataFrame, year: int) -> pd.DataFrame:
    """Normalise raw balancing summary CSV."""
    raw.columns = raw.columns.str.strip()

    col_map = _flexible_col_map(raw.columns, {
        "Trading Date": ["trading date", "tradingdate", "date"],
        "Interval Number": ["interval number", "intervalnumber", "trading interval", "tradinginterval"],
        "Final Price ($/MWh)": [
            "final price ($/mwh)", "final price", "balancing price ($/mwh)",
            "balancing price", "price ($/mwh)", "price",
        ],
    })
    raw = raw.rename(columns=col_map)

    required = {"Final Price ($/MWh)"}
    missing = required - set(raw.columns)
    if missing:
        logger.warning(f"Balancing summary {year}: missing columns {missing} — available: {list(raw.columns)}")
        return pd.DataFrame()

    df = raw.copy()
    df["Final Price ($/MWh)"] = pd.to_numeric(df["Final Price ($/MWh)"], errors="coerce")
    df = df.dropna(subset=["Final Price ($/MWh)"])

    if "Interval Number" in df.columns:
        df["Interval Number"] = pd.to_numeric(df["Interval Number"], errors="coerce")

    if "Trading Date" in df.columns:
        df["Trading Date"] = df["Trading Date"].astype(str).str.strip()

    keep = [c for c in ["Trading Date", "Interval Number", "Final Price ($/MWh)"] if c in df.columns]
    return df[keep].reset_index(drop=True)


def _slice_month(annual: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Filter an annual price DataFrame to rows matching year-month."""
    if "Trading Date" not in annual.columns:
        return pd.DataFrame()

    prefix = f"{year:04d}-{month:02d}"
    mask = annual["Trading Date"].astype(str).str.startswith(prefix)
    result = annual[mask].copy().reset_index(drop=True)

    if result.empty:
        logger.warning(f"No price data for {year}-{month:02d} in annual file")
    return result


def _flexible_col_map(columns: pd.Index, target_map: dict[str, list[str]]) -> dict[str, str]:
    result = {}
    lower_cols = {c: c.lower().strip() for c in columns}
    for target, candidates in target_map.items():
        for orig, lower in lower_cols.items():
            if any(c == lower for c in candidates):
                if target not in result.values():
                    result[orig] = target
                break
    return result


def _download_csv(url: str) -> pd.DataFrame | None:
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                headers={"User-Agent": config.USER_AGENT},
            )
            resp.raise_for_status()
            from io import StringIO
            return pd.read_csv(StringIO(resp.text))
        except Exception as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Price download failed ({url}): {e}. Retry in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"Price unavailable ({url}): {e}")
                return None
