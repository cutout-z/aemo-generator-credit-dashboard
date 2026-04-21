"""Download WEM pre-Reform facility SCADA data from old AEMO portal.

Source: http://data.wa.aemo.com.au/datafiles/facility-scada/
Format: monthly CSVs, 30-minute intervals (Interval Number 1-48 per trading day)
Columns: Trading Date, Interval Number, Trading Interval, Participant Code,
         Facility Code, Energy Generated (MWh), EOI Quantity (MW)

Coverage: Sep 2006 – Oct 2023 (pre-Reform only)
Energy is already in MWh — no MW × interval_duration conversion required.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)


def fetch_wem_scada_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Fetch WEM SCADA for one month, caching as a feather file.

    Returns DataFrame with columns:
      Facility Code (str), Trading Date (date str), Interval Number (int),
      Trading Interval (str), Energy Generated (MWh) (float)
    Returns empty DataFrame if the month is outside the available range or on error.
    """
    # Guard: only pre-Reform data is available
    start_y, start_m = config.WEM_DATA_START
    end_y, end_m = config.WEM_DATA_END
    if (year, month) < (start_y, start_m) or (year, month) > (end_y, end_m):
        logger.debug(f"SCADA {year}-{month:02d} outside pre-Reform range — skipping")
        return pd.DataFrame()

    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    feather_path = cache_path / f"scada_{year:04d}_{month:02d}.feather"

    if feather_path.exists() and not rebuild:
        return pd.read_feather(feather_path)

    url = config.SCADA_URL_TEMPLATE.format(year=year, month=month)
    logger.info(f"Downloading WEM SCADA {year}-{month:02d}...")

    raw = _download_csv(url)
    if raw is None:
        return pd.DataFrame()

    df = _parse_scada(raw, year, month)
    if df.empty:
        return df

    df.to_feather(feather_path)
    logger.info(f"SCADA {year}-{month:02d}: {len(df):,} rows ({df['Facility Code'].nunique()} facilities)")
    return df


def _parse_scada(raw: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Normalise raw SCADA CSV into a clean DataFrame."""
    raw.columns = raw.columns.str.strip()

    col_map = _flexible_col_map(raw.columns, {
        "Facility Code": ["facility code", "facilitycode"],
        "Trading Date": ["trading date", "tradingdate"],
        "Interval Number": ["interval number", "intervalnumber"],
        "Trading Interval": ["trading interval", "tradinginterval"],
        "Energy Generated (MWh)": ["energy generated (mwh)", "energy generated"],
        "EOI Quantity (MW)": ["eoi quantity (mw)", "eoi quantity"],
    })
    raw = raw.rename(columns=col_map)

    required = {"Facility Code", "Energy Generated (MWh)"}
    missing = required - set(raw.columns)
    if missing:
        logger.warning(f"SCADA {year}-{month:02d}: missing columns {missing}")
        return pd.DataFrame()

    df = raw.copy()
    df["Facility Code"] = df["Facility Code"].astype(str).str.strip()
    df["Energy Generated (MWh)"] = pd.to_numeric(df["Energy Generated (MWh)"], errors="coerce").fillna(0.0)

    if "Interval Number" in df.columns:
        df["Interval Number"] = pd.to_numeric(df["Interval Number"], errors="coerce")

    if "Trading Date" in df.columns:
        df["Trading Date"] = df["Trading Date"].astype(str).str.strip()

    # Drop rows with no Facility Code or clearly invalid data
    df = df[df["Facility Code"].str.len() > 0]
    df = df[df["Energy Generated (MWh)"].notna()]

    keep = ["Facility Code", "Trading Date", "Interval Number", "Energy Generated (MWh)"]
    if "Trading Interval" in df.columns:
        keep.append("Trading Interval")
    keep = [c for c in keep if c in df.columns]

    return df[keep].reset_index(drop=True)


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
            return pd.read_csv(StringIO(resp.text), low_memory=False)
        except Exception as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"SCADA download failed ({url}): {e}. Retry in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"SCADA unavailable ({url}): {e}")
                return None
