"""Download INTERMITTENT_GEN_SCADA data.

This table provides quality flags for intermittent generators (solar/wind)
that allow separating grid curtailment from mechanical downtime.

Two data sources:
1. MMSDM Archive (Dec 2024+): monthly zip files, reliable for backfill
2. NEMWeb "Current" page: daily files, rolling ~60 days, for recent data

The downloader tries the archive first (faster, one file per month), then
falls back to NEMOSIS/Current for months not yet in the archive.
"""

from __future__ import annotations

import csv
import io
import logging
import tempfile
import time
import zipfile
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from nemosis import dynamic_data_compiler
from nemosis.custom_errors import NoDataToReturn

from . import config

logger = logging.getLogger(__name__)

# MMSDM archive URL pattern for INTERMITTENT_GEN_SCADA (available Dec 2024+)
_ARCHIVE_URL = (
    "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
    "{year:04d}/MMSDM_{year:04d}_{month:02d}/"
    "MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_ARCHIVE%23INTERMITTENT_GEN_SCADA%23FILE01%23"
    "{year:04d}{month:02d}010000.zip"
)

# Archive availability start — data before this isn't in the MMSDM archive
_ARCHIVE_START = (2024, 12)

_COLUMNS = ["RUN_DATETIME", "DUID", "SCADA_TYPE", "SCADA_VALUE", "SCADA_QUALITY"]


def fetch_intermittent_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download INTERMITTENT_GEN_SCADA for a single month.

    Tries MMSDM archive first (Dec 2024+), then NEMOSIS/Current as fallback.
    Returns DataFrame with columns: SETTLEMENTDATE, DUID, SCADA_TYPE,
    SCADA_VALUE, SCADA_QUALITY.
    """
    if (year, month) < config.INTERMITTENT_SCADA_START:
        return pd.DataFrame()

    # Check for cached monthly feather first
    cache_path = Path(cache_dir) / f"intermittent_{year}_{month:02d}.feather"
    if cache_path.exists() and not rebuild:
        logger.info(f"Loading cached INTERMITTENT_GEN_SCADA for {year}-{month:02d}")
        return pd.read_feather(cache_path)

    # Try MMSDM archive first (Dec 2024+)
    df = pd.DataFrame()
    if (year, month) >= _ARCHIVE_START:
        df = _fetch_from_archive(year, month, cache_dir)

    # Fall back to NEMOSIS/Current for recent data or pre-archive months
    if df.empty:
        df = _fetch_from_nemosis(year, month, cache_dir, rebuild)

    if df.empty:
        return df

    # Cache as feather for future runs
    df.to_feather(cache_path)
    return df


def _fetch_from_archive(year: int, month: int, cache_dir: str) -> pd.DataFrame:
    """Download from MMSDM monthly archive zip.

    Streams the zip to a temp file and parses the CSV line-by-line to keep
    memory usage low (these files can be 100 MB+ compressed).
    """
    url = _ARCHIVE_URL.format(year=year, month=month)
    logger.info(f"Fetching INTERMITTENT_GEN_SCADA from MMSDM archive for {year}-{month:02d}...")

    for attempt in range(config.MAX_RETRIES):
        try:
            # Stream download to a temp file instead of loading into memory
            with requests.get(
                url, timeout=config.REQUEST_TIMEOUT, stream=True,
                headers={"User-Agent": config.USER_AGENT},
            ) as resp:
                if resp.status_code == 404:
                    logger.info(f"INTERMITTENT_GEN_SCADA not in archive for {year}-{month:02d}")
                    return pd.DataFrame()
                resp.raise_for_status()

                with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                    tmp_path = tmp.name
                    for chunk in resp.iter_content(chunk_size=8192):
                        tmp.write(chunk)

            # Extract and parse from the temp file on disk
            try:
                with zipfile.ZipFile(tmp_path) as zf:
                    csv_names = [n for n in zf.namelist() if n.endswith(".CSV") or n.endswith(".csv")]
                    if not csv_names:
                        logger.warning(f"No CSV found in archive zip for {year}-{month:02d}")
                        return pd.DataFrame()

                    # Read line-by-line to find header, then parse data rows
                    with zf.open(csv_names[0]) as csvfile:
                        text_stream = io.TextIOWrapper(csvfile, encoding="utf-8", errors="replace")

                        # Scan for header row (starts with "I,")
                        header_line = None
                        for line in text_stream:
                            if line.startswith("I,"):
                                header_line = line.rstrip("\n\r")
                                break

                        if not header_line:
                            logger.warning(f"No header row in archive for {year}-{month:02d}")
                            return pd.DataFrame()

                        # Parse header to find column indices
                        headers = header_line.split(",")
                        col_indices = {}
                        for col in _COLUMNS:
                            for i, h in enumerate(headers):
                                if h.strip().upper() == col:
                                    col_indices[col] = i
                                    break

                        if len(col_indices) < len(_COLUMNS):
                            missing = set(_COLUMNS) - set(col_indices.keys())
                            logger.warning(f"Missing columns in archive: {missing}")
                            return pd.DataFrame()

                        # Parse data rows line-by-line into column lists (much
                        # more memory-efficient than list-of-dicts)
                        col_data = {col: [] for col in _COLUMNS}
                        for line in text_stream:
                            if not line.startswith("D,"):
                                continue
                            parts = line.split(",")
                            for col, idx in col_indices.items():
                                if idx < len(parts):
                                    col_data[col].append(parts[idx].strip().strip('"'))
                                else:
                                    col_data[col].append(None)

                        if not col_data[_COLUMNS[0]]:
                            logger.warning(f"No data rows in archive for {year}-{month:02d}")
                            return pd.DataFrame()

                        df = pd.DataFrame(col_data)
            finally:
                Path(tmp_path).unlink(missing_ok=True)

            df = _clean_dataframe(df, year, month, "archive")
            return df

        except requests.RequestException as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Archive download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                logger.warning(f"Could not download from archive: {e}")
                return pd.DataFrame()

    return pd.DataFrame()


def _fetch_from_nemosis(
    year: int, month: int, cache_dir: str, rebuild: bool,
) -> pd.DataFrame:
    """Fall back to NEMOSIS (NEMWeb Current page) for recent data."""
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching INTERMITTENT_GEN_SCADA from NEMWeb Current for {year}-{month:02d}...")
    try:
        df = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="INTERMITTENT_GEN_SCADA",
            raw_data_location=nemosis_cache,
            select_columns=_COLUMNS,
            fformat="parquet",
            rebuild=rebuild,
        )
    except (NoDataToReturn, Exception) as e:
        logger.warning(f"NEMOSIS fallback failed for {year}-{month:02d}: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    return _clean_dataframe(df, year, month, "nemosis")


def _clean_dataframe(df: pd.DataFrame, year: int, month: int, source: str) -> pd.DataFrame:
    """Normalize column types and rename RUN_DATETIME → SETTLEMENTDATE."""
    df["RUN_DATETIME"] = pd.to_datetime(df["RUN_DATETIME"])
    df["SCADA_VALUE"] = pd.to_numeric(df["SCADA_VALUE"], errors="coerce")
    df = df.rename(columns={"RUN_DATETIME": "SETTLEMENTDATE"})

    logger.info(
        f"INTERMITTENT_GEN_SCADA {year}-{month:02d} ({source}): {len(df):,} rows, "
        f"{df['DUID'].nunique()} DUIDs"
    )
    return df
