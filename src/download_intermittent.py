"""Download INTERMITTENT_GEN_SCADA quality summaries.

This table provides quality flags for intermittent generators (solar/wind)
that allow separating grid curtailment from mechanical downtime.

Two data sources:
1. MMSDM Archive (Dec 2024+): monthly zip files, reliable for backfill
2. NEMWeb "Current" page: daily files, rolling ~60 days, for recent data

The downloader tries the archive first (faster, one file per month), then
falls back to NEMOSIS/Current for months not yet in the archive.

The dashboard only needs, per DUID/month, the count of ELAV intervals and how
many were quality="Good". Cache that small summary instead of full monthly
INTERMITTENT_GEN_SCADA extracts, which can be hundreds of MB per month.
"""

from __future__ import annotations

import io
import logging
import tempfile
import time
import zipfile
from pathlib import Path

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

# Archive availability start - data before this is not in the MMSDM archive.
_ARCHIVE_START = (2024, 12)

_COLUMNS = ["DUID", "SCADA_TYPE", "SCADA_QUALITY"]


def fetch_intermittent_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download INTERMITTENT_GEN_SCADA quality summary for a single month.

    Tries MMSDM archive first (Dec 2024+), then NEMOSIS/Current as fallback.
    Returns DataFrame with columns: DUID, total_intervals, good_intervals.
    """
    if (year, month) < config.INTERMITTENT_SCADA_START:
        return pd.DataFrame()

    cache_path = Path(cache_dir) / f"intermittent_quality_{year}_{month:02d}.feather"
    if cache_path.exists() and not rebuild:
        logger.info(f"Loading cached INTERMITTENT_GEN_SCADA quality summary for {year}-{month:02d}")
        return pd.read_feather(cache_path)

    # One-time migration path for older full-table caches.
    legacy_cache = Path(cache_dir) / f"intermittent_{year}_{month:02d}.feather"
    if legacy_cache.exists() and not rebuild:
        logger.info(f"Summarising legacy INTERMITTENT_GEN_SCADA cache for {year}-{month:02d}")
        summary = _summarise_quality(pd.read_feather(legacy_cache), year, month, "legacy-cache")
        if not summary.empty:
            summary.to_feather(cache_path)
        return summary

    df = pd.DataFrame()
    if (year, month) >= _ARCHIVE_START:
        df = _fetch_from_archive(year, month)

    if df.empty:
        df = _fetch_from_nemosis(year, month, cache_dir, rebuild)

    if df.empty:
        return df

    df.to_feather(cache_path)
    return df


def _fetch_from_archive(year: int, month: int) -> pd.DataFrame:
    """Download from MMSDM archive and count ELAV quality by DUID."""
    url = _ARCHIVE_URL.format(year=year, month=month)
    logger.info(f"Fetching INTERMITTENT_GEN_SCADA from MMSDM archive for {year}-{month:02d}...")

    for attempt in range(config.MAX_RETRIES):
        try:
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
                    bytes_read = 0
                    next_log_at = 50 * 1024 * 1024
                    for chunk in resp.iter_content(chunk_size=8192):
                        tmp.write(chunk)
                        bytes_read += len(chunk)
                        if bytes_read >= next_log_at:
                            logger.info(
                                "Downloaded %.0f MB of INTERMITTENT_GEN_SCADA archive for %s-%02d...",
                                bytes_read / 1_048_576, year, month,
                            )
                            next_log_at += 50 * 1024 * 1024

            try:
                with zipfile.ZipFile(tmp_path) as zf:
                    csv_names = [n for n in zf.namelist() if n.endswith(".CSV") or n.endswith(".csv")]
                    if not csv_names:
                        logger.warning(f"No CSV found in archive zip for {year}-{month:02d}")
                        return pd.DataFrame()

                    with zf.open(csv_names[0]) as csvfile:
                        text_stream = io.TextIOWrapper(csvfile, encoding="utf-8", errors="replace")

                        header_line = None
                        for line in text_stream:
                            if line.startswith("I,"):
                                header_line = line.rstrip("\n\r")
                                break

                        if not header_line:
                            logger.warning(f"No header row in archive for {year}-{month:02d}")
                            return pd.DataFrame()

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

                        total_by_duid: dict[str, int] = {}
                        good_by_duid: dict[str, int] = {}
                        rows_read = 0
                        for line in text_stream:
                            if not line.startswith("D,"):
                                continue
                            parts = line.split(",")
                            if any(idx >= len(parts) for idx in col_indices.values()):
                                continue

                            scada_type = parts[col_indices["SCADA_TYPE"]].strip().strip('"')
                            if scada_type != "ELAV":
                                continue

                            duid = parts[col_indices["DUID"]].strip().strip('"')
                            quality = parts[col_indices["SCADA_QUALITY"]].strip().strip('"')
                            total_by_duid[duid] = total_by_duid.get(duid, 0) + 1
                            if quality == "Good":
                                good_by_duid[duid] = good_by_duid.get(duid, 0) + 1

                            rows_read += 1
                            if rows_read % 1_000_000 == 0:
                                logger.info(
                                    "Parsed %s INTERMITTENT_GEN_SCADA ELAV rows for %s-%02d...",
                                    f"{rows_read:,}", year, month,
                                )

                        if not total_by_duid:
                            logger.warning(f"No ELAV data rows in archive for {year}-{month:02d}")
                            return pd.DataFrame()

                        df = pd.DataFrame(
                            {
                                "DUID": list(total_by_duid.keys()),
                                "total_intervals": list(total_by_duid.values()),
                                "good_intervals": [
                                    good_by_duid.get(duid, 0) for duid in total_by_duid
                                ],
                            }
                        )
            finally:
                Path(tmp_path).unlink(missing_ok=True)

            _log_summary(df, year, month, "archive")
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

    return _summarise_quality(df, year, month, "nemosis")


def _summarise_quality(df: pd.DataFrame, year: int, month: int, source: str) -> pd.DataFrame:
    """Reduce INTERMITTENT_GEN_SCADA rows to ELAV quality counts by DUID."""
    required = {"DUID", "SCADA_TYPE", "SCADA_QUALITY"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        logger.warning(f"Cannot summarise INTERMITTENT_GEN_SCADA; missing columns: {missing}")
        return pd.DataFrame()

    elav = df[df["SCADA_TYPE"] == "ELAV"]
    if elav.empty:
        return pd.DataFrame()

    summary = (
        elav.assign(is_good=elav["SCADA_QUALITY"] == "Good")
        .groupby("DUID", as_index=False)
        .agg(
            total_intervals=("SCADA_QUALITY", "size"),
            good_intervals=("is_good", "sum"),
        )
    )
    summary["good_intervals"] = summary["good_intervals"].astype(int)
    _log_summary(summary, year, month, source)
    return summary


def _log_summary(df: pd.DataFrame, year: int, month: int, source: str) -> None:
    logger.info(
        f"INTERMITTENT_GEN_SCADA quality {year}-{month:02d} ({source}): "
        f"{len(df):,} DUIDs, {int(df['total_intervals'].sum()):,} ELAV intervals"
    )
