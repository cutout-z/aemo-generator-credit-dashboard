"""Download MLF history from AEMO DUDETAILSUMMARY and extract per-FY MLFs.

Adapted from AEMO MLF Tracker project (src/download.py + src/analyse.py).
"""

from __future__ import annotations

import csv
import io
import logging
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

DUDETAILSUMMARY_COLUMNS = [
    "DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "CONNECTIONPOINTID",
    "REGIONID", "STATIONID", "PARTICIPANTID", "LASTCHANGED",
    "TRANSMISSIONLOSSFACTOR", "STARTTYPE", "DISTRIBUTIONLOSSFACTOR",
    "MINIMUM_ENERGY_PRICE", "MAXIMUM_ENERGY_PRICE", "SCHEDULE_TYPE",
    "MIN_RAMP_RATE_UP", "MIN_RAMP_RATE_DOWN", "MAX_RAMP_RATE_UP",
    "MAX_RAMP_RATE_DOWN", "IS_AGGREGATED", "DISPATCHSUBTYPE", "ADG_ID",
    "LOAD_MINIMUM_ENERGY_PRICE", "LOAD_MAXIMUM_ENERGY_PRICE",
    "LOAD_MIN_RAMP_RATE_UP", "LOAD_MIN_RAMP_RATE_DOWN",
    "LOAD_MAX_RAMP_RATE_UP", "LOAD_MAX_RAMP_RATE_DOWN", "SECONDARY_TLF",
]


def fetch_mlf_history(cache_dir: str, force: bool = False) -> pd.DataFrame:
    """Download DUDETAILSUMMARY, extract per-FY MLFs for all generators.

    Returns DataFrame with columns: DUID, fy_label, fy_start_year, mlf
    """
    cache_path = Path(cache_dir)
    feather_path = cache_path / "mlf_history.feather"

    if feather_path.exists() and not force:
        logger.info("Loading cached MLF history")
        return pd.read_feather(feather_path)

    # Find latest available month
    latest = _get_latest_available_month()
    if latest is None:
        raise RuntimeError("Could not determine latest available month from AEMO")

    year, month = latest

    # Download raw DUDETAILSUMMARY
    raw = _download_dudetailsummary(year, month)

    # Extract per-FY MLFs
    fy_end = config.current_fy_start()
    result = _extract_fy_mlfs(raw, config.FY_START, fy_end)

    # Cache
    result.to_feather(feather_path)
    logger.info(f"Cached MLF history: {len(result)} DUID×FY records")
    return result


def _get_latest_available_month() -> tuple[int, int] | None:
    """Probe AEMO MMSDM archive to find the newest published month."""
    from datetime import datetime, timedelta
    now = datetime.now()
    for months_back in range(0, 4):
        probe_date = now - timedelta(days=30 * months_back)
        year = probe_date.year
        month = probe_date.month
        url = f"{config.MMSDM_BASE_URL}{year:04d}/MMSDM_{year:04d}_{month:02d}/"
        try:
            resp = requests.head(url, timeout=15, allow_redirects=True)
            if resp.status_code == 200:
                return (year, month)
        except requests.RequestException:
            continue
    return None


def _download_dudetailsummary(year: int, month: int) -> pd.DataFrame:
    """Download and parse DUDETAILSUMMARY from MMSDM archive."""
    url = config.DUDETAILSUMMARY_URL_TEMPLATE.format(year=year, month=month)
    logger.info(f"Downloading DUDETAILSUMMARY for {year}-{month:02d}...")

    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt < config.MAX_RETRIES - 1:
                time.sleep(config.RETRY_BACKOFF * (attempt + 1))
            else:
                raise RuntimeError(f"Failed to download DUDETAILSUMMARY: {e}")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        csv_content = zf.read(csv_names[0]).decode("utf-8")

    data_lines = [line for line in csv_content.splitlines() if line.startswith("D,")]
    logger.info(f"Parsing {len(data_lines)} data rows...")

    rows = []
    reader = csv.reader(io.StringIO("\n".join(data_lines)))
    for fields in reader:
        values = fields[4:]
        if len(values) >= len(DUDETAILSUMMARY_COLUMNS):
            row = dict(zip(DUDETAILSUMMARY_COLUMNS, values[:len(DUDETAILSUMMARY_COLUMNS)]))
            rows.append(row)

    df = pd.DataFrame(rows)
    df["START_DATE"] = pd.to_datetime(df["START_DATE"], errors="coerce")
    # AEMO uses 2999/12/31 for records still in effect — cap at 2100
    df["END_DATE"] = df["END_DATE"].str.replace(r"^2999", "2100", regex=True)
    df["END_DATE"] = pd.to_datetime(df["END_DATE"], errors="coerce")
    df["TRANSMISSIONLOSSFACTOR"] = pd.to_numeric(df["TRANSMISSIONLOSSFACTOR"], errors="coerce")
    df = df[df["DISPATCHTYPE"] == "GENERATOR"].copy()

    logger.info(f"Parsed {len(df)} generator records for {df['DUID'].nunique()} DUIDs")
    return df


def _extract_fy_mlfs(detail_df: pd.DataFrame, fy_start: int, fy_end: int) -> pd.DataFrame:
    """Extract one MLF value per DUID per financial year."""
    rows = []
    for fy_start_year in range(fy_start, fy_end + 1):
        fy_begin = pd.Timestamp(f"{fy_start_year}-07-01")
        fy_end_ts = pd.Timestamp(f"{fy_start_year + 1}-07-01")
        fy_label = config.fy_label(fy_start_year)

        mask = (detail_df["START_DATE"] < fy_end_ts) & (detail_df["END_DATE"] > fy_begin)
        fy_data = detail_df[mask]
        if fy_data.empty:
            continue

        for duid, group in fy_data.groupby("DUID"):
            fy_start_records = group[group["START_DATE"] <= fy_begin]
            if not fy_start_records.empty:
                best = fy_start_records.sort_values("START_DATE").iloc[-1]
            else:
                best = group.sort_values("START_DATE").iloc[0]

            rows.append({
                "DUID": duid,
                "fy_label": fy_label,
                "fy_start_year": fy_start_year,
                "mlf": best["TRANSMISSIONLOSSFACTOR"],
            })

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values(["DUID", "fy_start_year"]).reset_index(drop=True)

    logger.info(f"Extracted {len(result)} DUID×FY MLF records across "
                f"{result['DUID'].nunique()} DUIDs")
    return result
