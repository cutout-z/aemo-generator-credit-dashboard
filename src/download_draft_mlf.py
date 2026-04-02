"""Download and parse AEMO's draft/indicative MLFs for the upcoming financial year.

Adapted from AEMO MLF Tracker project (src/indicative.py).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

# AEMO publishes draft MLFs here. URL pattern may change each year.
DRAFT_MLF_URL = (
    "https://aemo.com.au/-/media/files/electricity/nem/security_and_reliability/"
    "loss_factors_and_regional_boundaries/{fy_folder}/"
    "draft-marginal-loss-factors-for-the-{fy_label}-financial-year-xls.xlsx"
)

# Sheet name → NEM region mapping (Gen sheets only)
SHEET_REGION_MAP = {
    "QLD Gen": "QLD1",
    "NSW Gen": "NSW1",
    "ACT Gen": "NSW1",
    "VIC Gen": "VIC1",
    "SA Gen": "SA1",
    "TAS Gen": "TAS1",
}


def get_draft_fy() -> tuple[int, str, str]:
    """Determine which FY the next draft MLFs are for.

    Returns (start_year, fy_label, fy_folder) e.g. (2026, '2026-27', '2026-27')
    """
    next_fy = config.current_fy_start() + 1
    fy_label = f"{next_fy}-{(next_fy + 1) % 100:02d}"
    return next_fy, fy_label, fy_label


def fetch_draft_mlfs(cache_dir: str, force: bool = False) -> dict[str, float] | None:
    """Download and parse AEMO's draft MLF Excel file.

    Returns dict mapping DUID -> draft MLF value, or None if unavailable.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    next_fy, fy_label, fy_folder = get_draft_fy()
    url = DRAFT_MLF_URL.format(fy_folder=fy_folder, fy_label=fy_label)
    xlsx_path = cache_path / f"draft_mlf_{fy_label}.xlsx"

    if xlsx_path.exists() and not force:
        logger.info(f"Loading cached draft MLFs for FY{fy_label}")
    else:
        logger.info(f"Downloading draft MLFs for FY{fy_label}...")
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = requests.get(
                    url, timeout=30,
                    headers={"User-Agent": config.USER_AGENT},
                )
                if resp.status_code == 404:
                    logger.info(f"Draft MLFs for FY{fy_label} not yet published (404)")
                    return None
                resp.raise_for_status()
                xlsx_path.write_bytes(resp.content)
                logger.info(f"Downloaded draft MLFs ({len(resp.content) / 1024:.0f} KB)")
                break
            except requests.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait = config.RETRY_BACKOFF * (attempt + 1)
                    logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    logger.warning(f"Could not download draft MLFs: {e}")
                    if xlsx_path.exists():
                        logger.info("Falling back to previously cached draft MLFs")
                    else:
                        logger.warning(
                            f"No cached file available. Manually download from AEMO and save to: {xlsx_path}"
                        )
                        return None

    return _parse_draft_excel(xlsx_path, fy_label)


def _parse_draft_excel(xlsx_path: Path, fy_label: str) -> dict[str, float] | None:
    """Parse AEMO's draft MLF Excel into a DUID -> MLF dict."""
    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as e:
        logger.error(f"Failed to open draft MLF Excel: {e}")
        return None

    result = {}
    for sheet_name in SHEET_REGION_MAP:
        if sheet_name not in xls.sheet_names:
            continue

        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

        # Find header row containing "DUID"
        header_idx = None
        for i in range(min(20, len(df))):
            row_vals = [str(v).strip() for v in df.iloc[i].tolist()]
            if "DUID" in row_vals:
                header_idx = i
                break

        if header_idx is None:
            continue

        headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
        data = df.iloc[header_idx + 1:].copy()
        data.columns = headers
        data = data.dropna(subset=["DUID"])

        # Find the MLF column for the target FY
        mlf_col = [c for c in headers if fy_label in c and "MLF" in c]
        if not mlf_col:
            continue

        for _, row in data.iterrows():
            duid = str(row["DUID"]).strip()
            mlf = pd.to_numeric(row[mlf_col[0]], errors="coerce")
            if pd.notna(mlf) and duid and duid not in result:
                result[duid] = float(mlf)

    if not result:
        logger.warning("No draft MLF data parsed")
        return None

    logger.info(f"Parsed {len(result)} draft MLFs for FY{fy_label}")
    return result
