"""Download and parse WEM Transmission Loss Factor (TLF) annual files.

TLF = WEM equivalent of NEM's Marginal Loss Factor (MLF).
Applied as: Revenue = Energy_MWh × Price × TLF

Sources:
  AEMO website — annual CSV or XLSX (2012-13 to present).
  Requires Referer header to avoid 403.

TLF area codes:
  T-prefix: 15 transmission-connected generator nodes
  W-prefix: 158 distribution-connected points

No explicit Facility Code → TLF area code mapping is published.
Approach: name-match TLF Description against Facility Code tokens,
with a hardcoded override table for known ambiguous cases.
"""

from __future__ import annotations

import io
import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)


def fetch_tlf_history(cache_dir: str, force: bool = False) -> pd.DataFrame:
    """Download all available annual TLF files and return a combined DataFrame.

    Returns columns: fy_label (str), LossFactorAreaCode (str),
                     Description (str), LossFactor (float)
    The 'fy_label' uses the same format as NEM: "FY12-13", "FY22-23", etc.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    feather_path = cache_path / "tlf_history.feather"

    if feather_path.exists() and not force:
        logger.info("Loading cached WEM TLF history")
        return pd.read_feather(feather_path)

    frames = []
    for fy_str, url in config.TLF_URLS.items():
        fy_label = _fy_str_to_label(fy_str)
        df = _download_tlf_file(url, fy_label)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        logger.error("No TLF data downloaded — check network/Referer headers")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["fy_label", "LossFactorAreaCode"])
    combined.to_feather(feather_path)
    logger.info(f"Cached TLF history: {len(combined)} rows across {combined['fy_label'].nunique()} FYs")
    return combined


def build_facility_tlf_lookup(
    generators: pd.DataFrame,
    tlf_history: pd.DataFrame,
    fy_start: int,
) -> dict[str, float]:
    """Build Facility Code → TLF value lookup for a given financial year.

    Args:
        generators: WEM generator metadata with DUID column
        tlf_history: Output of fetch_tlf_history()
        fy_start: Start year of the FY (e.g. 2022 for FY22-23)

    Returns:
        Dict mapping DUID → TLF value. Missing DUIDs get 1.0 (no adjustment).
    """
    if tlf_history is None or tlf_history.empty:
        return {}

    fy_label = f"FY{fy_start % 100:02d}-{(fy_start + 1) % 100:02d}"

    # Find exact FY or nearest available
    available_fys = sorted(tlf_history["fy_label"].unique())
    if fy_label not in available_fys:
        # Use nearest FY (prefer earlier one to avoid look-ahead)
        earlier = [f for f in available_fys if f <= fy_label]
        later = [f for f in available_fys if f > fy_label]
        fy_label = earlier[-1] if earlier else (later[0] if later else None)
        if fy_label:
            logger.debug(f"TLF FY{fy_start%100:02d}-{(fy_start+1)%100:02d} not found — using {fy_label}")
        else:
            return {}

    fy_tlf = tlf_history[tlf_history["fy_label"] == fy_label].copy()
    area_to_tlf = dict(zip(fy_tlf["LossFactorAreaCode"], fy_tlf["LossFactor"]))
    area_to_desc = dict(zip(fy_tlf["LossFactorAreaCode"], fy_tlf["Description"]))

    if generators is None or generators.empty:
        return {}

    duids = generators["DUID"].tolist()
    result = {}
    for duid in duids:
        area_code = _match_duid_to_tlf_area(duid, area_to_desc)
        if area_code:
            result[duid] = area_to_tlf[area_code]
        # Missing → caller uses 1.0 (pass-through)

    matched = len(result)
    total = len(duids)
    logger.info(f"TLF lookup {fy_label}: matched {matched}/{total} DUIDs")
    return result


# ─── Hard-coded overrides ─────────────────────────────────────────────────────
# Maps DUID prefix/substring → TLF area code.
# Used when name matching is ambiguous or fails for known facilities.

_DUID_TO_AREA_OVERRIDES: dict[str, str] = {
    # Alcoa Pinjarra has two TLF codes for different participants at the same node
    "ALCOA_WGP": "TAPL",    # Alcoa Pinjarra (Alinta supply)
    "ALINTA_WGP": "TAPL",   # Alcoa Pinjarra (Alinta)
    # Kwinana facilities share WKPS node
    "KWINANA": "WKPS",
    "KGTS": "WKPS",
    # Muja — multiple units at Muja PWS node
    "MUJA": "WMPS",
    "MJA": "WMPS",
    # NewGen Kwinana
    "NEWGEN_KWINANA": "WNGK",
    "NGK": "WNGK",
}


def _match_duid_to_tlf_area(duid: str, area_to_desc: dict[str, str]) -> str | None:
    """Match a Facility Code to a TLF area code by name similarity.

    Strategy (in order):
    1. Hard-coded override table
    2. Token overlap: normalise both strings, find best Jaccard match
    """
    # 1. Override table
    duid_upper = duid.upper()
    for prefix, area_code in _DUID_TO_AREA_OVERRIDES.items():
        if duid_upper.startswith(prefix) or prefix in duid_upper:
            if area_code in area_to_desc:
                return area_code

    # 2. Token overlap
    duid_tokens = _tokenise(duid)
    if not duid_tokens:
        return None

    best_area = None
    best_score = 0.0

    for area_code, desc in area_to_desc.items():
        desc_tokens = _tokenise(desc)
        if not desc_tokens:
            continue
        intersection = duid_tokens & desc_tokens
        union = duid_tokens | desc_tokens
        score = len(intersection) / len(union)
        if score > best_score:
            best_score = score
            best_area = area_code

    # Only accept if at least one meaningful token matched
    if best_score >= 0.25:
        return best_area
    return None


def _tokenise(text: str) -> set[str]:
    """Normalise and tokenise a string for name matching."""
    text = text.lower()
    # Split on underscores, spaces, hyphens, and digits/letter boundaries
    tokens = re.split(r"[_\s\-/(),]+", text)
    # Remove short noise tokens and numeric suffixes (GT1, WF2, etc.)
    stop = {"the", "a", "and", "of", "at", "in", "gt", "st", "pws", "ps", "power",
            "station", "farm", "park", "project", "energy", "generation"}
    cleaned = set()
    for t in tokens:
        t = re.sub(r"\d+$", "", t)  # strip trailing digits
        if len(t) >= 3 and t not in stop:
            cleaned.add(t)
    return cleaned


def _fy_str_to_label(fy_str: str) -> str:
    """Convert '2022-23' to 'FY22-23'."""
    parts = fy_str.split("-")
    if len(parts) == 2:
        return f"FY{parts[0][-2:]}-{parts[1][-2:]}"
    return f"FY{fy_str}"


def _download_tlf_file(url: str, fy_label: str) -> pd.DataFrame | None:
    """Download a single TLF file (CSV or XLSX) and parse it."""
    headers = {
        "User-Agent": config.USER_AGENT,
        "Referer": config.TLF_REFERER,
    }
    is_xlsx = url.lower().endswith(".xlsx")

    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=config.REQUEST_TIMEOUT, headers=headers)
            resp.raise_for_status()
            content = resp.content
            break
        except Exception as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"TLF {fy_label} download failed: {e}. Retry in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"TLF {fy_label} unavailable ({url}): {e}")
                return None

    try:
        if is_xlsx:
            df = pd.read_excel(io.BytesIO(content), header=0)
        else:
            # Some older files have BOM or extra commas at end of row
            text = content.decode("utf-8-sig", errors="replace")
            df = pd.read_csv(io.StringIO(text))
    except Exception as e:
        logger.error(f"TLF {fy_label} parse error: {e}")
        return None

    # Normalise column names
    df.columns = df.columns.str.strip()
    col_map = {}
    for col in df.columns:
        lower = col.lower().strip()
        if "lossfactorareacode" in lower or lower == "lossfactorareacode":
            col_map[col] = "LossFactorAreaCode"
        elif "description" in lower:
            col_map[col] = "Description"
        elif "lossfactor" in lower and "area" not in lower:
            col_map[col] = "LossFactor"
        elif "startdate" in lower or "start date" in lower:
            col_map[col] = "StartDate"
    df = df.rename(columns=col_map)

    required = {"LossFactorAreaCode", "LossFactor"}
    missing = required - set(df.columns)
    if missing:
        logger.warning(f"TLF {fy_label}: missing columns {missing} — skipping")
        return None

    df = df.dropna(subset=["LossFactorAreaCode"])
    df["LossFactorAreaCode"] = df["LossFactorAreaCode"].astype(str).str.strip()
    df = df[df["LossFactorAreaCode"].str.len() > 0]
    df["LossFactor"] = pd.to_numeric(df["LossFactor"], errors="coerce")
    df = df.dropna(subset=["LossFactor"])

    if "Description" not in df.columns:
        df["Description"] = df["LossFactorAreaCode"]
    else:
        df["Description"] = df["Description"].astype(str).str.strip()

    df["fy_label"] = fy_label
    result = df[["fy_label", "LossFactorAreaCode", "Description", "LossFactor"]].copy()
    logger.info(f"TLF {fy_label}: {len(result)} area codes downloaded")
    return result
