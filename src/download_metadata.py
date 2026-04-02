"""Download and parse generator metadata from AEMO NEM Registration List.

Unlike the Renewable Generator Dashboard, this includes ALL fuel types
(solar, wind, hydro, battery, fossil) for credit risk analysis.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)


def fetch_generators(cache_dir: str, force: bool = False) -> pd.DataFrame:
    """Download AEMO NEM Registration List and extract generator metadata.

    Returns DataFrame with DUID, STATION_NAME, FUEL_SOURCE, FUEL_CATEGORY,
    TECHNOLOGY, CAPACITY_MW, REGION, CONNECTION_POINT, DISPATCH_TYPE.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    feather_path = cache_path / "generators.feather"
    if feather_path.exists() and not force:
        logger.info("Loading cached generator metadata")
        return pd.read_feather(feather_path)

    xls_path = cache_path / "NEM-Registration-and-Exemption-List.xls"

    # Download
    if not xls_path.exists() or force:
        logger.info("Downloading NEM Registration List from AEMO...")
        _download_with_retry(config.REGISTRATION_URL, xls_path)

    # Parse
    df = _parse_registration_list(xls_path)

    # Cache
    df.to_feather(feather_path)
    logger.info(f"Cached {len(df)} generators to {feather_path}")
    return df


def _parse_registration_list(xls_path: Path) -> pd.DataFrame:
    """Parse the NEM Registration and Exemption List for all generators."""
    logger.info("Parsing NEM Registration List...")

    try:
        df = pd.read_excel(xls_path, engine="openpyxl", sheet_name=config.REGISTRATION_SHEET)
    except Exception:
        df = pd.read_excel(xls_path, sheet_name=config.REGISTRATION_SHEET)

    # Map columns — flexible matching for AEMO's inconsistent headers
    col_map = {}
    columns_lower = {c: c.lower().strip() for c in df.columns}
    mappings = {
        "DUID": ["duid"],
        "STATION_NAME": ["station name", "station"],
        "REGION": ["region"],
        "TECHNOLOGY": ["technology type - descriptor", "technology type"],
        "FUEL_SOURCE": ["fuel source - descriptor", "fuel source - primary"],
        "CAPACITY_MW": ["reg cap generation (mw)", "reg cap (mw)", "nameplate capacity"],
        "DISPATCH_TYPE": ["dispatch type"],
        "CLASSIFICATION": ["classification"],
        "CONNECTION_POINT": ["connection point id", "connection point"],
    }
    for target, candidates in mappings.items():
        for orig_col, lower_col in columns_lower.items():
            if any(c in lower_col for c in candidates):
                if target not in col_map.values():
                    col_map[orig_col] = target
                break

    df = df.rename(columns=col_map)
    df = df.dropna(subset=["DUID"])
    df["DUID"] = df["DUID"].astype(str).str.strip()
    df = df[df["DUID"] != "-"]  # Exclude placeholder DUIDs (e.g. Portland Wind Farm, Callide)

    # Filter to generators and bidirectional units (exclude pure loads)
    if "DISPATCH_TYPE" in df.columns:
        dt = df["DISPATCH_TYPE"].astype(str).str.lower()
        df = df[dt.str.contains("generat|bidirectional", na=False)].copy()

    # Classify fuel type
    df["FUEL_CATEGORY"] = df.apply(_classify_fuel, axis=1)

    # Convert capacity to numeric
    if "CAPACITY_MW" in df.columns:
        df["CAPACITY_MW"] = pd.to_numeric(df["CAPACITY_MW"], errors="coerce")

    # Deduplicate
    df = df.drop_duplicates(subset="DUID", keep="first")

    # Select final columns
    keep_cols = [
        "DUID", "STATION_NAME", "REGION", "FUEL_SOURCE", "FUEL_CATEGORY",
        "TECHNOLOGY", "CAPACITY_MW", "CONNECTION_POINT", "DISPATCH_TYPE",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].copy()

    fuel_counts = df["FUEL_CATEGORY"].value_counts().to_dict()
    logger.info(f"Parsed {len(df)} generators: {fuel_counts}")
    return df


def _classify_fuel(row) -> str:
    """Classify a generator's fuel type from its registration data."""
    for col in ["FUEL_SOURCE", "TECHNOLOGY"]:
        val = str(row.get(col, "")).lower()
        if "solar" in val or "photovoltaic" in val:
            return "Solar"
        if "wind" in val:
            return "Wind"
        if "hydro" in val or "water" in val:
            return "Hydro"
        if "battery" in val:
            return "Battery"
        if any(f in val for f in ["coal", "gas", "oil", "diesel", "fossil"]):
            return "Fossil"
        if any(f in val for f in ["biomass", "waste", "bagasse", "landfill", "biogas"]):
            return "Other Renewable"
    return "Other"


def _download_with_retry(url: str, dest: Path):
    """Download a file with retry logic."""
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                timeout=config.REQUEST_TIMEOUT,
                headers={"User-Agent": config.USER_AGENT},
            )
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            logger.info(f"Downloaded {len(resp.content) / 1024:.0f} KB → {dest.name}")
            return
        except requests.RequestException as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Failed to download {url}: {e}")
