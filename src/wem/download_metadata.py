"""Download and parse WEM facility metadata.

Sources:
  - Pre-Reform: facilities.csv — 114 facilities with capacity credits + max capacity
  - Post-Reform: post-facilities/facilities.csv — 97 facilities with system size

Both are combined; pre-Reform record wins on conflicts (richer field set).
Fuel type is inferred from facility name/code keywords — WEM has no fuel type field.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)


def fetch_wem_generators(cache_dir: str, force: bool = False) -> pd.DataFrame:
    """Download WEM facility lists and return a generator metadata DataFrame.

    Returns DataFrame with columns matching the NEM schema:
      DUID, STATION_NAME, FUEL_CATEGORY, CAPACITY_MW, REGION, TECHNOLOGY
    (REGION is always "WEM"; TECHNOLOGY is empty — WEM doesn't publish it)
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    feather_path = cache_path / "facilities.feather"

    if feather_path.exists() and not force:
        logger.info("Loading cached WEM facility metadata")
        return pd.read_feather(feather_path)

    pre = _fetch_pre_reform_facilities()
    post = _fetch_post_reform_facilities()
    df = _merge_facilities(pre, post)

    df.to_feather(feather_path)
    logger.info(f"Cached {len(df)} WEM generators to {feather_path}")
    return df


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _fetch_pre_reform_facilities() -> pd.DataFrame:
    """Download pre-Reform facilities.csv."""
    logger.info("Downloading pre-Reform WEM facilities...")
    raw = _get_csv(config.FACILITIES_URL)
    if raw is None or raw.empty:
        logger.warning("Pre-Reform facilities CSV unavailable")
        return pd.DataFrame()

    # Normalise column names (strip whitespace, lowercase for matching)
    raw.columns = raw.columns.str.strip()

    col_map = _flexible_col_map(raw.columns, {
        "Facility Code": ["facility code", "facilitycode"],
        "Participant Name": ["participant name", "participantname"],
        "Facility Type": ["facility type", "facilitytype"],
        "Maximum Capacity (MW)": ["maximum capacity (mw)", "maximum capacity"],
        "Capacity Credits (MW)": ["capacity credits (mw)", "capacity credits"],
        "Registered From": ["registered from", "registeredfrom"],
    })
    raw = raw.rename(columns=col_map)

    required = {"Facility Code"}
    if not required.issubset(raw.columns):
        logger.warning(f"Pre-Reform facilities missing columns: {required - set(raw.columns)}")
        return pd.DataFrame()

    raw["Facility Code"] = raw["Facility Code"].astype(str).str.strip()
    raw = raw[raw["Facility Code"].str.len() > 0]

    cap = None
    for c in ["Maximum Capacity (MW)", "Capacity Credits (MW)"]:
        if c in raw.columns:
            cap = pd.to_numeric(raw[c], errors="coerce")
            if cap.notna().sum() > 0:
                break

    return pd.DataFrame({
        "DUID": raw["Facility Code"],
        "STATION_NAME": raw["Facility Code"],  # WEM has no separate station name
        "FACILITY_TYPE": raw.get("Facility Type", pd.Series("", index=raw.index)),
        "CAPACITY_MW": cap if cap is not None else pd.Series(dtype=float),
        "_source": "pre",
    })


def _fetch_post_reform_facilities() -> pd.DataFrame:
    """Download post-Reform post-facilities/facilities.csv."""
    logger.info("Downloading post-Reform WEM facilities...")
    raw = _get_csv(config.POST_FACILITIES_URL)
    if raw is None or raw.empty:
        logger.warning("Post-Reform facilities CSV unavailable")
        return pd.DataFrame()

    raw.columns = raw.columns.str.strip()

    col_map = _flexible_col_map(raw.columns, {
        "Facility Code": ["facility code", "facilitycode"],
        "Participant Name": ["participant name", "participantname"],
        "Facility Class": ["facility class", "facilityclass", "class"],
        "System Size (MW)": ["system size (mw)", "system size"],
    })
    raw = raw.rename(columns=col_map)

    if "Facility Code" not in raw.columns:
        logger.warning("Post-Reform facilities CSV missing 'Facility Code'")
        return pd.DataFrame()

    raw["Facility Code"] = raw["Facility Code"].astype(str).str.strip()
    raw = raw[raw["Facility Code"].str.len() > 0]

    cap = pd.to_numeric(raw.get("System Size (MW)", pd.Series(dtype=str)), errors="coerce")

    return pd.DataFrame({
        "DUID": raw["Facility Code"],
        "STATION_NAME": raw["Facility Code"],
        "FACILITY_TYPE": raw.get("Facility Class", pd.Series("", index=raw.index)),
        "CAPACITY_MW": cap,
        "_source": "post",
    })


def _merge_facilities(pre: pd.DataFrame, post: pd.DataFrame) -> pd.DataFrame:
    """Merge pre- and post-Reform facility lists; pre-Reform record wins on conflict."""
    frames = []
    if not pre.empty:
        frames.append(pre)
    if not post.empty:
        frames.append(post)

    if not frames:
        logger.error("No WEM facility data available")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Pre-Reform wins: sort so pre rows come first, then deduplicate keeping first
    combined = combined.sort_values("_source").drop_duplicates(subset="DUID", keep="first")
    combined = combined.drop(columns=["_source"])

    combined["REGION"] = config.WEM_REGION
    combined["MARKET"] = "WEM"
    combined["TECHNOLOGY"] = ""  # Not published in WEM facility lists

    # Classify fuel type from facility name/code
    combined["FUEL_CATEGORY"] = combined["DUID"].apply(_classify_fuel)

    # Exclude non-generator facility types (loads, network elements)
    if "FACILITY_TYPE" in combined.columns:
        ftype = combined["FACILITY_TYPE"].astype(str).str.lower()
        # Keep GENERATOR, STORAGE, and facilities with no type listed
        # Exclude LOAD, NETWORK, DEMAND SIDE PROGRAMME
        exclude = ftype.str.contains("load|network|demand side|dsp", na=False)
        combined = combined[~exclude].copy()

    combined = combined.drop(columns=["FACILITY_TYPE"], errors="ignore")
    combined = combined.reset_index(drop=True)

    fuel_counts = combined["FUEL_CATEGORY"].value_counts().to_dict()
    logger.info(f"Merged {len(combined)} WEM generators: {fuel_counts}")
    return combined


def _classify_fuel(duid: str) -> str:
    """Infer fuel type from WEM Facility Code (all caps, underscore-delimited).

    WEM codes use abbreviations: _WF = Wind Farm, _PV/_SF = Solar,
    _GT/_OCGT/_CCGT = Gas Turbine, _BESS = Battery, _WTE = Waste-to-Energy.
    """
    name = duid.lower()
    tokens = set(name.replace("_", " ").split())

    # Abbreviation-first checks (WEM-specific)
    abbrev_wind = {"_wf", "_wf1", "_wf2", "wfman", "wfman2"}
    if any(a in name for a in ("_wf", "wfman")) or "windfarm" in name or "wind" in name:
        return "Wind"

    if any(a in name for a in ("_pv", "_sf", "solar")):
        return "Solar"

    if any(a in name for a in ("_bess", "battery", " storage")):
        return "Battery"

    if any(a in name for a in ("_wte", "wte", "biomass", "waste", "landfill", "biogas", "bagasse")):
        return "Other Renewable"

    if any(a in name for a in ("_gt", "_ocgt", "_ccgt", "ccgt", "ocgt", "gasturbine", "ngps")):
        return "Fossil"

    # Keyword scan on full normalised name
    for fuel, keywords in config.FUEL_KEYWORDS:
        if any(kw in name for kw in keywords):
            return fuel

    # Known WEM station names that don't fit patterns
    _known: dict[str, str] = {
        "collie":   "Fossil",   # Collie coal power station
        "muja":     "Fossil",   # Muja coal
        "bluewaters": "Fossil", # Bluewaters coal
        "cockburn": "Fossil",   # Cockburn gas
        "pinjar":   "Fossil",   # Pinjar gas
        "kwinana":  "Fossil",   # Kwinana gas/steam
        "pinjarra": "Fossil",   # Alcoa Pinjarra gas
        "alcoa":    "Fossil",
        "alinta":   "Fossil",
        "newgen":   "Fossil",
        "kemerton": "Fossil",
        "southwest": "Fossil",
        "swcjv":    "Fossil",
        "prk":      "Fossil",   # Perth Regional Kerosene?
        "mersea":   "Fossil",
        "perthenergy": "Fossil",
        "perthenrg": "Fossil",
        "red_hill": "Fossil",   # Red Hill diesel
        "tamala":   "Other Renewable",  # Tamala Park landfill gas
        "waldeck":  "Other Renewable",  # Waldeck landfill
        "sterlng":  "Other Renewable",
        "south_cardup": "Other Renewable",
        "cardup":   "Other Renewable",  # South Cardup landfill gas
        "tesla":    "Battery",          # Tesla Picton BESS
        "picton":   "Battery",
        "walpole":  "Hydro",            # Walpole hydro
        "_hg":      "Hydro",            # _HG = Hydro Generator suffix
        "hydro":    "Hydro",
        "mumbida":  "Wind",
        "collgar":  "Wind",
        "badgingarra": "Wind",
        "warradarge": "Wind",
        "flat_rock": "Wind",
        "flatrock":  "Wind",
        "albany":   "Wind",
        "edwf":     "Wind",     # Emu Downs Wind Farm
        "emudowns": "Wind",
    }
    for prefix, fuel in _known.items():
        if prefix in name:
            return fuel

    return config.FUEL_FALLBACK


def _flexible_col_map(columns: pd.Index, target_map: dict[str, list[str]]) -> dict[str, str]:
    """Build rename dict: original column → target name, using case-insensitive substring match."""
    result = {}
    lower_cols = {c: c.lower() for c in columns}
    for target, candidates in target_map.items():
        for orig, lower in lower_cols.items():
            if any(c == lower for c in candidates):
                if target not in result.values():
                    result[orig] = target
                break
    return result


def _get_csv(url: str) -> pd.DataFrame | None:
    """Download a CSV with retry, return as DataFrame or None on failure."""
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
                logger.warning(f"Download failed ({url}): {e}. Retry in {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"Failed to download {url}: {e}")
                return None
