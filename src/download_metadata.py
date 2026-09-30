"""Download and parse generator metadata from AEMO NEM Registration List.

Unlike the Renewable Generator Dashboard, this includes ALL fuel types
(solar, wind, hydro, battery, fossil) for credit risk analysis.

DUID metadata lookup:
  Tier 1 — NEM Registration and Exemption List (currently registered units)

S3-06: the former "historical" tier that renamed MMSDM GENUNITS.GENSETID to
DUID and appended it without a verified crosswalk or region enrichment is
removed. AEMO distinguishes physical units (GENSETID) from dispatch units
(DUID); GENUNITS carries no dispatch identity or NEM region, aggregation
drops unmapped regions, so those appends could never supply dispatch
history — they only polluted the search index with 'nan'-region entries.
Only registration-list DUIDs (which carry a NEM region) are indexed.

Open loop df4d20dd76f90960 (ADPPV3): the Registration List itself can carry a
blank Region cell for a stub row (a 0.02 MW non-scheduled unit), which used to
pass straight through to index.json and inflate the dashboard region filter.
``_resolve_missing_regions`` now recovers those cells from the same-station
sibling units at parse time and reports anything it cannot resolve.
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

    Cache validity: a cached generators.feather is served only when every row
    carries one of the five NEM regions (config.REGIONS). S3-06 removed the
    historical GENSETID→DUID append; caches written in that era contain rows
    with no REGION and must be rebuilt from the Registration List rather than
    served. This is the deliberate metadata-cache invalidation for the fix —
    it also covers a docs/data/processed-cache snapshot restore that still
    holds GENSETID-era rows.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    feather_path = cache_path / "generators.feather"
    if feather_path.exists() and not force:
        cached = pd.read_feather(feather_path)
        if _cache_is_valid(cached):
            logger.info("Loading cached generator metadata")
            return cached
        logger.warning(
            "Cached generator metadata is invalid (rows outside the five NEM "
            "regions — GENSETID-era build). Rebuilding from Registration List."
        )
        _drop_stale_genset_caches(cache_path)

    xls_path = cache_path / "NEM-Registration-and-Exemption-List.xls"
    if not xls_path.exists() or force:
        logger.info("Downloading NEM Registration List from AEMO...")
        _download_with_retry(config.REGISTRATION_URL, xls_path)

    # Parse the Registration List (single authoritative metadata source)
    df = _parse_registration_list(xls_path)

    # Apply known capacity corrections (stale or unit-level registrations)
    if config.CAPACITY_OVERRIDES:
        for duid, corrected_mw in config.CAPACITY_OVERRIDES.items():
            mask = df["DUID"] == duid
            if mask.any():
                original = df.loc[mask, "CAPACITY_MW"].values[0]
                df.loc[mask, "CAPACITY_MW"] = corrected_mw
                logger.info(
                    f"Capacity override applied: {duid} {original} MW → {corrected_mw} MW"
                )

    logger.info(f"Total generator metadata: {len(df)} DUIDs")

    # Cache
    df.to_feather(feather_path)
    _drop_stale_genset_caches(cache_path)
    logger.info(f"Cached {len(df)} generators to {feather_path}")
    return df


def _cache_is_valid(generators: pd.DataFrame) -> bool:
    """A metadata cache is servable only when every row has a real NEM region.

    Rows appended by the removed GENSETID→DUID path carry no REGION (they were
    never dispatch-mapped) — any cache containing such rows comes from the old
    build and must be rebuilt, never served.
    """
    if generators is None or generators.empty or "REGION" not in generators.columns:
        return False
    region = generators["REGION"]
    return bool(region.notna().all() and region.isin(config.REGIONS).all())


def _drop_stale_genset_caches(cache_path: Path) -> None:
    """Remove orphaned caches written by the removed MMSDM GENUNITS tier."""
    for name in ("mmsdm_station.feather", "mmsdm_genunits.feather"):
        stale = cache_path / name
        if stale.exists():
            stale.unlink()
            logger.info(f"Removed stale GENSETID-era cache {stale.name}")


def _clean_region(value):
    """Normalise a Registration List Region cell to a stripped string or None."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _resolve_missing_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Backfill registration rows whose Region cell is blank (no NEM region).

    Open loop df4d20dd76f90960 (ADPPV3): the Registration List vintage behind the
    2026-09-17 publish carried an EMPTY Region cell for ADPPV3 (Adelaide
    Desalination Plant solar, 0.02 MW — a registration stub row). Nothing between
    the parse and the published JSON rejected a blank region, so
    ``docs/data/index.json`` and ``docs/data/generators/ADPPV3.json`` published
    ``"region": ""``. The dashboard builds its region filter from the distinct
    ``region`` values in the index (docs/index.html populateFilters/updateStats),
    so the blank read as a sixth region.

    Only the Registration List's own rows exist at this point, so the region is
    recovered from the most authoritative local evidence available: the other
    units registered on the same STATION_NAME, which resolve to exactly one NEM
    region (a station physically sits in one region — the current list has 453
    stations and no name spanning two regions). A missing region that no sibling
    supplies is left unset and reported loudly rather than guessed; a
    present-but-not-NEM value is reported too and never overwritten, because
    silently rewriting a stated region would hide a source anomaly.

    Returns the frame with REGION normalised (stripped strings, None when
    unresolved); rows that end with no NEM region keep failing
    ``_cache_is_valid`` so they can never be served from cache.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    if "REGION" not in df.columns:
        df["REGION"] = None

    region = df["REGION"].map(_clean_region)
    valid = region.isin(config.REGIONS)
    missing = region.isna()
    anomaly = (~valid) & (~missing)

    if anomaly.any():
        logger.warning(
            "Registration List rows carry a non-NEM region value (left as-is, "
            "never overwritten): %s",
            ", ".join(
                f"{df.at[i, 'DUID']}={region.at[i]!r}" for i in df.index[anomaly]
            ),
        )

    if missing.any():
        station = df["STATION_NAME"].astype(object).map(
            lambda v: str(v).strip() if isinstance(v, str) else None
        )
        consensus: dict[str, str] = {}
        for name, group in df[valid].groupby(station[valid]):
            regions = set(region[group.index].dropna())
            if name and len(regions) == 1:
                consensus[name] = regions.pop()

        recovered = {
            df.at[i, "DUID"]: consensus[station.at[i]]
            for i in df.index[missing]
            if station.at[i] in consensus
        }
        if recovered:
            region = region.copy()
            for i in df.index[missing]:
                name = station.at[i]
                if name in consensus:
                    region.at[i] = consensus[name]
            logger.info(
                "Backfilled REGION for %d Registration List row(s) from "
                "same-station consensus: %s",
                len(recovered),
                ", ".join(f"{d}→{r}" for d, r in sorted(recovered.items())),
            )

        unresolved = [
            df.at[i, "DUID"] for i in df.index[missing] if pd.isna(region.at[i])
        ]
        if unresolved:
            logger.warning(
                "Registration List rows with no NEM region and no resolvable "
                "sibling unit — they will publish a blank region and inflate the "
                "dashboard region filter until AEMO restores the cell: %s",
                ", ".join(unresolved),
            )

    df["REGION"] = region
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

    # Region guard (open loop df4d20dd76f90960): a blank Region cell in the
    # Registration List vintage must not reach index.json / the per-DUID docs.
    df = _resolve_missing_regions(df)

    # Select final columns
    keep_cols = [
        "DUID", "STATION_NAME", "REGION", "FUEL_SOURCE", "FUEL_CATEGORY",
        "TECHNOLOGY", "CAPACITY_MW", "CONNECTION_POINT", "DISPATCH_TYPE",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].copy()

    fuel_counts = df["FUEL_CATEGORY"].value_counts().to_dict()
    logger.info(f"Parsed {len(df)} generators from Registration List: {fuel_counts}")
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
