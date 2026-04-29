"""Download and parse generator metadata from AEMO NEM Registration List.

Unlike the Renewable Generator Dashboard, this includes ALL fuel types
(solar, wind, hydro, battery, fossil) for credit risk analysis.

Three-tier DUID metadata lookup:
  Tier 1 — NEM Registration List (currently registered generators)
  Tier 2 — MMSDM GENUNITS + STATION tables (historical/deregistered DUIDs)
  Tier 3 — Fallback labelling (NL suffix → Network Load, else Unknown)
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

# MMSDM PARTICIPANTREGISTRATION tables for historical DUID coverage
MMSDM_PR_URL_TEMPLATE = (
    "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
    "{year:04d}/MMSDM_{year:04d}_{month:02d}/"
    "MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_ARCHIVE%23{table}%23FILE01%23{year:04d}{month:02d}010000.zip"
)
STATION_COLS = [
    "STATIONID", "STATIONNAME", "ADDRESS1", "ADDRESS2", "ADDRESS3",
    "ADDRESS4", "CITY", "STATE", "POSTCODE", "LASTCHANGED", "CONNECTIONPOINTID",
]
GENUNITS_COLS = [
    "GENSETID", "STATIONID", "SETLOSSFACTOR", "CDINDICATOR", "AGCFLAG",
    "SPINNINGFLAG", "VOLTLEVEL", "REGISTEREDCAPACITY", "DISPATCHTYPE", "STARTTYPE",
    "MKTGENERATORIND", "NORMALSTATUS", "MAXCAPACITY", "GENSETTYPE", "GENSETNAME",
    "LASTCHANGED", "CO2E_EMISSIONS_FACTOR", "CO2E_ENERGY_SOURCE", "CO2E_DATA_SOURCE",
    "MINCAPACITY", "REGISTEREDMINCAPACITY", "MAXSTORAGECAPACITY",
]
# Maps GENUNITS CO2E_ENERGY_SOURCE → FUEL_CATEGORY used in the dashboard
CO2E_TO_FUEL_MAP = {
    "Solar": "Solar",
    "Wind": "Wind",
    "Hydro": "Hydro",
    "Battery Storage": "Battery",
    "Black coal": "Fossil",
    "Brown coal": "Fossil",
    "Natural Gas (Pipeline)": "Fossil",
    "Natural Gas (LNG)": "Fossil",
    "Diesel oil": "Fossil",
    "Kerosene - non aviation": "Fossil",
    "Coal seam methane": "Fossil",
    "Coal mine waste gas": "Fossil",
    "Landfill biogas methane": "Other Renewable",
    "Biomass and industrial materials": "Other Renewable",
    "Bagasse": "Other Renewable",
    "Biogas": "Other Renewable",
}

logger = logging.getLogger(__name__)


def _parse_aemo_csv(content: bytes, col_names: list[str]) -> pd.DataFrame:
    """Parse an AEMO MMSDM CSV (rows prefixed D, table_group, table, version)."""
    data_lines = [l for l in content.decode("utf-8").splitlines() if l.startswith("D,")]
    rows = []
    for fields in csv.reader(io.StringIO("\n".join(data_lines))):
        values = fields[4:]  # skip D, group, table, version
        if len(values) >= len(col_names):
            rows.append(dict(zip(col_names, values[:len(col_names)])))
    return pd.DataFrame(rows)


def _fetch_mmsdm_genunits_station(
    cache_dir: str, year: int, month: int
) -> tuple[pd.Series, pd.DataFrame]:
    """Download STATION and GENUNITS tables from MMSDM archive.

    Returns:
        station_names — pd.Series(STATIONID → STATIONNAME)
        genunits_df   — DataFrame with GENSETID, STATIONID, REGISTEREDCAPACITY,
                        CO2E_ENERGY_SOURCE, FUEL_CATEGORY
    """
    cache_path = Path(cache_dir)
    station_cache = cache_path / "mmsdm_station.feather"
    genunits_cache = cache_path / "mmsdm_genunits.feather"
    headers = {"User-Agent": config.USER_AGENT}

    # --- STATION table ---
    if station_cache.exists():
        station_df = pd.read_feather(station_cache)
        logger.info(f"Loaded STATION cache ({len(station_df)} rows)")
    else:
        url = MMSDM_PR_URL_TEMPLATE.format(year=year, month=month, table="STATION")
        try:
            resp = requests.get(url, timeout=30, headers=headers)
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                content = zf.read(zf.namelist()[0])
            station_df = _parse_aemo_csv(content, STATION_COLS)
            station_df = station_df[["STATIONID", "STATIONNAME"]].copy()
            station_df = station_df[
                station_df["STATIONID"].notna() & (station_df["STATIONID"] != "")
            ].drop_duplicates("STATIONID", keep="first")
            station_df.reset_index(drop=True).to_feather(station_cache)
            logger.info(f"STATION table: {len(station_df)} stations")
        except Exception as e:
            logger.warning(f"Could not fetch MMSDM STATION table: {e}")
            station_df = pd.DataFrame(columns=["STATIONID", "STATIONNAME"])

    station_names = station_df.set_index("STATIONID")["STATIONNAME"]

    # --- GENUNITS table ---
    if genunits_cache.exists():
        genunits_df = pd.read_feather(genunits_cache)
        logger.info(f"Loaded GENUNITS cache ({len(genunits_df)} rows)")
    else:
        url = MMSDM_PR_URL_TEMPLATE.format(year=year, month=month, table="GENUNITS")
        try:
            resp = requests.get(url, timeout=30, headers=headers)
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                content = zf.read(zf.namelist()[0])
            genunits_df = _parse_aemo_csv(content, GENUNITS_COLS)
            keep = ["GENSETID", "STATIONID", "REGISTEREDCAPACITY", "CO2E_ENERGY_SOURCE"]
            genunits_df = genunits_df[[c for c in keep if c in genunits_df.columns]].copy()
            genunits_df["REGISTEREDCAPACITY"] = pd.to_numeric(
                genunits_df["REGISTEREDCAPACITY"], errors="coerce"
            )
            genunits_df = genunits_df[
                genunits_df["GENSETID"].notna() & (genunits_df["GENSETID"] != "")
            ].drop_duplicates("GENSETID", keep="last")
            genunits_df.reset_index(drop=True).to_feather(genunits_cache)
            logger.info(f"GENUNITS table: {len(genunits_df)} units")
        except Exception as e:
            logger.warning(f"Could not fetch MMSDM GENUNITS table: {e}")
            genunits_df = pd.DataFrame(columns=["GENSETID", "STATIONID",
                                                 "REGISTEREDCAPACITY", "CO2E_ENERGY_SOURCE"])

    if "CO2E_ENERGY_SOURCE" in genunits_df.columns:
        genunits_df["FUEL_CATEGORY"] = (
            genunits_df["CO2E_ENERGY_SOURCE"].map(CO2E_TO_FUEL_MAP).fillna("Other")
        )

    return station_names, genunits_df


def fetch_generators(cache_dir: str, force: bool = False,
                     mmsdm_year: int | None = None,
                     mmsdm_month: int | None = None) -> pd.DataFrame:
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

    # Tier 1: parse Registration List
    df = _parse_registration_list(xls_path)

    # Tier 2: MMSDM GENUNITS — historical/deregistered DUIDs not in Registration List
    if mmsdm_year is not None and mmsdm_month is not None:
        try:
            station_names, genunits_df = _fetch_mmsdm_genunits_station(
                cache_dir, mmsdm_year, mmsdm_month
            )
            registered_duids = set(df["DUID"])
            new_rows = genunits_df[~genunits_df["GENSETID"].isin(registered_duids)].copy()
            new_rows = new_rows.rename(columns={
                "GENSETID": "DUID",
                "REGISTEREDCAPACITY": "CAPACITY_MW",
            })
            if "STATIONID" in new_rows.columns:
                new_rows["STATION_NAME"] = new_rows["STATIONID"].map(station_names)
            new_rows = new_rows.drop_duplicates(subset="DUID", keep="first")
            logger.info(f"MMSDM GENUNITS tier: {len(new_rows)} historical DUIDs added")
            df = pd.concat([df, new_rows], ignore_index=True)
        except Exception as e:
            logger.warning(f"MMSDM enrichment failed, proceeding with Registration List only: {e}")

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

    # Cache (includes Tier 2 enrichment)
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
