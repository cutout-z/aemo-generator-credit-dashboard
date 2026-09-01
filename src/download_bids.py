"""Download BIDPEROFFER_D FCAS offer data via NEMOSIS.

BIDPEROFFER_D (monthly MMSDM archive) carries per-DUID, per-interval,
per-service FCAS offers: MAXAVAIL (offered MW), ENABLEMENTMIN/MAX.
This is what makes a genuine per-generator FCAS participation factor
possible — regional DISPATCHPRICE averages cannot distinguish a unit that
offers 30 MW of Raise Reg around the clock from one that never offers.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from nemosis import dynamic_data_compiler

from . import config

logger = logging.getLogger(__name__)

FCAS_BID_TYPES = [
    "RAISE6SEC", "RAISE60SEC", "RAISE5MIN", "RAISEREG",
    "LOWER6SEC", "LOWER60SEC", "LOWER5MIN", "LOWERREG",
]


def fetch_fcas_bids_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download FCAS rows of BIDPEROFFER_D for one month via NEMOSIS.

    Returns DataFrame with columns:
        INTERVAL_DATETIME, DUID, BIDTYPE, MAXAVAIL, ENABLEMENTMIN, ENABLEMENTMAX
    Rows filtered to the 8 FCAS bid types; ENERGY bids are dropped here.
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching BIDPEROFFER_D (FCAS) for {year}-{month:02d}...")
    bids = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="BIDPEROFFER_D",
        raw_data_location=nemosis_cache,
        select_columns=[
            "INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL",
            "ENABLEMENTMIN", "ENABLEMENTMAX", "VERSIONNO",
        ],
        fformat="parquet",
        rebuild=rebuild,
    )

    if bids is None or bids.empty:
        logger.warning(f"No BIDPEROFFER_D data for {year}-{month:02d}")
        return pd.DataFrame()

    # Months whose archive is not yet published come back schemaless
    # (nemosis loads only the columns it finds). Fail soft — the factor step
    # simply skips this month — instead of KeyError-ing the whole run.
    required = {"INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL"}
    missing = required - set(bids.columns)
    if missing:
        logger.warning(
            f"BIDPEROFFER_D {year}-{month:02d}: missing columns {sorted(missing)} "
            "(archive not yet published?) — skipping month"
        )
        return pd.DataFrame()

    bids = bids[bids["BIDTYPE"].isin(FCAS_BID_TYPES)].copy()
    if bids.empty:
        logger.warning(f"BIDPEROFFER_D {year}-{month:02d}: no FCAS bid rows")
        return pd.DataFrame()

    # Rebids create multiple versions per (unit, service, interval):
    # the latest VERSIONNO is the operative offer. VERSIONNO is optional —
    # parquets cached before it was ever requested lack the column; then we
    # skip dedupe (slight offer-minute overstatement possible, logged).
    if "VERSIONNO" in bids.columns:
        bids["VERSIONNO"] = pd.to_numeric(bids["VERSIONNO"], errors="coerce")
        bids = bids.sort_values("VERSIONNO").drop_duplicates(
            subset=["INTERVAL_DATETIME", "DUID", "BIDTYPE"], keep="last"
        )
    else:
        logger.warning(
            f"BIDPEROFFER_D {year}-{month:02d}: VERSIONNO absent from cache — "
            "skipping rebid dedupe (delete the cached parquet to rebuild)"
        )

    bids["MAXAVAIL"] = pd.to_numeric(bids["MAXAVAIL"], errors="coerce")
    bids = bids.dropna(subset=["MAXAVAIL"])
    bids = bids[bids["MAXAVAIL"] > 0]
    bids["INTERVAL_DATETIME"] = pd.to_datetime(bids["INTERVAL_DATETIME"])

    logger.info(
        f"BIDPEROFFER_D FCAS {year}-{month:02d}: {len(bids):,} rows, "
        f"{bids['DUID'].nunique()} DUIDs offering"
    )
    return bids
