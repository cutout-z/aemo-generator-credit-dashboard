"""Download BIDPEROFFER_D offer data via NEMOSIS.

BIDPEROFFER_D (monthly MMSDM archive) carries per-DUID, per-interval,
per-service offers: MAXAVAIL (offered MW), ENABLEMENTMIN/MAX, plus the
per-interval ENERGY volume bands (BANDAVAIL1-10). This is what makes a genuine
per-generator FCAS participation factor possible — regional DISPATCHPRICE
averages cannot distinguish a unit that offers 30 MW of Raise Reg around the
clock from one that never offers.

S3-12 single-decode contract: every consumer in a run reads BIDPEROFFER_D from
the SAME raw frame, fetched ONCE per (table, month) via
``fetch_bidperoffer_union`` (union of all consumers' columns). The FCAS lane
slices FCAS bid rows, the offer lanes slice ENERGY volume rows — no consumer
re-invokes the nemosis compiler for a month the run already decoded. The
standalone ``fetch_fcas_bids_month`` remains as a thin fetch+slice wrapper for
direct callers.

Cache note: nemosis caches one parquet per (table, month) holding only the
columns previously requested. A cached parquet that lacks any required column
is healed ONCE by re-downloading it "fat" (rebuild=True) — the column set is
inspected from the parquet SCHEMA ONLY (no row decode), so the check itself
never pays for a full read.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

FCAS_BID_TYPES = [
    "RAISE6SEC", "RAISE60SEC", "RAISE5MIN", "RAISEREG",
    "LOWER6SEC", "LOWER60SEC", "LOWER5MIN", "LOWERREG",
]

# Column groups of the two BIDPEROFFER_D consumers in this pipeline:
#   - FCAS factors: MAXAVAIL/ENABLEMENTMIN/ENABLEMENTMAX (offer capacity)
#   - ENERGY offer volumes: BANDAVAIL1-10 (per-interval band volumes)
# VERSIONNO/OFFERDATE drive rebid dedupe and are best-effort: monthly archive
# vintages may lack them, so they are requested but never required for heal.
_BIDPEROFFER_KEYS = ["INTERVAL_DATETIME", "DUID", "BIDTYPE", "VERSIONNO"]
_BIDPEROFFER_FCAS_COLS = ["MAXAVAIL", "ENABLEMENTMIN", "ENABLEMENTMAX"]
BIDPEROFFER_UNION_COLS = (
    _BIDPEROFFER_KEYS
    + _BIDPEROFFER_FCAS_COLS
    + [f"BANDAVAIL{i}" for i in range(1, 11)]
)
# Columns a healthy fetch must actually carry (VERSIONNO may be absent from
# monthly archive CSVs; the dedupe then degrades with a warning).
BIDPEROFFER_REQUIRED_COLS = {
    "INTERVAL_DATETIME", "DUID", "BIDTYPE",
    "MAXAVAIL", "ENABLEMENTMIN", "ENABLEMENTMAX",
    *(f"BANDAVAIL{i}" for i in range(1, 11)),
}

_FCAS_REQUIRED = {"INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL"}


def _nemosis_cache_path(cache_dir: str, table: str, year: int, month: int) -> Path:
    return (
        Path(cache_dir) / "nemosis_cache"
        / f"PUBLIC_ARCHIVE#{table}#FILE01#{year}{month:02d}010000.parquet"
    )


def _cached_parquet_columns(cache_dir: str, table: str, year: int, month: int):
    """Column names of the cached (table, month) parquet, SCHEMA ONLY.

    Returns None when no cache file exists; returns a set of column names
    otherwise (empty when the file exists but cannot be read — treated as
    "cannot confirm coverage", which triggers a one-time rebuild). Never
    decodes the parquet's rows: only the file footer schema is read.
    """
    parquet = _nemosis_cache_path(cache_dir, table, year, month)
    if not parquet.exists():
        return None
    try:
        import pyarrow.parquet as pq

        return set(pq.ParquetFile(parquet).schema_arrow.names)
    except Exception as e:  # corrupt/unreadable — cannot confirm coverage
        logger.warning(
            "%s: cached parquet unreadable (%s) — rebuilding", parquet.name, e,
        )
        return set()


def fetch_bidperoffer_union(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool | None = None,
) -> pd.DataFrame:
    """Fetch one month of raw BIDPEROFFER_D (all bid types, union columns).

    This is the ONLY nemosis compiler call a run makes for BIDPEROFFER_D in a
    given month (S3-12). Consumers slice their rows/columns from the returned
    frame; nobody re-decodes the month.

    ``rebuild=None`` (default): inspect the cached parquet's SCHEMA ONLY and
    re-download once when it lacks any required column (a thin cache written
    by an older narrow fetch). ``rebuild=True`` forces a full re-download
    (``--full-refresh``); ``rebuild=False`` never re-downloads — consumers
    then tolerate whatever columns the cache holds.

    Returns the raw frame (unfiltered by BIDTYPE, undeduped) or an empty
    DataFrame when the month has no data (not yet published / no rows).
    """
    if rebuild is None:
        cols = _cached_parquet_columns(cache_dir, "BIDPEROFFER_D", year, month)
        rebuild = bool(
            cols is not None
            and not BIDPEROFFER_REQUIRED_COLS.issubset(cols)
        )
        if rebuild:
            logger.info(
                f"BIDPEROFFER_D {year}-{month:02d}: cached parquet lacks required "
                "columns — rebuilding fat (one-time)"
            )

    from nemosis import dynamic_data_compiler

    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching BIDPEROFFER_D {year}-{month:02d}...")
    bids = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="BIDPEROFFER_D",
        raw_data_location=nemosis_cache,
        select_columns=BIDPEROFFER_UNION_COLS,
        fformat="parquet",
        rebuild=rebuild,
    )

    if bids is None or bids.empty:
        logger.warning(f"No BIDPEROFFER_D data for {year}-{month:02d}")
        return pd.DataFrame()
    return bids


def fcas_bids_from_raw(
    raw: pd.DataFrame,
    year: int,
    month: int,
) -> pd.DataFrame:
    """Slice + post-process the FCAS rows out of a raw BIDPEROFFER_D frame.

    Mirrors the post-fetch steps of the historical ``fetch_fcas_bids_month``:
    requires the FCAS columns, keeps only the 8 FCAS bid types, dedupes to the
    latest offer version per (unit, service, interval) when VERSIONNO is
    present, and returns rows with columns:
        INTERVAL_DATETIME, DUID, BIDTYPE, MAXAVAIL, ENABLEMENTMIN, ENABLEMENTMAX
    ENERGY bids are dropped here. MAXAVAIL == 0 rows are KEPT (S3-05): an
    explicit zero offer is observed source coverage, not an absence.
    """
    if raw is None or raw.empty:
        return pd.DataFrame()

    # Months whose archive is not yet published come back schemaless
    # (nemosis loads only the columns it finds). Fail soft — the factor step
    # simply skips this month — instead of KeyError-ing the whole run.
    missing = _FCAS_REQUIRED - set(raw.columns)
    if missing:
        logger.warning(
            f"BIDPEROFFER_D {year}-{month:02d}: missing columns {sorted(missing)} "
            "(archive not yet published?) — skipping month"
        )
        return pd.DataFrame()

    bids = raw[raw["BIDTYPE"].isin(FCAS_BID_TYPES)].copy()
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
    # S3-05: retain explicit zero offers. A MAXAVAIL=0 row is an OBSERVED
    # offer (the unit was bidding that service at zero) — dropping it before
    # aggregation conflated "offered nothing" with "no offer on record" and
    # hid intervals from downstream coverage counts. Zero rows keep flowing;
    # factor aggregation separates observed intervals from positive ones.
    bids = bids.dropna(subset=["MAXAVAIL"])
    bids["INTERVAL_DATETIME"] = pd.to_datetime(bids["INTERVAL_DATETIME"])

    logger.info(
        f"BIDPEROFFER_D FCAS {year}-{month:02d}: {len(bids):,} rows, "
        f"{bids['DUID'].nunique()} DUIDs with offers "
        f"(incl. {int((bids['MAXAVAIL'] == 0).sum()):,} zero-offer rows)"
    )
    return bids


def fetch_fcas_bids_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download FCAS rows of BIDPEROFFER_D for one month via NEMOSIS.

    Thin wrapper kept for direct callers and the historical API: fetches the
    month's union raw frame (once) and slices the FCAS rows. Returns
    DataFrame with columns:
        INTERVAL_DATETIME, DUID, BIDTYPE, MAXAVAIL, ENABLEMENTMIN, ENABLEMENTMAX
    Rows filtered to the 8 FCAS bid types; ENERGY bids are dropped here.
    """
    raw = fetch_bidperoffer_union(year, month, cache_dir, rebuild=rebuild)
    return fcas_bids_from_raw(raw, year, month)
