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
from datetime import datetime
from pathlib import Path

import numpy as np
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
# DIRECTION (IESS, 2024-06) is best-effort too: pre-IESS archives lack it.
_BIDPEROFFER_KEYS = ["INTERVAL_DATETIME", "DUID", "BIDTYPE", "DIRECTION", "VERSIONNO"]
_BIDPEROFFER_FCAS_COLS = ["MAXAVAIL", "ENABLEMENTMIN", "ENABLEMENTMAX"]
BIDPEROFFER_UNION_COLS = (
    _BIDPEROFFER_KEYS
    + _BIDPEROFFER_FCAS_COLS
    + [f"BANDAVAIL{i}" for i in range(1, 11)]
)
# Columns a healthy fetch must actually carry (VERSIONNO may be absent from
# monthly archive CSVs; the dedupe then degrades with a warning. DIRECTION is
# absent before IESS; offer_direction_mask then treats every row as GEN).
BIDPEROFFER_REQUIRED_COLS = {
    "INTERVAL_DATETIME", "DUID", "BIDTYPE",
    "MAXAVAIL", "ENABLEMENTMIN", "ENABLEMENTMAX",
    *(f"BANDAVAIL{i}" for i in range(1, 11)),
}

_FCAS_REQUIRED = {"INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL"}
# The columns an FCAS slice carries (the raw frame's order is kept). The
# BANDAVAIL bands are ENERGY volumes: copying them for ~13M FCAS rows a month
# cost ~1 GiB the FCAS lane never reads.
_FCAS_SLICE_COLS = {
    "INTERVAL_DATETIME", "DUID", "BIDTYPE", "DIRECTION", "VERSIONNO", *_BIDPEROFFER_FCAS_COLS,
}

# ── Offer direction: the ONE rule every BIDPEROFFER_D / BIDDAYOFFER_D consumer uses ──
# Since IESS (June 2024) a bidirectional unit (a battery) offers each service
# per DIRECTION: ENERGY and regulation FCAS carry a GEN row AND a LOAD row
# for the same unit and interval (BIDDAYOFFER_D price bands differ by
# DIRECTION too); contingency FCAS carries one BIDIRECTIONAL row. Ordinary
# generators carry GEN, scheduled loads LOAD. This dashboard measures a
# unit's generating (discharge) side, so a consumer keeps:
#   - ENERGY, RAISEREG, LOWERREG: DIRECTION == "GEN" only;
#   - contingency FCAS: "GEN" or "BIDIRECTIONAL";
#   - "LOAD" rows: never (no consumer here measures charging).
# A frame without DIRECTION (pre-IESS archives), or a null DIRECTION in a
# mixed-vintage frame, is the old single-direction world: kept, as GEN.
# Without this, a (DUID, interval) dedupe kept GEN or LOAD by file order and
# the FCAS lane averaged both directions' MAXAVAIL.
DIRECTION_COL = "DIRECTION"
GEN_DIRECTION = "GEN"
BIDIRECTIONAL_DIRECTION = "BIDIRECTIONAL"
GEN_ONLY_BID_TYPES = frozenset({"ENERGY", "RAISEREG", "LOWERREG"})


# IESS start: from this date a bidirectional unit's rows carry DIRECTION, so a
# frame from on or after it without the column cannot separate GEN from LOAD
# (e.g. a parquet written by an older nemosis that only kept requested columns).
IESS_START = pd.Timestamp("2024-06-03")


def _warn_if_post_iess_without_direction(frame: pd.DataFrame) -> None:
    for col in ("INTERVAL_DATETIME", "SETTLEMENTDATE"):
        if col in frame.columns and len(frame):
            latest = pd.to_datetime(frame[col], errors="coerce").max()
            if pd.notna(latest) and latest >= IESS_START:
                logger.warning(
                    "Offer rows dated %s lack DIRECTION: battery GEN and LOAD offers "
                    "cannot be separated; the dedupe falls back to file order. "
                    "Rebuild that month's parquet with DIRECTION.", latest.date()
                )
            return


def offer_direction_mask(frame: pd.DataFrame) -> pd.Series | None:
    """Rows of a BIDPEROFFER_D / BIDDAYOFFER_D frame on the generating side.

    Returns a boolean Series aligned to ``frame``, or None when the frame has
    no DIRECTION column (keep every row). See the rule above.
    """
    if DIRECTION_COL not in frame.columns:
        _warn_if_post_iess_without_direction(frame)
        return None
    direction = frame[DIRECTION_COL]
    keep = direction.isna() | (direction == GEN_DIRECTION)
    if "BIDTYPE" in frame.columns:
        keep |= (direction == BIDIRECTIONAL_DIRECTION) & ~frame["BIDTYPE"].isin(GEN_ONLY_BID_TYPES)
    return keep


def generating_offer_rows(frame: pd.DataFrame, bid_types) -> pd.Series:
    """Rows of ``bid_types`` on the generating side: the slice every offer
    consumer takes (BIDTYPE filter AND ``offer_direction_mask``)."""
    rows = frame["BIDTYPE"].isin(bid_types)
    direction = offer_direction_mask(frame)
    return rows if direction is None else rows & direction


def offer_dedupe_keys(frame: pd.DataFrame, keys: list[str]) -> list[str]:
    """Dedupe keys for offer rows: ``keys`` plus DIRECTION when the frame has it,
    so a GEN offer and a LOAD offer are never collapsed into one row."""
    return keys + [DIRECTION_COL] if DIRECTION_COL in frame.columns else list(keys)


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


# ── Low-memory cached read (NAS incident 2026-10-12) ──────────────────────
# nemosis's compiler decodes every cached (table, month) parquet WHOLE before
# date-filtering, and for a month M it reads the M-1, M and M+1 archives
# (its month generator pushes the start back a month). A BIDPEROFFER_D month
# is ~20M rows x 21 columns, cached as strings: one fetch peaked at ~14 GiB,
# and a not-yet-published month still decoded all of M-1 (~7 GiB) to keep a
# few boundary hours. When the cache holds the month, read it here instead:
# row groups outside the window are skipped from the footer statistics, only
# the union columns are read, and each row group is filtered and typed before
# the next is read. The frame matches the compiler's exactly (rows, order,
# index, columns, dtypes); anything unexpected returns None and the caller
# falls back to the compiler.

_ARCHIVE_TS_FORMAT = "%Y/%m/%d %H:%M:%S"
_TEXT_COLS = {"DUID", "BIDTYPE", "DIRECTION"}


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + delta
    return idx // 12, idx % 12 + 1


def _has_required_cols(cache_dir: str, year: int, month: int) -> bool:
    cols = _cached_parquet_columns(cache_dir, "BIDPEROFFER_D", year, month)
    return cols is not None and BIDPEROFFER_REQUIRED_COLS.issubset(cols)


def _stat_ts(value) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, bytes):
        value = value.decode()
    try:
        return datetime.strptime(str(value), _ARCHIVE_TS_FORMAT)
    except ValueError:
        return None


def _typed_ts(col):
    import pyarrow as pa
    import pyarrow.compute as pc

    if pa.types.is_timestamp(col.type):
        return col.cast(pa.timestamp("us"))
    return pc.strptime(col, format=_ARCHIVE_TS_FORMAT, unit="us", error_is_null=False)


def _typed_number(col):
    """A row group's numeric column, typed as pd.to_numeric types it:
    int64 when every value is an integer string, else float64."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if pa.types.is_integer(col.type):
        return col.cast(pa.int64())
    if (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)) \
            and not col.null_count \
            and pc.all(pc.match_substring_regex(col, r"^[+-]?[0-9]+$")).as_py():
        return col.cast(pa.int64())
    return col.cast(pa.float64())


def _window_parts(path: Path, start: datetime, end: datetime):
    """Typed row-group tables of one cached parquet, start < INTERVAL_DATETIME <= end.

    Returns (columns in file order, tables, each table's row positions in the
    file). nemosis leaves those positions as the frame's index.
    Raises on any parse/cast problem (the caller falls back to nemosis).
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    names = pf.schema_arrow.names
    union = set(BIDPEROFFER_UNION_COLS)
    cols = [c for c in names if c in union]  # file order, as nemosis selects
    ts_idx = names.index("INTERVAL_DATETIME")
    lo, hi = pa.scalar(start, pa.timestamp("us")), pa.scalar(end, pa.timestamp("us"))

    parts: list = []
    positions: list[np.ndarray] = []
    offset = 0
    for g in range(pf.metadata.num_row_groups):
        n = pf.metadata.row_group(g).num_rows
        stats = pf.metadata.row_group(g).column(ts_idx).statistics
        if stats is not None and stats.has_min_max:
            gmin, gmax = _stat_ts(stats.min), _stat_ts(stats.max)
            if gmin is not None and gmax is not None and (gmax <= start or gmin > end):
                offset += n
                continue
        table = pf.read_row_group(g, columns=cols)
        ts = _typed_ts(table.column("INTERVAL_DATETIME"))
        mask = pc.and_(pc.greater(ts, lo), pc.less_equal(ts, hi))
        keep = np.flatnonzero(mask.to_numpy(zero_copy_only=False))
        if keep.size:
            table = table.set_column(cols.index("INTERVAL_DATETIME"), "INTERVAL_DATETIME", ts)
            if keep.size < n:  # most groups sit wholly in the window: no copy
                table = table.take(pa.array(keep))
            for i, name in enumerate(cols):
                col = table.column(i)
                if name in _TEXT_COLS:
                    col = col.cast(pa.large_string())
                elif name != "INTERVAL_DATETIME":
                    col = _typed_number(col)
                table = table.set_column(i, name, col)
            parts.append(table.replace_schema_metadata(None))
            positions.append(keep + offset)
        offset += n
    return cols, parts, positions


def _parts_to_frame(cols: list[str], parts: list, positions: list[np.ndarray]) -> pd.DataFrame:
    import pyarrow as pa

    if not parts:
        return pd.DataFrame(columns=cols)
    # pd.to_numeric types the WHOLE frame, so a column that is int64 in some
    # row groups and float64 in others is float64 throughout. Replaced in
    # place: the caller's list must not keep the uncast tables alive.
    for i, name in enumerate(cols):
        if len({t.schema.field(i).type for t in parts}) > 1:
            for j, t in enumerate(parts):
                parts[j] = t.set_column(i, name, t.column(i).cast(pa.float64()))
    table = pa.concat_tables(parts)
    # The table is now the only owner of the row groups' buffers, so
    # self_destruct can free each Arrow column as soon as it is converted.
    parts.clear()
    # The parquet's stored pandas metadata was dropped per part: it records
    # the cached string dtypes and would cast the typed columns back.
    frame = table.to_pandas(split_blocks=True, self_destruct=True)
    del table
    frame.index = pd.Index(np.concatenate(positions))
    return frame


def _cache_month(year: int, month: int, nemosis_cache: str, start_time: str, end_time: str) -> None:
    """Download + cache an uncached month without decoding the cached ones.

    nemosis's caching mode skips files already on disk and writes a parquet
    for each archive it downloads. An unpublished month just logs "not
    downloaded", as the compiler does.
    """
    from nemosis import cache_compiler

    try:
        cache_compiler(
            start_time, end_time, "BIDPEROFFER_D", nemosis_cache,
            fformat="parquet", keep_csv=True,
        )
    except Exception as e:  # network/AEMO trouble: read what is cached
        logger.warning(f"BIDPEROFFER_D {year}-{month:02d}: caching attempt failed ({e})")


def _read_cached_window(
    year: int, month: int, cache_dir: str, start_time: str, end_time: str,
) -> pd.DataFrame | None:
    """The month's union frame read from cache, or None to use the compiler.

    Needs the M-1 archive cached with every required column (it carries the
    month's first trading-day hours). An uncached month M is fetched in
    caching mode first; while AEMO has not published it the window holds
    only the M-1 boundary rows, exactly what the compiler returns.
    """
    if not _has_required_cols(cache_dir, *_shift_month(year, month, -1)):
        return None
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    if _cached_parquet_columns(cache_dir, "BIDPEROFFER_D", year, month) is None:
        _cache_month(year, month, nemosis_cache, start_time, end_time)
        if _cached_parquet_columns(cache_dir, "BIDPEROFFER_D", year, month) is not None \
                and not _has_required_cols(cache_dir, year, month):
            return None

    start = datetime.strptime(start_time, _ARCHIVE_TS_FORMAT)
    end = datetime.strptime(end_time, _ARCHIVE_TS_FORMAT)
    try:
        pieces = []
        for delta in (-1, 0, 1):
            path = _nemosis_cache_path(cache_dir, "BIDPEROFFER_D", *_shift_month(year, month, delta))
            if path.exists():
                pieces.append(_window_parts(path, start, end))
        if len({tuple(cols) for cols, _, _ in pieces}) == 1:
            # One conversion for all files: no pd.concat copy of the month.
            cols = pieces[0][0]
            parts = [t for _, ps, _ in pieces for t in ps]
            positions = [p for _, _, pos in pieces for p in pos]
            # Drop every other reference to the row-group tables first. While
            # `pieces` held them, to_pandas(self_destruct=True) freed nothing,
            # so the month was resident twice, as Arrow and as pandas (~2 GiB
            # more at the read's peak; NAS 2026-10-19).
            pieces.clear()
            frame = _parts_to_frame(cols, parts, positions)
        else:
            frame = pd.concat([_parts_to_frame(*piece) for piece in pieces], sort=False)
    except Exception as e:
        logger.warning(
            f"BIDPEROFFER_D {year}-{month:02d}: low-memory cache read failed ({e}) "
            "— falling back to the nemosis compiler"
        )
        return None
    logger.info(f"Read BIDPEROFFER_D {year}-{month:02d} from cache (low-memory path)")
    return frame


def fetch_bidperoffer_union(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool | None = None,
) -> pd.DataFrame:
    """Fetch one month of raw BIDPEROFFER_D (all bid types, union columns).

    This is the ONLY decode a run makes for BIDPEROFFER_D in a given month
    (S3-12). Consumers slice their rows/columns from the returned frame;
    nobody re-decodes the month. A month whose archives are already cached is
    read by the low-memory path above, not the nemosis compiler.

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

    if not rebuild:
        bids = _read_cached_window(year, month, cache_dir, start_time, end_time)
        if bids is not None:
            if bids.empty:
                logger.warning(f"No BIDPEROFFER_D data for {year}-{month:02d}")
                return pd.DataFrame()
            return bids

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
    requires the FCAS columns, keeps only the 8 FCAS bid types on the
    generating side (``offer_direction_mask``: regulation GEN, contingency GEN
    or BIDIRECTIONAL), dedupes to the latest offer version per (unit, service,
    direction, interval) when VERSIONNO is present, and returns rows with
    columns (plus DIRECTION when the archive carries it):
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

    cols = [c for c in raw.columns if c in _FCAS_SLICE_COLS]
    bids = raw.loc[generating_offer_rows(raw, FCAS_BID_TYPES), cols].copy()
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
            subset=offer_dedupe_keys(bids, ["INTERVAL_DATETIME", "DUID", "BIDTYPE"]), keep="last"
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
    if bids["MAXAVAIL"].isna().any():  # no whole-frame copy when nothing drops
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
