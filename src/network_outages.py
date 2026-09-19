"""AEMO MMSDM NETWORK_OUTAGEDETAIL monthly lane: parse-and-slice transmission outages.

The MMSDM monthly ``NETWORK_OUTAGEDETAIL`` archive is the transmission outage
lifecycle register: one row per outage window (LINE / CB / TRANS / BUS / CAP /
SVC / REAC …) with the submitted start/end, the actual start/end and a status
code (``WDRAWN`` withdrawn, ``COMPLETE``, ``SUBMIT``/``UTP``/``MTLTP``/``PTP``…
planned or in progress). It is the physical explanation behind the things this
dashboard already tracks: an outage of a 500 kV line or an interconnector
transformer is a leading indicator for MLF deterioration and for the
curtailment/constraint behaviour a generator's capture price sits on top of.

Access notes (live-verified 2026-09-19; see docs/FUTURE_DATA_SOURCES.md):

- Monthly route: ``PUBLIC_ARCHIVE%23NETWORK_OUTAGEDETAIL%23FILE01%23{YYYYMM}010000.zip``
  under the standard MMSDM archive path. Each zip is ~23 MB and extracts to a
  ~205 MB **full-history** CSV (2002 → present, ~896k rows) — not a monthly
  slice. The lane therefore parses-and-slices: it streams the CSV, keeps only
  the windows overlapping the target months (plus AEMO's standing windows),
  and deletes the raw zip + CSV before returning. Storage never grows with the
  archive.
- **Availability correction**: the spec recorded "exists only from MMSDM_2026_07,
  older months 404, no monthly backfill route". That is stale — the monthly
  route was live-probed back through 2024_06…2026_08 (only 2024_06 answered
  404), so a monthly backfill exists after all.
- **Join contract**: OUTAGEDETAIL carries no REGIONID and no VOLTAGE. Region
  comes from ``NETWORK_RATING`` (exact SUBSTATIONID+EQUIPMENTTYPE+EQUIPMENTID,
  falling back to the substation's majority region) and then from
  ``NETWORK_SUBSTATIONDETAIL`` (substation-level REGIONID); voltage comes from
  ``NETWORK_EQUIPMENTDETAIL`` (same key, latest VALIDFROM). A window whose
  substation joins nowhere is dropped with a warning and counted — it is never
  zero-filled or assigned to a guessed region.
- Far-future windows (start year ≥ ``config.NETWORK_OUTAGE_STANDING_YEAR``:
  observed 2098/2099/2100/2202) are AEMO's standing/recurring windows, not data
  errors. They are kept and flagged ``standing_window``.

Published artifact: ``docs/data/network_outages.json`` — per-region outage-days
by voltage class for the processed months, the active-window list and the
standing windows, written through the S3-12 semantic-diff publish gate.
Machine-local cache: ``data/network_outages/`` (per-month slices + the
accumulated window-month snapshot), never committed.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from . import config
from .factor_cache import merge_month_rows
from .run_status import STATUS_DEGRADED, STATUS_ERROR, STATUS_OK
from .semantic_publish import write_json_if_facts_changed

logger = logging.getLogger(__name__)

# ─── Source coordinates ────────────────────────────────────────────────────

OUTAGE_TABLE = "NETWORK_OUTAGEDETAIL"
# MMS flat files label rows as ``D,<group>,<table>,...`` — the group is NETWORK
# and the row-level table token drops the group prefix, so row matching uses
# this, while archive/member names use OUTAGE_TABLE above.
OUTAGE_MMS_TABLE = "OUTAGEDETAIL"
EQUIPMENT_TABLE = "NETWORK_EQUIPMENTDETAIL"
RATING_TABLE = "NETWORK_RATING"
SUBSTATION_TABLE = "NETWORK_SUBSTATIONDETAIL"
SUPPORT_TABLES = (EQUIPMENT_TABLE, RATING_TABLE, SUBSTATION_TABLE)
# MMS row-level table tokens for the context tables (group prefix stripped).
EQUIPMENT_MMS_TABLE = "EQUIPMENTDETAIL"
RATING_MMS_TABLE = "RATING"
SUBSTATION_MMS_TABLE = "SUBSTATIONDETAIL"


ROUTE_MONTHLY = "mmsdm_monthly"

# Outage statuses. Withdrawn windows are kept in the snapshot (audit trail) but
# excluded from the outage-days metric: a withdrawn request never took plant
# out of service.
STATUS_WITHDRAWN = frozenset({"WDRAWN", "WD REQ"})
# Statuses that mean the window is closed and observed (kept, counted).
STATUS_COMPLETE = frozenset({"COMPLETE"})

VOLTAGE_UNKNOWN = "unknown"
# Ordered voltage bands → the class label published in the artifact.
VOLTAGE_BANDS = (
    (500, "500kV"),
    (330, "330kV"),
    (275, "275kV"),
    (220, "220kV"),
    (110, "110-132kV"),
    (33, "33-66kV"),
)
VOLTAGE_LOW_BAND = "<33kV"

# Published window lists are bounded so the artifact stays dashboard-sized.
MAX_ACTIVE_WINDOWS = 60
MAX_STANDING_WINDOWS = 40

MIN_ZIP_BYTES = 5_000
ZIP_MAGIC = b"PK\x03\x04"

# Output filenames (local cache + published artifact).
SNAPSHOT_DIRNAME = "network_outages"
SNAPSHOT_FILENAME = "network_outage_snapshot.feather"
STANDING_FILENAME = "network_outage_standing.feather"
META_FILENAME = "network_outage_meta.json"
ARTIFACT_FILENAME = "network_outages.json"
MONTH_SLICE_TEMPLATE = "network_outages_{ym}.feather"

# Columns kept from the 205 MB full-history CSV (the rest are dropped at parse).
OUTAGE_COLUMNS = (
    "OUTAGEID", "SUBSTATIONID", "EQUIPMENTTYPE", "EQUIPMENTID", "ELEMENTID",
    "STARTTIME", "ENDTIME", "ACTUAL_STARTTIME", "ACTUAL_ENDTIME",
    "SUBMITTEDDATE", "OUTAGESTATUSCODE", "REASON", "ISSECONDARY", "LASTCHANGED",
)

# Parsed-window frame columns (before the context join adds region/voltage).
WINDOW_COLUMNS = (
    "outage_id", "substation_id", "equipment_type", "equipment_id", "element_id",
    "status_code", "reason", "is_secondary", "start_time", "end_time",
    "actual_start_time", "actual_end_time", "submitted_date", "last_changed",
    "standing_window",
)

SNAPSHOT_COLUMNS = (
    "month", "outage_id", "region", "region_source", "voltage", "voltage_class",
    "substation_id", "equipment_type", "equipment_id", "element_id",
    "status_code", "start_time", "end_time", "actual_start_time",
    "actual_end_time", "submitted_date", "last_changed", "reason",
    "is_secondary", "outage_days", "open_window", "standing_window",
    "withdrawn",
)


class NetworkOutageError(RuntimeError):
    """Base failure for the network-outage lane (fetch/parse/join)."""


class NetworkOutageAccessError(NetworkOutageError):
    """AEMO refused the request (403/CDN block) — never retried, never hammered."""


# ─── HTTP plumbing (same shape as the GenInfo lane) ────────────────────────

_SESSION: requests.Session | None = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": config.USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": "en-AU,en;q=0.9",
        })
        _SESSION = s
    return _SESSION


def _request(method: str, url: str, *, timeout: int | None = None) -> requests.Response:
    """One HTTP call with bounded retries and backoff (403 = fail fast)."""
    timeout = timeout or config.REQUEST_TIMEOUT
    last_error: Exception | None = None
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = _session().request(method, url, timeout=timeout, allow_redirects=True)
        except requests.RequestException as e:
            last_error = e
        else:
            if resp.status_code == 403:
                raise NetworkOutageAccessError(f"HTTP 403 (block/rate-limit) for {url}")
            if resp.status_code < 400:
                return resp
            if resp.status_code == 404:
                raise NetworkOutageError(f"HTTP 404 for {url}")
            last_error = NetworkOutageError(f"HTTP {resp.status_code} for {url}")
        if attempt < config.MAX_RETRIES - 1:
            wait = config.RETRY_BACKOFF * (attempt + 1)
            logger.warning(
                "Network outage request failed (%s, attempt %d/%d) — retrying in %ds",
                last_error, attempt + 1, config.MAX_RETRIES, wait,
            )
            time.sleep(wait)
    raise NetworkOutageError(f"request failed after {config.MAX_RETRIES} attempts: {last_error}")


# ─── Month discovery ───────────────────────────────────────────────────────

def month_url(year: int, month: int) -> str:
    return config.NETWORK_OUTAGE_URL_TEMPLATE.format(year=year, month=month)


def support_url(table: str, year: int, month: int) -> str:
    return config.NETWORK_SUPPORT_URL_TEMPLATE.format(table=table, year=year, month=month)


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    total = (year * 12 + (month - 1)) + delta
    return total // 12, total % 12 + 1


def probe_month(year: int, month: int) -> dict | None:
    """Cheap existence probe for a month's outage zip (HEAD only)."""
    url = month_url(year, month)
    try:
        resp = _request("HEAD", url)
    except NetworkOutageError as e:
        logger.info("Network outage probe miss for %04d-%02d (%s)", year, month, e)
        return None
    length = resp.headers.get("content-length")
    if length is not None and int(length) < MIN_ZIP_BYTES:
        return None
    return {
        "year": year,
        "month": month,
        "url": url,
        "content_length": int(length) if length is not None else None,
        "last_modified": resp.headers.get("last-modified"),
    }


def discover_month(
    today: date | None = None,
    *,
    months_back: int | None = None,
    probe=None,
) -> dict | None:
    """Newest month whose NETWORK_OUTAGEDETAIL zip AEMO is serving, else None.

    Probes the current month backwards (AEMO publishes the monthly archive ~2
    weeks after month-end, so the current month normally 404s). ``probe`` is
    injectable for hermetic tests.
    """
    today = today or date.today()
    probe = probe or probe_month
    back = config.NETWORK_OUTAGE_PROBE_MONTHS_BACK if months_back is None else months_back
    for delta in range(0, max(1, back) + 1):
        year, month = _shift_month(today.year, today.month, -delta)
        info = probe(year, month)
        if info:
            return info
    return None


# ─── Download / extraction (parse-and-slice) ───────────────────────────────

def download_zip(url: str, dest: Path) -> dict:
    """Download an MMSDM monthly zip to ``dest`` (atomic) and validate it."""
    resp = _request("GET", url, timeout=max(config.REQUEST_TIMEOUT, 300))
    content = resp.content
    if len(content) < MIN_ZIP_BYTES or content[:4] != ZIP_MAGIC:
        raise NetworkOutageError(
            f"{url}: response is not a zip archive ({len(content)} bytes, "
            f"magic={content[:4]!r})"
        )
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    tmp.write_bytes(content)
    tmp.replace(dest)
    length = resp.headers.get("content-length")
    logger.info("Network outage archive downloaded: %d KB → %s", len(content) // 1024, dest)
    return {
        "url": url,
        "content_length": int(length) if length else len(content),
        "last_modified": resp.headers.get("last-modified"),
    }


def extract_table_csv(zip_path: str | Path, dest_dir: str | Path, table: str) -> Path:
    """Extract the ``table`` CSV member of an MMSDM zip into ``dest_dir``."""
    zip_path = Path(zip_path)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        members = [
            n for n in zf.namelist()
            if table in n.upper() and n.upper().endswith((".CSV", ".csv"))
        ]
        if not members:
            raise NetworkOutageError(f"{zip_path.name}: no {table} CSV member ({zf.namelist()})")
        member = members[0]
        zf.extract(member, dest_dir)
    return dest_dir / Path(member).name


# ─── MMS CSV parsing ───────────────────────────────────────────────────────

def _text(value) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def parse_mms_timestamp(value) -> pd.Timestamp | None:
    """Parse an MMS date/datetime cell ('2026/08/01 07:00:00[.000]') → Timestamp."""
    text = _text(value)
    if text is None:
        return None
    text = text.split(".")[0]
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return pd.Timestamp(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return None


def iter_mms_rows(path: str | Path):
    """Yield ``(table, row_dict)`` for every ``D`` record in an MMS CSV file.

    MMS flat files carry a ``C`` comment row, one ``I`` header row per table
    and ``D`` data rows; every row is prefixed by ``D,<group>,<table>,<version>``.
    """
    path = Path(path)
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        headers: dict[str, list[str]] = {}
        for parts in reader:
            if not parts:
                continue
            kind = parts[0].strip().upper()
            if kind == "I" and len(parts) >= 5:
                headers[parts[2].strip().upper()] = parts[4:]
            elif kind == "D" and len(parts) >= 5:
                table = parts[2].strip().upper()
                header = headers.get(table)
                if not header:
                    continue
                yield table, dict(zip(header, parts[4:]))


def _window_bounds(start, end, standing: bool) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Normalise a window's bounds (standing/far-future ends read as open)."""
    if end is not None and standing:
        end = None
    return start, end


def _row_standing(start: pd.Timestamp | None) -> bool:
    return start is not None and start.year >= config.NETWORK_OUTAGE_STANDING_YEAR


def _window_overlaps(start, end, lo: pd.Timestamp, hi: pd.Timestamp) -> bool:
    """True when [start, end] intersects [lo, hi); an open end runs to +inf."""
    if start is None:
        return False
    if start >= hi:
        return False
    if end is None:
        return True
    return end >= lo


def parse_outage_windows(
    path: str | Path,
    *,
    keep_months: list[tuple[int, int]] | None = None,
    chunk_rows: int = 50_000,
) -> pd.DataFrame:
    """Stream the full-history outage CSV into a typed window frame.

    The file is ~205 MB / ~896k rows of full history. Rows are decoded in
    chunks and immediately filtered to the windows that overlap ``keep_months``
    (plus every standing window, which never overlaps a target month by
    construction), so only the slice ever materialises in memory.
    """
    windows = [
        (
            (pd.Timestamp(y, m, 1), pd.Timestamp(y, m, 1) + pd.offsets.MonthBegin(1))
        )
        for y, m in (keep_months or [])
    ]

    frames: list[pd.DataFrame] = []
    chunk: list[dict] = []
    kept = 0
    scanned = 0

    def _flush() -> None:
        nonlocal chunk
        if not chunk:
            return
        frames.append(pd.DataFrame(chunk))
        chunk = []

    for table, row in iter_mms_rows(path):
        if table != OUTAGE_MMS_TABLE:
            continue
        scanned += 1
        start = parse_mms_timestamp(row.get("STARTTIME"))
        standing = _row_standing(start)
        record = None
        keep = standing
        if not keep and windows and start is not None:
            end = parse_mms_timestamp(row.get("ENDTIME"))
            if end is not None and end.year >= config.NETWORK_OUTAGE_STANDING_YEAR:
                end = None
            keep = any(_window_overlaps(start, end, lo, hi) for lo, hi in windows)
        elif not windows:
            keep = True
        if keep:
            record = {
                "outage_id": _text(row.get("OUTAGEID")),
                "substation_id": _text(row.get("SUBSTATIONID")),
                "equipment_type": _text(row.get("EQUIPMENTTYPE")),
                "equipment_id": _text(row.get("EQUIPMENTID")),
                "element_id": _text(row.get("ELEMENTID")),
                "status_code": _text(row.get("OUTAGESTATUSCODE")),
                "reason": _text(row.get("REASON")),
                "is_secondary": _text(row.get("ISSECONDARY")),
                "start_time": start,
                "end_time": parse_mms_timestamp(row.get("ENDTIME")),
                "actual_start_time": parse_mms_timestamp(row.get("ACTUAL_STARTTIME")),
                "actual_end_time": parse_mms_timestamp(row.get("ACTUAL_ENDTIME")),
                "submitted_date": parse_mms_timestamp(row.get("SUBMITTEDDATE")),
                "last_changed": parse_mms_timestamp(row.get("LASTCHANGED")),
                "standing_window": standing,
            }
            kept += 1
            chunk.append(record)
            if len(chunk) >= chunk_rows:
                _flush()
    _flush()

    logger.info(
        "Network outage parse: %d window(s) kept of %d OUTAGEDETAIL row(s) scanned",
        kept, scanned,
    )
    if not frames:
        return pd.DataFrame(columns=list(WINDOW_COLUMNS))
    frame = pd.concat(frames, ignore_index=True)
    return frame


def parse_context_table(path: str | Path) -> pd.DataFrame:
    """Parse a small MMS context table (equipment detail / rating / substation)."""
    rows = []
    for table, row in iter_mms_rows(path):
        row = dict(row)
        row["_table"] = table
        rows.append(row)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ─── Region / voltage context ──────────────────────────────────────────────

def voltage_class(voltage) -> str:
    """Bucket a kV value into the published voltage class label."""
    if voltage is None:
        return VOLTAGE_UNKNOWN
    try:
        value = float(voltage)
    except (TypeError, ValueError):
        return VOLTAGE_UNKNOWN
    if value != value or value <= 0:  # NaN / nonsense → unknown, never zero-filled
        return VOLTAGE_UNKNOWN
    for threshold, label in VOLTAGE_BANDS:
        if value >= threshold:
            return label
    return VOLTAGE_LOW_BAND


def load_voltage_map(path: str | Path) -> pd.DataFrame:
    """VOLTAGE per (substation, equipmenttype, equipmentid), latest VALIDFROM wins."""
    df = parse_context_table(path)
    if df.empty or "SUBSTATIONID" not in df.columns:
        return pd.DataFrame(columns=["substation_id", "equipment_type", "equipment_id", "voltage"])
    df = df.copy()
    df["_validfrom"] = df.get("VALIDFROM", pd.Series([None] * len(df))).map(parse_mms_timestamp)
    df["voltage"] = pd.to_numeric(df.get("VOLTAGE"), errors="coerce")
    df = df[df["voltage"].notna() & (df["voltage"] > 0)]
    df = df.rename(columns={
        "SUBSTATIONID": "substation_id",
        "EQUIPMENTTYPE": "equipment_type",
        "EQUIPMENTID": "equipment_id",
    })
    df = df.sort_values("_validfrom", na_position="first")
    df = df.drop_duplicates(
        subset=["substation_id", "equipment_type", "equipment_id"], keep="last",
    )
    return df[["substation_id", "equipment_type", "equipment_id", "voltage"]]


def load_region_map(path: str | Path) -> pd.DataFrame:
    """REGIONID per (substation, equipmenttype, equipmentid), latest VALIDFROM wins."""
    df = parse_context_table(path)
    if df.empty or "SUBSTATIONID" not in df.columns or "REGIONID" not in df.columns:
        return pd.DataFrame(columns=["substation_id", "equipment_type", "equipment_id", "region"])
    df = df.copy()
    df["_validfrom"] = df.get("VALIDFROM", pd.Series([None] * len(df))).map(parse_mms_timestamp)
    df["region"] = df["REGIONID"].map(_text)
    df = df[df["region"].notna()]
    df = df.rename(columns={
        "SUBSTATIONID": "substation_id",
        "EQUIPMENTTYPE": "equipment_type",
        "EQUIPMENTID": "equipment_id",
    })
    df = df.sort_values("_validfrom", na_position="first")
    df = df.drop_duplicates(
        subset=["substation_id", "equipment_type", "equipment_id"], keep="last",
    )
    return df[["substation_id", "equipment_type", "equipment_id", "region"]]


def load_substation_regions(path: str | Path) -> dict[str, str]:
    """Substation-level REGIONID map (the fallback the live probe showed is needed)."""
    df = parse_context_table(path)
    if df.empty or "SUBSTATIONID" not in df.columns or "REGIONID" not in df.columns:
        return {}
    df = df.copy()
    df["region"] = df["REGIONID"].map(_text)
    df = df[df["region"].notna()]
    if df.empty:
        return {}
    return (
        df.sort_values("SUBSTATIONID")
        .drop_duplicates(subset=["SUBSTATIONID"], keep="last")
        .set_index("SUBSTATIONID")["region"]
        .to_dict()
    )


def _substation_majority_regions(region_map: pd.DataFrame) -> dict[str, str]:
    """Majority region of a substation, derived from its rated equipment rows."""
    if region_map is None or region_map.empty:
        return {}
    counts = (
        region_map.groupby(["substation_id", "region"]).size()
        .reset_index(name="n")
        .sort_values(["substation_id", "n"], ascending=[True, False])
    )
    return counts.drop_duplicates(subset=["substation_id"], keep="first").set_index(
        "substation_id",
    )["region"].to_dict()


def attach_context(
    windows: pd.DataFrame,
    voltage_map: pd.DataFrame,
    region_map: pd.DataFrame,
    substation_regions: dict[str, str],
) -> pd.DataFrame:
    """Join region + voltage onto window rows; never invent either.

    Region resolution order (each step documented in the artifact's
    ``region_source``): rated-equipment exact match → substation majority region
    from the rating table → substation-level NETWORK_SUBSTATIONDETAIL → dropped
    (``region`` stays null and the row is counted as unjoined). Voltage is the
    exact equipment match, else ``unknown`` (kept: voltage is context for the
    rollup, not window identity).
    """
    if windows is None or windows.empty:
        out = windows.copy() if windows is not None else pd.DataFrame()
        for col in ("region", "region_source", "voltage", "voltage_class"):
            out[col] = pd.Series(dtype="object")
        return out

    df = windows.copy()
    key = ["substation_id", "equipment_type", "equipment_id"]

    if voltage_map is not None and not voltage_map.empty:
        df = df.merge(
            voltage_map.rename(columns={"voltage": "_voltage"}),
            on=key, how="left",
        )
    else:
        df["_voltage"] = None

    df["region"] = None
    df["region_source"] = None
    if region_map is not None and not region_map.empty:
        df = df.merge(
            region_map.rename(columns={"region": "_region_exact"}), on=key, how="left",
        )
    else:
        df["_region_exact"] = None

    found = df["_region_exact"].notna()
    df.loc[found, "region"] = df.loc[found, "_region_exact"]
    df.loc[found, "region_source"] = "equipment_match"

    if df["region"].isna().any():
        majority = _substation_majority_regions(region_map)
        if majority:
            fallback = df["substation_id"].map(majority)
            use = df["region"].isna() & fallback.notna()
            df.loc[use, "region"] = fallback[use]
            df.loc[use, "region_source"] = "substation_majority_rating"

    if df["region"].isna().any() and substation_regions:
        fallback = df["substation_id"].map(substation_regions)
        use = df["region"].isna() & fallback.notna()
        df.loc[use, "region"] = fallback[use]
        df.loc[use, "region_source"] = "substation_detail"

    df["voltage"] = df["_voltage"]
    df["voltage_class"] = df["voltage"].map(voltage_class)
    df = df.drop(columns=["_voltage", "_region_exact"], errors="ignore")
    return df


# ─── Windowing / aggregation ───────────────────────────────────────────────

def window_open(end) -> bool:
    """True when a window has no effective end (open or far-future standing end)."""
    if end is None or (isinstance(end, float) and end != end):
        return True
    ts = pd.Timestamp(end)
    return ts.year >= config.NETWORK_OUTAGE_STANDING_YEAR


def outage_days_in_month(start, end, year: int, month: int) -> float:
    """Days of a window that fall inside the calendar month (0 when no overlap)."""
    lo = pd.Timestamp(year, month, 1)
    hi = lo + pd.offsets.MonthBegin(1)
    if start is None:
        return 0.0
    start = pd.Timestamp(start)
    eff_end = None if window_open(end) else pd.Timestamp(end)
    if start >= hi:
        return 0.0
    if eff_end is not None and eff_end < lo:
        return 0.0
    span_start = max(start, lo)
    span_end = hi if eff_end is None else min(eff_end, hi)
    days = (span_end - span_start).total_seconds() / 86400.0
    return round(max(0.0, days), 2)


def slice_month(frame: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Per-month fact rows: one row per window overlapping that month."""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(SNAPSHOT_COLUMNS))
    df = frame.copy()
    df["outage_days"] = [
        outage_days_in_month(r.start_time, r.end_time, year, month)
        for r in df.itertuples()
    ]
    df["open_window"] = df["end_time"].map(window_open)
    df["withdrawn"] = df["status_code"].isin(STATUS_WITHDRAWN)
    df = df[df["outage_days"] > 0]
    df["month"] = f"{year:04d}-{month:02d}"
    for col in SNAPSHOT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[list(SNAPSHOT_COLUMNS)].reset_index(drop=True)


def aggregate_by_region_voltage(facts: pd.DataFrame) -> dict:
    """Per-region outage-days by voltage class (withdrawn windows excluded)."""
    out: dict[str, dict] = {}
    if facts is None or facts.empty:
        return out
    df = facts[~facts["withdrawn"].astype(bool)]
    df = df[df["region"].notna()]
    for region, group in df.groupby("region"):
        entry = {"outage_days": 0.0, "windows": 0, "open_windows": 0, "by_voltage": {}}
        entry["windows"] = int(pd.unique(group["outage_id"]).size)
        entry["outage_days"] = round(float(group["outage_days"].sum()), 2)
        entry["open_windows"] = int(group.loc[group["open_window"].astype(bool), "outage_id"].nunique())
        for vclass, vgroup in group.groupby("voltage_class"):
            entry["by_voltage"][str(vclass)] = {
                "outage_days": round(float(vgroup["outage_days"].sum()), 2),
                "windows": int(pd.unique(vgroup["outage_id"]).size),
            }
        out[str(region)] = entry
    return out


def _window_records(facts: pd.DataFrame, *, limit: int) -> list[dict]:
    """Active/upcoming window records (withdrawn excluded), newest facts first."""
    if facts is None or facts.empty:
        return []
    df = facts[~facts["withdrawn"].astype(bool)]
    df = df[~df["standing_window"].astype(bool)]
    df = df.sort_values("outage_days", ascending=False).head(limit)
    records = []
    for row in df.itertuples():
        records.append({
            "outage_id": row.outage_id,
            "region": row.region,
            "region_source": row.region_source,
            "voltage": None if pd.isna(row.voltage) else float(row.voltage),
            "voltage_class": row.voltage_class,
            "substation": row.substation_id,
            "equipment": f"{row.equipment_type}:{row.equipment_id}",
            "status": row.status_code,
            "start": None if pd.isna(row.start_time) else str(row.start_time),
            "end": None if pd.isna(row.end_time) else str(row.end_time),
            "outage_days": float(row.outage_days),
            "open_window": bool(row.open_window),
        })
    return records


def _standing_records(frame: pd.DataFrame, *, limit: int) -> list[dict]:
    """AEMO's standing/recurring windows (start year ≥ the standing threshold).

    These never overlap a target month by construction, so they carry no
    outage-days: they are published as context because dropping them would
    silently discard 147 real register rows, and because their status codes are
    what an operator sees for long-horizon planned works.
    """
    if frame is None or frame.empty:
        return []
    df = frame[frame["standing_window"].astype(bool)].copy()
    if df.empty:
        return []
    df = df.sort_values(["start_time", "outage_id"])
    df = df.drop_duplicates(subset=["outage_id"], keep="first").head(limit)
    records = []
    for row in df.itertuples():
        records.append({
            "outage_id": row.outage_id,
            "region": row.region,
            "voltage": None if pd.isna(row.voltage) else float(row.voltage),
            "voltage_class": row.voltage_class,
            "substation": row.substation_id,
            "equipment": f"{row.equipment_type}:{row.equipment_id}",
            "status": row.status_code,
            "start": str(row.start_time) if not pd.isna(row.start_time) else None,
            "end": str(row.end_time) if not pd.isna(row.end_time) else None,
        })
    return records



# ─── Payload + publish ─────────────────────────────────────────────────────

def build_network_outages_payload(
    facts: pd.DataFrame,
    meta: dict,
    standing_frame: pd.DataFrame | None = None,
) -> dict:
    """Dashboard-readable artifact: regional outage-days by voltage class."""
    months = sorted({str(m) for m in (facts["month"].tolist() if not facts.empty else [])})
    unjoined = 0
    unjoined_voltage = 0
    withdrawn = 0
    if not facts.empty:
        unjoined = int(facts["region"].isna().sum())
        unjoined_voltage = int((facts["voltage_class"] == VOLTAGE_UNKNOWN).sum())
        withdrawn = int(facts["withdrawn"].astype(bool).sum())

    standing = _standing_records(standing_frame, limit=MAX_STANDING_WINDOWS)
    standing_total = 0
    if standing_frame is not None and not standing_frame.empty:
        standing_total = int(
            standing_frame.loc[standing_frame["standing_window"].astype(bool), "outage_id"].nunique()
        )

    by_region = aggregate_by_region_voltage(facts)
    payload = {
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "asof_month": months[-1] if months else None,
        "months": months,
        "source": {
            "table": OUTAGE_TABLE,
            "route": meta.get("route") or ROUTE_MONTHLY,
            "url": meta.get("source_url"),
            "content_length": meta.get("source_bytes"),
            "last_modified": meta.get("source_last_modified"),
            "months_fetched": meta.get("fetched_months", []),
        },
        "summary": {
            "windows": int(pd.unique(facts["outage_id"]).size) if not facts.empty else 0,
            "regions": len(by_region),
            "nem_outage_days": round(
                float(sum(v["outage_days"] for v in by_region.values())), 2,
            ),
            "standing_windows": standing_total,
            "withdrawn_windows_excluded": withdrawn,
            "unjoined_region_windows": unjoined,
            "unknown_voltage_windows": unjoined_voltage,
        },
        "by_region": by_region,
        "active_windows": _window_records(facts, limit=MAX_ACTIVE_WINDOWS),
        "standing_windows": standing,
        "source_status": {
            "status": meta.get("status") or STATUS_OK,
            "note": meta.get("note"),
            "join_warning": meta.get("join_warning"),
        },
    }
    return payload


def publish_network_outages_json(payload: dict, docs_data_dir: str | Path) -> Path | None:
    """Publish docs/data/network_outages.json through the semantic-diff gate."""
    out_dir = Path(docs_data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / ARTIFACT_FILENAME
    wrote = write_json_if_facts_changed(path, payload)
    if wrote:
        logger.info("Published network-outage artifact to %s", path)
    return path if wrote else None


# ─── Storage ───────────────────────────────────────────────────────────────

def snapshot_dir(data_dir: str | Path) -> Path:
    return Path(data_dir) / SNAPSHOT_DIRNAME


def load_snapshot(data_dir: str | Path) -> tuple[pd.DataFrame, dict]:
    """Accumulated window-month snapshot + metadata (empty/{} when absent)."""
    base = snapshot_dir(data_dir)
    feather = base / SNAPSHOT_FILENAME
    meta_path = base / META_FILENAME
    df = pd.DataFrame()
    meta: dict = {}
    if feather.exists():
        try:
            df = pd.read_feather(feather)
        except Exception as e:
            logger.warning("Network outage snapshot unreadable (%s) — treating as absent", e)
            df = pd.DataFrame()
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, ValueError) as e:
            logger.warning("Network outage snapshot metadata unreadable: %s", e)
            meta = {}
    return df, meta


def save_snapshot(data_dir: str | Path, facts: pd.DataFrame, meta: dict) -> Path:
    base = snapshot_dir(data_dir)
    base.mkdir(parents=True, exist_ok=True)
    feather = base / SNAPSHOT_FILENAME
    facts.to_feather(feather)
    (base / META_FILENAME).write_text(json.dumps(meta, indent=1, sort_keys=True))
    return feather


def save_month_slice(data_dir: str | Path, facts: pd.DataFrame, month: str) -> Path:
    base = snapshot_dir(data_dir)
    base.mkdir(parents=True, exist_ok=True)
    path = base / MONTH_SLICE_TEMPLATE.format(ym=month.replace("-", ""))
    facts.to_feather(path)
    return path


def save_standing(data_dir: str | Path, frame: pd.DataFrame) -> Path | None:
    """Persist the standing/recurring windows (start year ≥ the standing threshold).

    They overlap no target month, so they cannot live in the month-keyed
    snapshot; a dedicated small file keeps them published on the cached
    (no-download) runs too.
    """
    base = snapshot_dir(data_dir)
    base.mkdir(parents=True, exist_ok=True)
    path = base / STANDING_FILENAME
    if frame is None or frame.empty:
        path.unlink(missing_ok=True)
        return None
    frame.to_feather(path)
    return path


def load_standing(data_dir: str | Path) -> pd.DataFrame:
    """Standing windows from the last fetch (empty when none stored)."""
    path = snapshot_dir(data_dir) / STANDING_FILENAME
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_feather(path)
    except Exception as e:
        logger.warning("Network outage standing-window cache unreadable: %s", e)
        return pd.DataFrame()


def prune_raw(base: str | Path, keep: tuple[Path, ...] = ()) -> int:
    """Delete raw zips/CSVs after parse (parse-and-slice rule). Returns count.

    The full-history CSV is ~205 MB per month: keeping raw archives would grow
    storage monotonically for data that is re-sliced on every run anyway. The
    lane therefore never leaves a ``.zip`` or ``.CSV`` behind — a test asserts
    it.
    """
    base = Path(base)
    keep_names = {Path(p).name for p in keep}
    removed = 0
    if not base.exists():
        return 0
    for pattern in ("*.zip", "*.CSV", "*.csv", "*.part"):
        for path in base.glob(pattern):
            if path.name in keep_names:
                continue
            path.unlink(missing_ok=True)
            removed += 1
    if removed:
        logger.info("Pruned %d raw outage archive file(s) from %s", removed, base)
    return removed


# ─── Lane orchestration ────────────────────────────────────────────────────

@dataclass
class NetworkOutagesResult:
    status: str = STATUS_OK
    month: str | None = None
    months: list[str] = field(default_factory=list)
    fetched: bool = False
    window_count: int = 0
    frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    payload: dict | None = None
    artifact_path: Path | None = None
    note: str | None = None
    error: str | None = None

    @property
    def retained(self) -> bool:
        return self.status == STATUS_DEGRADED and not self.fetched


def _months_to_fetch(
    latest: tuple[int, int], months_back: int, backfill: int,
) -> list[tuple[int, int]]:
    """Newest month + ``months_back - 1`` (or ``backfill``) prior months."""
    span = max(1, months_back)
    if backfill:
        span = max(span, backfill)
    return [_shift_month(latest[0], latest[1], -delta) for delta in range(span)]


def fetch_context_tables(base: Path, year: int, month: int) -> dict[str, Path]:
    """Download + extract the three small monthly context tables.

    NETWORK_EQUIPMENTDETAIL (0.7 MB zip), NETWORK_RATING (54 KB) and
    NETWORK_SUBSTATIONDETAIL (0.2 MB) are small enough to re-fetch with the
    outage month. A table that fails is skipped with a warning: the lane
    degrades the join quality (and the artifact records it) rather than failing.
    """
    table_files: dict[str, Path] = {}
    for table in SUPPORT_TABLES:
        zip_path = base / f"{table}_{year:04d}{month:02d}.zip"
        try:
            download_zip(support_url(table, year, month), zip_path)
            table_files[table] = extract_table_csv(zip_path, base, table)
        except NetworkOutageError as e:
            logger.warning("Network outage context %s unavailable: %s", table, e)
    return table_files


def run_network_outages_lane(
    data_dir: str | Path,
    docs_data_dir: str | Path,
    *,
    months_back: int = 1,
    backfill: int = 0,
    force: bool = False,
    today: date | None = None,
) -> NetworkOutagesResult:
    """Slice the newest MMSDM month's outage windows and publish the artifact.

    Never raises on an AEMO-side failure: the result carries the lane status
    (``ok`` / ``degraded`` with the last-known-good snapshot retained) so the
    daily pipeline records it in the run manifest and a failed fetch never
    erases published outage blocks.
    """
    today = today or date.today()
    data_dir = Path(data_dir)
    base = snapshot_dir(data_dir)
    prev_facts, prev_meta = load_snapshot(data_dir)

    try:
        discovered = discover_month(today)
    except NetworkOutageError as e:  # defensive: discovery catches its own misses
        discovered = None
        logger.warning("Network outage month discovery failed: %s", e)

    if discovered is None:
        error = (
            "no NETWORK_OUTAGEDETAIL month discovered (AEMO served no monthly "
            f"archive in the last {config.NETWORK_OUTAGE_PROBE_MONTHS_BACK} months)"
        )
        status = STATUS_DEGRADED if not prev_facts.empty else STATUS_ERROR
        if not prev_facts.empty:
            logger.warning("%s — retaining snapshot through %s", error, prev_meta.get("month"))
        return NetworkOutagesResult(
            status=status,
            month=prev_meta.get("month"),
            months=sorted({str(m) for m in prev_meta.get("months", [])}),
            frame=prev_facts,
            error=error,
            note="last-known-good snapshot retained" if not prev_facts.empty else None,
        )

    year, month = discovered["year"], discovered["month"]
    target_months = _months_to_fetch((year, month), months_back, backfill)
    fetched_months = [f"{y:04d}-{m:02d}" for y, m in target_months]

    # Skip the 23 MB download when the same month was already sliced and AEMO's
    # file is unchanged (size + last-modified match the stored metadata).
    stored_month = prev_meta.get("month")
    if (
        not force
        and stored_month == f"{year:04d}-{month:02d}"
        and not prev_facts.empty
        and prev_meta.get("source_bytes") == discovered.get("content_length")
        and prev_meta.get("source_last_modified") == discovered.get("last_modified")
    ):
        logger.info(
            "Network outages %s: source unchanged (size + last-modified match) — "
            "no download needed", stored_month,
        )
        standing_frame = load_standing(data_dir)
        payload = build_network_outages_payload(prev_facts, prev_meta, standing_frame)
        artifact = publish_network_outages_json(payload, docs_data_dir)
        return NetworkOutagesResult(
            status=STATUS_OK,
            month=stored_month,
            months=sorted({str(m) for m in prev_facts["month"].unique()}),
            fetched=False,
            window_count=int(pd.unique(prev_facts["outage_id"]).size),
            frame=prev_facts,
            payload=payload,
            artifact_path=artifact or (Path(docs_data_dir) / ARTIFACT_FILENAME),
            note="same month, source unchanged (size + last-modified match)",
        )

    base.mkdir(parents=True, exist_ok=True)
    zip_path = base / f"{OUTAGE_TABLE}_{year:04d}{month:02d}.zip"
    csv_path: Path | None = None
    try:
        fetch_meta = download_zip(discovered["url"], zip_path)
        csv_path = extract_table_csv(zip_path, base, OUTAGE_TABLE)
        windows = parse_outage_windows(csv_path, keep_months=target_months)

        table_files = fetch_context_tables(base, year, month)

        voltage_map = (
            load_voltage_map(table_files[EQUIPMENT_TABLE])
            if EQUIPMENT_TABLE in table_files else pd.DataFrame()
        )
        region_map = (
            load_region_map(table_files[RATING_TABLE])
            if RATING_TABLE in table_files else pd.DataFrame()
        )
        substation_regions = (
            load_substation_regions(table_files[SUBSTATION_TABLE])
            if SUBSTATION_TABLE in table_files else {}
        )
        windows = attach_context(windows, voltage_map, region_map, substation_regions)

        slices = [slice_month(windows, y, m) for y, m in target_months]
        slices = [s for s in slices if not s.empty]
        fresh = pd.concat(slices, ignore_index=True) if slices else pd.DataFrame(columns=list(SNAPSHOT_COLUMNS))
    except Exception as e:
        prune_raw(base)
        status = STATUS_DEGRADED if not prev_facts.empty else STATUS_ERROR
        logger.warning(
            "Network outage fetch/parse failed for %04d-%02d: %s: %s",
            year, month, type(e).__name__, e,
        )
        return NetworkOutagesResult(
            status=status,
            month=prev_meta.get("month"),
            months=sorted({str(m) for m in prev_meta.get("months", [])}),
            frame=prev_facts,
            error=f"{year:04d}-{month:02d}: {type(e).__name__}: {e}",
            note="last-known-good snapshot retained" if not prev_facts.empty else None,
        )
    finally:
        # Parse-and-slice from day one: the raw zip (23 MB) and the extracted
        # full-history CSV (205 MB) are never retained.
        prune_raw(base)

    if fresh.empty:
        status = STATUS_DEGRADED if not prev_facts.empty else STATUS_ERROR
        error = f"{year:04d}-{month:02d}: no outage windows overlapped the target months"
        return NetworkOutagesResult(
            status=status,
            month=prev_meta.get("month"),
            months=sorted({str(m) for m in prev_meta.get("months", [])}),
            frame=prev_facts,
            error=error,
            note="last-known-good snapshot retained" if not prev_facts.empty else None,
        )

    facts = merge_month_rows(
        base / SNAPSHOT_FILENAME, fresh, full_refresh=force, label="network outage window-month",
    )
    for month_label in sorted({str(m) for m in fresh["month"].unique()}):
        save_month_slice(data_dir, facts[facts["month"] == month_label], month_label)

    standing_frame = windows[windows["standing_window"].astype(bool)].copy()
    save_standing(data_dir, standing_frame)

    unjoined = int(facts["region"].isna().sum())
    join_warning = (
        f"{unjoined} window-month row(s) had no region join (rating + substation "
        "detail) and are excluded from the regional rollup"
        if unjoined else None
    )
    logger.info(
        "Network outages %04d-%02d: %d window(s) (%d standing), %d window-month "
        "row(s), %d unjoined region row(s)",
        year, month, len(windows), len(standing_frame), len(fresh), unjoined,
    )

    new_meta = {
        "month": f"{year:04d}-{month:02d}",
        "months": sorted({str(m) for m in facts["month"].unique()}),
        "fetched_months": fetched_months,
        "route": ROUTE_MONTHLY,
        "source_url": discovered["url"],
        "source_bytes": fetch_meta.get("content_length"),
        "source_last_modified": fetch_meta.get("last_modified"),
        "checked_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rows": int(len(facts)),
        "standing_rows": int(len(standing_frame)),
        "status": STATUS_OK,
        "join_warning": join_warning,
    }
    save_snapshot(data_dir, facts, new_meta)

    payload = build_network_outages_payload(facts, new_meta, standing_frame)
    artifact = publish_network_outages_json(payload, docs_data_dir)

    return NetworkOutagesResult(
        status=STATUS_OK,
        month=new_meta["month"],
        months=new_meta["months"],
        fetched=True,
        window_count=int(pd.unique(facts["outage_id"]).size),
        frame=facts,
        payload=payload,
        artifact_path=artifact,
        note=f"parsed {len(windows)} window(s) from {new_meta['month']} full-history archive"
             + (f"; {join_warning}" if join_warning else ""),
    )


# ─── CLI (manual run; the daily lane calls run_network_outages_lane) ───────

def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="AEMO MMSDM NETWORK_OUTAGEDETAIL monthly parse-and-slice lane",
    )
    parser.add_argument("--data-dir", default=None, help="machine-local cache dir")
    parser.add_argument("--docs-data-dir", default=None, help="published docs/data dir")
    parser.add_argument("--months-back", type=int, default=1,
                        help="months of outage windows to slice (default 1)")
    parser.add_argument("--backfill", type=int, default=0,
                        help="slice N prior months too (monthly backfill route)")
    parser.add_argument("--force", action="store_true",
                        help="re-download and replace the snapshot wholesale")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    root = Path(__file__).resolve().parent.parent
    data_dir = Path(args.data_dir) if args.data_dir else root / config.DATA_DIR
    docs_data_dir = Path(args.docs_data_dir) if args.docs_data_dir else root / config.DOCS_DATA_DIR

    result = run_network_outages_lane(
        data_dir, docs_data_dir,
        months_back=args.months_back, backfill=args.backfill, force=args.force,
    )
    print(json.dumps({
        "status": result.status,
        "month": result.month,
        "months": result.months,
        "fetched": result.fetched,
        "windows": result.window_count,
        "note": result.note,
        "error": result.error,
        "artifact": str(result.artifact_path) if result.artifact_path else None,
    }, indent=1))
    return 0 if result.status == STATUS_OK else 1


if __name__ == "__main__":
    raise SystemExit(_main())
