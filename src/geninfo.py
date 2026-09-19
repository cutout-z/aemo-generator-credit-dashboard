"""AEMO Generation Information (quarterly xlsx): fetcher + commitment-status diff.

The Generation Information workbook is AEMO's project register: every existing,
committed, anticipated and withdrawn generation project in the NEM, with a
Commitment Status per unit (In Service / Committed / Committed* / In
Commissioning / Anticipated / Publicly Announced / Announced Withdrawal /
Withdrawn). It carries credit-risk value the market-data lanes cannot:

- **forward cannibalisation** — committed capacity in a region (BESS, solar,
  wind) compresses every incumbent's future capture spread;
- **counterparty event triggers** — commitment decisions, de-commitments and
  announced withdrawals, diffed per quarterly edition.

Access notes (verified live; see docs/FUTURE_DATA_SOURCES.md):

- The landing page hrefs carry a per-publication ``?rev=<hash>&sc_lang=en``
  query. A scraped href + its rev query is the canonical route; the rev hash
  changes with every republication (do not hard-code).
- The landing page sits behind a CDN that blocks plain non-browser GETs from
  some networks (observed 403 Cloudflare HTML from the Mac). The module
  therefore has a second route: construct the deterministic quarterly media
  URL (``.../generation_information/{year}/nem-generation-information-
  {month}-{year}.xlsx``, month-name spellings vary by publication) and probe
  the current quarter backwards. Probing is HEAD-based and cheap.
- **Key join contract**: DUID is blank on ~62% of rows (pre-connection
  projects), so edition-to-edition identity keys on ``Gen Info Unit ID``
  (+ ``Unit Name`` when one ID collides across two physical units) — never on
  DUID. DUID is joined opportunistically for the per-generator docs.

Storage follows the parse-and-slice rule: the ~900 KB xlsx is parsed into a
compact 19-column snapshot (``data/geninfo/geninfo_snapshot.feather``); the
raw workbook is kept only for the current edition and pruned on the next
fetch. The dashboard-readable artifact is ``docs/data/gen_info.json`` (edition
counts, per-region committed-capacity rollups, the diff event lists and a
compact unit table), written through the S3-12 semantic-diff publish gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from . import config
from .run_status import STATUS_DEGRADED, STATUS_ERROR, STATUS_OK

logger = logging.getLogger(__name__)

# ─── Source coordinates ────────────────────────────────────────────────────

GENINFO_MEDIA_BASE = (
    "https://www.aemo.com.au/-/media/files/electricity/nem/"
    "planning_and_forecasting/generation_information"
)
FILENAME_PREFIX = "nem-generation-information-"

# Quarterly series: January / April / July / October.
QUARTER_MONTHS = (1, 4, 7, 10)
# How many quarters back the probe route searches when it cannot scrape.
PROBE_QUARTERS_BACK = 5

SHEET_NAME = "Generator Information"
HEADER_ROW = 3  # 0-based: header lives in row 4 (three banner rows above it)

MIN_XLSX_BYTES = 50_000
XLSX_MAGIC = b"PK\x03\x04"

# ─── Commitment-status vocabulary ──────────────────────────────────────────

# "Committed" family: a financial/construction decision exists. Committed* is
# AEMO's marker for a commitment pending final documentation; In Commissioning
# is a committed project energising. These are the rows that move a region's
# forward supply stack.
COMMITTED_STATUSES = frozenset({"Committed", "Committed*", "In Commissioning"})
WITHDRAWN_STATUSES = frozenset({"Announced Withdrawal", "Withdrawn"})
# Leaving the committed family back to the marketing stages (without a formal
# withdrawal notice) is a de-commitment.
DECOMMIT_STATUSES = frozenset({"Anticipated", "Publicly Announced"})
IN_SERVICE_STATUS = "In Service"

# Edition-to-edition diff event buckets (ordered most-specific first — the doc
# attach layer keeps the first event it sees for a unit).
EVENT_BUCKETS = {
    "new_commitments": "new_commitment",
    "withdrawals": "withdrawal",
    "decommitments": "decommitment",
    "commissioned": "commissioned",
    "status_changes": "status_change",
}

# ─── Column contract (post-restructure, single 'Generator Information' sheet) ─

UNIT_ID_COL = "Gen Info Unit ID"
DUID_COL = "DUID"
STATUS_COL = "Commitment Status"

# Canonical snapshot field -> candidate workbook headers, first match wins.
# AEMO renames columns between editions (Apr-2026 'Agg Nameplate Capacity
# (MW AC)' and 'AEMO Survey ID' became 'Aggregated Nameplate Capacity (MW AC)'
# and 'Survey ID' by Jul-2026), so only the diff key (Gen Info Unit ID) and the
# discriminator (Commitment Status) are hard requirements; every other field
# degrades to null on an edition that does not carry it.
_FIELD_COLUMNS: dict[str, list[str]] = {
    "unit_name": ["Unit Name"],
    "site_name": ["Site Name"],
    "duid": [DUID_COL],
    "region": ["Region"],
    "technology": ["Technology Type"],
    "technology_detail": ["Technology Detail"],
    "capacity_mw": [
        "Aggregated Nameplate Capacity (MW AC)",
        "Agg Nameplate Capacity (MW AC)",
    ],
    "unit_capacity_mw": ["Unit Capacity (MW AC)"],
    "storage_mwh": ["Agg Nameplate Storage Capacity (MWh)"],
    "full_commercial_use_date": ["Full Commercial Use Date"],
    "expected_closure_year": ["Expected Closure Year"],
    "closure_date": ["Closure Date"],
    "site_owner": ["Site Owner"],
    "custodian": ["Custodian"],
    "dispatch_type": ["Dispatch Type"],
    "survey_id": ["Survey ID", "AEMO Survey ID"],
    "kci_id": ["KCI Id", "AEMO KCI ID"],
    "unit_count": ["Unit Count"],
    "survey_latest_update": ["Survey Latest Update Date"],
}

# Canonical snapshot column order (stable feather schema across editions).
SNAPSHOT_COLUMNS = ["unit_id", "unit_name", "site_name", "duid", "region",
                    "technology", "technology_detail", "commitment_status",
                    "capacity_mw", "unit_capacity_mw", "storage_mwh",
                    "full_commercial_use_date", "expected_closure_year",
                    "closure_date", "site_owner", "custodian", "dispatch_type",
                    "survey_id", "kci_id", "unit_count", "survey_latest_update",
                    "edition"]

SNAPSHOT_DIRNAME = "geninfo"
SNAPSHOT_FILENAME = "geninfo_snapshot.feather"
BASELINE_FILENAME = "geninfo_baseline.feather"
META_FILENAME = "geninfo_meta.json"
ARTIFACT_FILENAME = "gen_info.json"

_MONTH_NAMES = {
    1: ("jan", "january"), 2: ("feb", "february"), 3: ("mar", "march"),
    4: ("apr", "april"), 5: ("may",), 6: ("jun", "june"),
    7: ("jul", "july"), 8: ("aug", "august"), 9: ("sep", "september", "sept"),
    10: ("oct", "october"), 11: ("nov", "november"), 12: ("dec", "december"),
}
_MONTH_TOKENS = {
    token: month for month, names in _MONTH_NAMES.items() for token in names
}


class GenInfoError(RuntimeError):
    """Base failure for the GenInfo lane (fetch/parse/diff)."""


class GenInfoAccessError(GenInfoError):
    """AEMO refused the request (403/CDN block) — never retried, never hammered."""


# ─── HTTP plumbing ─────────────────────────────────────────────────────────

_SESSION: requests.Session | None = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-AU,en;q=0.9",
        })
        _SESSION = s
    return _SESSION


def _request(method: str, url: str, *, timeout: int | None = None) -> requests.Response:
    """One HTTP call with bounded retries and backoff.

    Transient failures (connection errors, timeouts, 429/5xx) retry up to
    ``config.MAX_RETRIES`` with ``config.RETRY_BACKOFF``-scaled waits. A 403 is
    the CDN/rate-limit signal: raise immediately so a blocked route fails fast
    and the caller switches route rather than hammering AEMO.
    """
    timeout = timeout or config.REQUEST_TIMEOUT
    last_error: Exception | None = None
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = _session().request(method, url, timeout=timeout, allow_redirects=True)
        except requests.RequestException as e:  # includes Timeout/ConnectionError
            last_error = e
        else:
            if resp.status_code == 403:
                raise GenInfoAccessError(
                    f"HTTP 403 (CDN/rate-limit block) for {url}"
                )
            if resp.status_code < 400:
                return resp
            if resp.status_code == 404:
                raise GenInfoError(f"HTTP 404 for {url}")
            last_error = GenInfoError(f"HTTP {resp.status_code} for {url}")
        if attempt < config.MAX_RETRIES - 1:
            wait = config.RETRY_BACKOFF * (attempt + 1)
            logger.warning(
                "GenInfo request failed (%s, attempt %d/%d) — retrying in %ds",
                last_error, attempt + 1, config.MAX_RETRIES, wait,
            )
            time.sleep(wait)
    raise GenInfoError(f"request failed after {config.MAX_RETRIES} attempts: {last_error}")


def fetch_landing_html(url: str | None = None) -> str:
    """Fetch the Generation Information landing page HTML (route 1)."""
    resp = _request("GET", url or config.GENINFO_LANDING_URL)
    return resp.text


def probe_edition_url(url: str) -> dict | None:
    """Cheap existence/size probe for a candidate media URL (HEAD only).

    Returns ``{"url", "content_length", "last_modified", "content_type"}`` when
    the URL serves an xlsx, else None. AEMO answers existing media with 200 +
    the xlsx content type; unknown/future quarters answer 403/302 — both read
    as "not published".
    """
    try:
        resp = _request("HEAD", url)
    except GenInfoError as e:
        logger.info("GenInfo probe miss for %s (%s)", url, e)
        return None
    ctype = (resp.headers.get("content-type") or "").lower()
    length = resp.headers.get("content-length")
    if "spreadsheetml" not in ctype:
        return None
    if length is not None and int(length) < MIN_XLSX_BYTES:
        return None
    return {
        "url": url,
        "content_length": int(length) if length is not None else None,
        "last_modified": resp.headers.get("last-modified"),
        "content_type": ctype,
    }


def fetch_xlsx(url: str, dest: Path) -> dict:
    """Download an edition workbook to ``dest`` (atomic) and validate it.

    Returns probe-style metadata (content length / last-modified) so the
    caller can skip re-downloads when the source is unchanged. Raises
    ``GenInfoAccessError`` on a 403 block.
    """
    resp = _request("GET", url, timeout=max(config.REQUEST_TIMEOUT, 120))
    content = resp.content
    if len(content) < MIN_XLSX_BYTES or not content[:4] == XLSX_MAGIC:
        raise GenInfoError(
            f"{url}: response is not an xlsx workbook "
            f"({len(content)} bytes, magic={content[:4]!r})"
        )
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    tmp.write_bytes(content)
    tmp.replace(dest)
    logger.info("GenInfo workbook downloaded: %d KB → %s", len(content) // 1024, dest)
    length = resp.headers.get("content-length")
    return {
        "url": url,
        "content_length": int(length) if length else len(content),
        "last_modified": resp.headers.get("last-modified"),
        "content_type": (resp.headers.get("content-type") or "").lower(),
    }


# ─── Edition discovery ─────────────────────────────────────────────────────

def _parse_edition_from_filename(filename: str) -> tuple[int, int] | None:
    """Parse (year, month) from a national-series filename, else None.

    Handles the month-name spellings AEMO has used (jan/january, apr/april,
    jul/july, oct/october, '7-feb-2024', ...). Non-series files (regional
    legacy 'generation_information_*', closure-year workbook, KCI datafile)
    return None.
    """
    name = filename.split("?")[0].split("/")[-1]
    if not name.lower().startswith(FILENAME_PREFIX):
        return None
    stem = name[: -len(".xlsx")] if name.lower().endswith(".xlsx") else name
    tokens = [t for t in re.split(r"[-_\s]+", stem[len(FILENAME_PREFIX):].lower()) if t]
    month = next((_MONTH_TOKENS[t] for t in tokens if t in _MONTH_TOKENS), None)
    year = next((int(t) for t in tokens if re.fullmatch(r"20\d{2}", t)), None)
    if month is None or year is None:
        return None
    return year, month


def extract_edition_links(html: str, base_url: str | None = None) -> list[dict]:
    """All national GenInfo xlsx links in a landing page, sorted by edition.

    Each entry: ``{"url", "edition", "year", "month", "filename"}``. Only the
    national ``nem-generation-information-*.xlsx`` series is kept (companion
    closure-year / KCI workbooks and the pre-2025 per-region files are not
    editions of the register).
    """
    base = base_url or config.GENINFO_LANDING_URL
    seen: set[str] = set()
    links: list[dict] = []
    for raw in re.findall(r'href="([^"]+\.xlsx[^"]*)"', html):
        url = raw.replace("&amp;", "&")
        url = urljoin(base, url)
        filename = url.split("/")[-1].split("?")[0]
        parsed = _parse_edition_from_filename(filename)
        if parsed is None or url in seen:
            continue
        seen.add(url)
        year, month = parsed
        links.append({
            "url": url,
            "edition": f"{year}-{month:02d}",
            "year": year,
            "month": month,
            "filename": filename,
        })
    links.sort(key=lambda e: (e["year"], e["month"]))
    return links


def select_current_edition(links: list[dict], today: date | None = None) -> dict | None:
    """Newest listed edition not in the future (quarter published <= today)."""
    today = today or date.today()
    eligible = [
        e for e in links
        if (e["year"], e["month"]) <= (today.year, today.month)
    ]
    return eligible[-1] if eligible else None


def candidate_edition_urls(year: int, month: int) -> list[str]:
    """Deterministic media URLs for one edition (month-name spellings tried)."""
    return [
        f"{GENINFO_MEDIA_BASE}/{year}/{FILENAME_PREFIX}{name}-{year}.xlsx"
        for name in _MONTH_NAMES.get(month, ())
    ]


def _recent_quarter_months(today: date, count: int = PROBE_QUARTERS_BACK) -> list[tuple[int, int]]:
    """(year, month) of the most recent quarter months at/before today, newest first."""
    quarters: list[tuple[int, int]] = []
    year = today.year
    while len(quarters) < count:
        for qm in sorted(QUARTER_MONTHS, reverse=True):
            if (year, qm) <= (today.year, today.month):
                quarters.append((year, qm))
                if len(quarters) >= count:
                    break
        year -= 1
    return quarters


def discover_edition(today: date | None = None) -> dict | None:
    """Resolve the current edition: landing-page scrape first, probe second.

    Returns ``{"url", "edition", "year", "month", "route"}`` or None when both
    routes fail (blocked landing page + no probe hit) — the caller records a
    degraded lane rather than inventing an edition.
    """
    today = today or date.today()

    try:
        html = fetch_landing_html()
    except GenInfoError as e:
        logger.warning("GenInfo landing page unavailable (%s) — probing media URLs", e)
    else:
        links = extract_edition_links(html)
        current = select_current_edition(links, today=today)
        if current is not None:
            return {**current, "route": "landing_page"}

    for year, month in _recent_quarter_months(today):
        for url in candidate_edition_urls(year, month):
            if probe_edition_url(url) is not None:
                logger.info(
                    "GenInfo edition discovered by probe: %s-%02d (%s)", year, month, url,
                )
                return {
                    "url": url,
                    "edition": f"{year}-{month:02d}",
                    "year": year,
                    "month": month,
                    "filename": url.split("/")[-1],
                    "route": "probe",
                }
    return None


def discover_previous_edition(current: dict, today: date | None = None) -> dict | None:
    """The newest edition strictly older than ``current`` (baseline bootstrap).

    Used once, on the very first run, so the initial artifact already carries a
    real edition-over-edition diff instead of an empty baseline.
    """
    today = today or date.today()
    current_key = (current["year"], current["month"])
    try:
        links = extract_edition_links(fetch_landing_html())
    except GenInfoError:
        links = []
    older = [e for e in links if (e["year"], e["month"]) < current_key]
    if older:
        return {**older[-1], "route": "landing_page"}

    for year, month in _recent_quarter_months(today, count=PROBE_QUARTERS_BACK + 2):
        if (year, month) >= current_key:
            continue
        for url in candidate_edition_urls(year, month):
            if probe_edition_url(url) is not None:
                return {
                    "url": url,
                    "edition": f"{year}-{month:02d}",
                    "year": year,
                    "month": month,
                    "filename": url.split("/")[-1],
                    "route": "probe",
                }
    return None


# ─── Parse + normalise ─────────────────────────────────────────────────────

def parse_geninfo_workbook(path: str | Path) -> pd.DataFrame:
    """Read the register sheet (header on row 4) into a frame.

    Falls back to schema-probing other sheets when AEMO renames the sheet, and
    fails loudly when no sheet carries the Gen Info Unit ID header.
    """
    path = Path(path)
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
        sheets = list(xls.sheet_names)
        target = SHEET_NAME if SHEET_NAME in sheets else None
        if target is None:
            for cand in sheets:
                probe = pd.read_excel(xls, sheet_name=cand, header=HEADER_ROW, nrows=0)
                if UNIT_ID_COL in [str(c).strip() for c in probe.columns]:
                    target = cand
                    break
        if target is None:
            raise GenInfoError(
                f"{path.name}: no sheet carries a {UNIT_ID_COL!r} header "
                f"(sheets: {sheets})"
            )
        df = pd.read_excel(xls, sheet_name=target, header=HEADER_ROW)
    except GenInfoError:
        raise
    except Exception as e:  # truncated / non-xlsx download, schema churn
        raise GenInfoError(f"{path.name}: unreadable GenInfo workbook ({e})") from e
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    return None if text in ("", "-", "nan", "NaT", "None") else text


def _number(value) -> float | None:
    out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(out) else float(out)


def _iso_date(value) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    ts = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(ts):
        return _text(value)
    return ts.strftime("%Y-%m-%d")


def _int_year(value) -> int | None:
    if value is None:
        return None
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        ts = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
        return None if pd.isna(ts) else int(ts.year)
    return int(num)


# Field -> value converter (default is _text for anything not listed here).
_FIELD_CONVERTERS = {
    "capacity_mw": _number,
    "unit_capacity_mw": _number,
    "storage_mwh": _number,
    "full_commercial_use_date": _iso_date,
    "closure_date": _iso_date,
    "survey_latest_update": _iso_date,
    "expected_closure_year": _int_year,
    "unit_count": _int_year,
}


def normalize_geninfo(df: pd.DataFrame, edition: str) -> pd.DataFrame:
    """Slice the 76-column workbook down to the canonical snapshot columns.

    Parse-and-slice from day one: only the columns the credit view and the
    diff need are retained, typed, and stored — DUID stays optional (blank on
    ~62% of rows; the edition key is Gen Info Unit ID + Unit Name). Column
    names drift between editions, so fields resolve through the
    ``_FIELD_COLUMNS`` alias table and a field absent from this edition is
    null, not an error; only the diff key (Gen Info Unit ID) and the
    discriminator (Commitment Status) must exist.
    """
    if df is None or df.empty:
        raise GenInfoError("GenInfo workbook parsed to zero rows")

    missing = [
        col for col in (UNIT_ID_COL, STATUS_COL)
        if col not in df.columns
    ]
    if missing:
        raise GenInfoError(
            f"GenInfo workbook missing required column(s) {missing} "
            "(schema changed?)"
        )

    out = pd.DataFrame(index=df.index)
    out["unit_id"] = df[UNIT_ID_COL].map(_text)
    out["commitment_status"] = df[STATUS_COL].map(_text)

    absent: list[str] = []
    for field, candidates in _FIELD_COLUMNS.items():
        column = next((c for c in candidates if c in df.columns), None)
        if column is None:
            absent.append(field)
            out[field] = None
            continue
        converter = _FIELD_CONVERTERS.get(field, _text)
        out[field] = df[column].map(converter)
    if absent:
        logger.warning(
            "GenInfo edition %s: field(s) not carried by this workbook "
            "(nulled): %s", edition, ", ".join(sorted(absent)),
        )

    out["edition"] = edition
    out = out[SNAPSHOT_COLUMNS]
    for column in ("expected_closure_year", "unit_count"):
        out[column] = pd.to_numeric(out[column], errors="coerce").astype("Int64")

    out = out[out["unit_id"].notna()].reset_index(drop=True)
    if out.empty:
        raise GenInfoError("GenInfo workbook has no rows with a Gen Info Unit ID")
    return out


def snapshot_fingerprint(df: pd.DataFrame) -> str:
    """Stable content hash of a snapshot (edition metadata excluded)."""
    if df is None or df.empty:
        return ""
    cols = [c for c in df.columns if c != "edition"]
    stable = df[cols].astype(str).sort_values(cols).reset_index(drop=True)
    hashed = pd.util.hash_pandas_object(stable, index=False).values.tobytes()
    return hashlib.sha256(hashed).hexdigest()


# ─── Commitment-status diff ────────────────────────────────────────────────

def _unit_key(row) -> str:
    """Edition-to-edition identity: Gen Info Unit ID + Unit Name.

    The ID alone is not unique (two Whitwood Road units share one ID in the
    Jul-2026 edition), and DUID is blank on ~62% of rows, so the composite is
    the safest key available.
    """
    unit_id = _text(row.get("unit_id")) or ""
    unit_name = (_text(row.get("unit_name")) or "").upper()
    return f"{unit_id}|{unit_name}"


def _event_row(row, from_status: str | None, to_status: str | None) -> dict:
    return {
        "unit_id": row.get("unit_id"),
        "unit_name": row.get("unit_name"),
        "site_name": row.get("site_name"),
        "duid": row.get("duid"),
        "region": row.get("region"),
        "technology": row.get("technology"),
        "capacity_mw": row.get("capacity_mw"),
        "storage_mwh": row.get("storage_mwh"),
        "from_status": from_status,
        "to_status": to_status,
        "full_commercial_use_date": row.get("full_commercial_use_date"),
        "expected_closure_year": row.get("expected_closure_year"),
        "site_owner": row.get("site_owner"),
    }


def _clean_json(value):
    """numpy → native JSON types (NaN → None)."""
    if value is None:
        return None
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, (int, str, bool)):
        return value
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def diff_commitment_status(prev: pd.DataFrame | None, curr: pd.DataFrame) -> dict:
    """Edition-over-edition Commitment Status diff.

    Identity: composite (Gen Info Unit ID, Unit Name) key; rows left over are
    re-matched by Unit ID alone when unambiguous (a unit rename must not read
    as remove + add) and recorded under ``unit_name_changes``. Anything still
    unmatched is a genuinely new or removed unit.

    Event buckets (all carry from_status/to_status):
      - ``new_commitments``: entered the committed family (Committed /
        Committed* / In Commissioning) from outside it, or arrived new already
        committed — the forward-cannibalisation trigger;
      - ``withdrawals``: entered Withdrawn / Announced Withdrawal;
      - ``decommitments``: left the committed family back to Anticipated /
        Publicly Announced without a withdrawal notice;
      - ``commissioned``: committed family → In Service (clean energisation);
      - ``status_changes``: every other status transition (superset view).
    """
    if curr is None or curr.empty:
        raise GenInfoError("diff_commitment_status requires a current snapshot")

    zero_counts = {
        key: 0 for key in (
            "matched_units", "new_commitments", "withdrawals", "decommitments",
            "commissioned", "status_changes", "new_units", "removed_units",
            "unit_name_changes",
        )
    }
    if prev is None or prev.empty:
        # Baseline: no previous edition to compare against. Reporting the
        # entire register as "new" would be noise, not a diff — publish the
        # baseline flag and leave the event buckets empty.
        baseline = {
            "compared_to_edition": None,
            "edition": _edition_of(curr),
            "counts": zero_counts,
            "baseline": True,
            "note": (
                "first stored edition — no previous snapshot to compare "
                "against (the lane bootstraps a diff against the previous "
                "published edition when it can be fetched)"
            ),
        }
        for key in ("new_commitments", "withdrawals", "decommitments",
                    "commissioned", "status_changes", "new_units",
                    "removed_units", "unit_name_changes"):
            baseline[key] = []
        return baseline

    have_prev = True  # baseline returned early above
    prev_rows = list(prev.to_dict("records"))
    curr_rows = list(curr.to_dict("records"))

    prev_by_key = {_unit_key(r): r for r in prev_rows}
    curr_seen: set[str] = set()
    pairs: list[tuple[dict | None, dict]] = []
    unmatched_curr: list[dict] = []

    for row in curr_rows:
        key = _unit_key(row)
        if key in prev_by_key and key not in curr_seen:
            curr_seen.add(key)
            pairs.append((prev_by_key[key], row))
        else:
            unmatched_curr.append(row)

    unmatched_prev = [r for r in prev_rows if _unit_key(r) not in curr_seen]

    # Second pass: unscoped Unit ID matches for leftovers — unique on both
    # sides means it is the same physical unit under a new name.
    prev_by_id: dict[str, list[dict]] = {}
    for r in unmatched_prev:
        prev_by_id.setdefault(str(r.get("unit_id")), []).append(r)
    still_curr: list[dict] = []
    id_pairs: list[tuple[dict, dict]] = []
    for row in unmatched_curr:
        candidates = prev_by_id.get(str(row.get("unit_id")), [])
        if len(candidates) == 1 and candidates[0] not in [p for p, _ in id_pairs]:
            id_pairs.append((candidates[0], row))
        else:
            still_curr.append(row)
    id_paired_prev = {id(p) for p, _ in id_pairs}
    still_prev = [r for r in unmatched_prev if id(r) not in id_paired_prev]

    buckets: dict[str, list[dict]] = {name: [] for name in EVENT_BUCKETS}
    unit_name_changes: list[dict] = []

    def _record(from_row: dict | None, to_row: dict) -> None:
        from_status = from_row.get("commitment_status") if from_row else None
        to_status = to_row.get("commitment_status")
        if from_row is not None and from_status == to_status:
            return
        if to_status in COMMITTED_STATUSES and (
            from_status is None or from_status not in COMMITTED_STATUSES
        ):
            buckets["new_commitments"].append(_event_row(to_row, from_status, to_status))
        elif from_status in COMMITTED_STATUSES and to_status in WITHDRAWN_STATUSES:
            buckets["withdrawals"].append(_event_row(to_row, from_status, to_status))
        elif to_status in WITHDRAWN_STATUSES and (
            from_status is None or from_status not in WITHDRAWN_STATUSES
        ):
            buckets["withdrawals"].append(_event_row(to_row, from_status, to_status))
        elif from_status in COMMITTED_STATUSES and to_status in DECOMMIT_STATUSES:
            buckets["decommitments"].append(_event_row(to_row, from_status, to_status))
        elif from_status in COMMITTED_STATUSES and to_status == IN_SERVICE_STATUS:
            buckets["commissioned"].append(_event_row(to_row, from_status, to_status))
        elif from_row is not None and to_status != from_status:
            buckets["status_changes"].append(_event_row(to_row, from_status, to_status))
        elif from_row is None:
            # New unit that has not (yet) reached the committed family: it is
            # still a register addition, reported under ``new_units`` only.
            pass

    for from_row, to_row in pairs:
        _record(from_row, to_row)
    for from_row, to_row in id_pairs:
        unit_name_changes.append({
            "unit_id": to_row.get("unit_id"),
            "from_name": from_row.get("unit_name"),
            "to_name": to_row.get("unit_name"),
        })
        _record(from_row, to_row)
    for row in still_curr:
        # A brand-new unit already inside the committed/withdrawn families is a
        # real event (never silently absorbed as "just a new row").
        _record(None, row)

    new_units = [
        _event_row(r, None, r.get("commitment_status")) for r in still_curr
    ]
    removed_units = [
        _event_row(r, r.get("commitment_status"), None) for r in still_prev
    ]

    diff = {
        "compared_to_edition": _edition_of(prev),
        "edition": _edition_of(curr),
        "counts": {
            "matched_units": len(pairs) + len(id_pairs),
            "new_commitments": len(buckets["new_commitments"]),
            "withdrawals": len(buckets["withdrawals"]),
            "decommitments": len(buckets["decommitments"]),
            "commissioned": len(buckets["commissioned"]),
            "status_changes": len(buckets["status_changes"]),
            "new_units": len(new_units),
            "removed_units": len(removed_units),
            "unit_name_changes": len(unit_name_changes),
        },
    }
    for name, rows in buckets.items():
        diff[name] = rows
    diff["new_units"] = new_units
    diff["removed_units"] = removed_units
    diff["unit_name_changes"] = unit_name_changes
    return diff


def _edition_of(df: pd.DataFrame | None) -> str | None:
    if df is None or df.empty or "edition" not in df.columns:
        return None
    editions = [e for e in pd.unique(df["edition"]) if e is not None]
    return str(editions[0]) if len(editions) else None


def events_by_duid(diff: dict | None) -> dict[str, list[dict]]:
    """{DUID: [event, ...]} for the per-generator docs (rows with a DUID only).

    Buckets are read most-specific first and a unit keeps only its first
    (most specific) event, so a new commitment is not also duplicated as a
    generic status change.
    """
    if not diff:
        return {}
    out: dict[str, list[dict]] = {}
    seen: set[tuple] = set()
    for bucket, label in EVENT_BUCKETS.items():
        for event in diff.get(bucket) or []:
            duid = event.get("duid")
            if not duid:
                continue
            key = (event.get("unit_id"), event.get("unit_name"))
            if key in seen:
                continue
            seen.add(key)
            out.setdefault(duid, []).append({
                "type": label,
                "from_status": event.get("from_status"),
                "to_status": event.get("to_status"),
                "edition": diff.get("edition"),
            })
    return out


# ─── Snapshot storage (local cache) ────────────────────────────────────────

def snapshot_dir(data_dir: str | Path) -> Path:
    return Path(data_dir) / SNAPSHOT_DIRNAME


def load_snapshot(data_dir: str | Path) -> tuple[pd.DataFrame, dict]:
    """Last stored snapshot + its metadata (empty/{} when absent or unreadable)."""
    base = snapshot_dir(data_dir)
    feather = base / SNAPSHOT_FILENAME
    meta_path = base / META_FILENAME
    df = pd.DataFrame()
    meta: dict = {}
    if feather.exists():
        try:
            df = pd.read_feather(feather)
        except Exception as e:  # corrupt cache must not crash the lane
            logger.warning("GenInfo snapshot unreadable (%s) — treating as absent", e)
            df = pd.DataFrame()
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, ValueError) as e:
            logger.warning("GenInfo snapshot metadata unreadable: %s", e)
            meta = {}
    return df, meta


def save_snapshot(data_dir: str | Path, df: pd.DataFrame, meta: dict) -> Path:
    base = snapshot_dir(data_dir)
    base.mkdir(parents=True, exist_ok=True)
    feather = base / SNAPSHOT_FILENAME
    df.to_feather(feather)
    (base / META_FILENAME).write_text(json.dumps(meta, indent=1, sort_keys=True))
    return feather


def load_baseline(data_dir: str | Path) -> tuple[pd.DataFrame, str | None]:
    """The diff baseline snapshot (previous published edition), if stored.

    Persisting the baseline keeps the artifact's quarter-over-quarter diff
    stable across same-edition re-fetches: a republication re-derives the new
    commitments/withdrawals since the PREVIOUS edition, never "since the last
    time we happened to re-download the current one".
    """
    feather = snapshot_dir(data_dir) / BASELINE_FILENAME
    if not feather.exists():
        return pd.DataFrame(), None
    try:
        df = pd.read_feather(feather)
    except Exception as e:
        logger.warning("GenInfo baseline snapshot unreadable (%s)", e)
        return pd.DataFrame(), None
    return df, _edition_of(df)


def save_baseline(data_dir: str | Path, df: pd.DataFrame | None) -> Path | None:
    base = snapshot_dir(data_dir)
    base.mkdir(parents=True, exist_ok=True)
    feather = base / BASELINE_FILENAME
    if df is None or df.empty:
        feather.unlink(missing_ok=True)  # no baseline must not read as "stale"
        return None
    df.to_feather(feather)
    return feather


def _prune_raw_editions(base: Path, keep: Path) -> int:
    """Keep only the current edition's raw workbook (parse-and-slice rule)."""
    removed = 0
    for path in base.glob(f"{FILENAME_PREFIX}*.xlsx"):
        if path.name != keep.name:
            path.unlink()
            removed += 1
    return removed


# ─── Publish (dashboard artifact) ──────────────────────────────────────────

def build_gen_info_payload(
    snapshot: pd.DataFrame,
    meta: dict,
    diff: dict | None,
) -> dict:
    """Dashboard-readable payload: edition counts, region rollups, diff, units."""
    region_rollups: dict[str, dict] = {}
    for region, group in snapshot.groupby(snapshot["region"].fillna("UNKNOWN")):
        by_status: dict[str, dict] = {}
        for status, sgroup in group.groupby(group["commitment_status"].fillna("UNKNOWN")):
            by_status[str(status)] = {
                "units": int(len(sgroup)),
                "capacity_mw": _clean_json(round(float(sgroup["capacity_mw"].sum()), 2)),
                "storage_mwh": _clean_json(round(float(sgroup["storage_mwh"].sum()), 2)),
            }
        committed = group[group["commitment_status"].isin(COMMITTED_STATUSES)]
        tech: dict[str, object] = {}
        for technology, tgroup in committed.groupby(committed["technology"].fillna("UNKNOWN")):
            tech[str(technology)] = _clean_json(
                round(float(tgroup["capacity_mw"].sum()), 2)
            )
        region_rollups[str(region)] = {
            "units": int(len(group)),
            "capacity_mw": _clean_json(round(float(group["capacity_mw"].sum()), 2)),
            "committed_units": int(len(committed)),
            "committed_mw": _clean_json(round(float(committed["capacity_mw"].sum()), 2)),
            "committed_storage_mwh": _clean_json(
                round(float(committed["storage_mwh"].sum()), 2)
            ),
            "committed_by_technology": tech,
            "by_status": by_status,
        }

    units = [
        {
            "unit_id": _clean_json(r.get("unit_id")),
            "unit_name": _clean_json(r.get("unit_name")),
            "site_name": _clean_json(r.get("site_name")),
            "duid": _clean_json(r.get("duid")),
            "region": _clean_json(r.get("region")),
            "technology": _clean_json(r.get("technology")),
            "commitment_status": _clean_json(r.get("commitment_status")),
            "capacity_mw": _clean_json(r.get("capacity_mw")),
            "storage_mwh": _clean_json(r.get("storage_mwh")),
            "full_commercial_use_date": _clean_json(r.get("full_commercial_use_date")),
            "expected_closure_year": _clean_json(r.get("expected_closure_year")),
            "site_owner": _clean_json(r.get("site_owner")),
        }
        for r in snapshot.to_dict("records")
    ]

    counts_by_status = {
        str(k): int(v)
        for k, v in snapshot["commitment_status"].value_counts().items()
    }

    return {
        "scope": "aemo_generation_information",
        "description": (
            "AEMO Generation Information project register (quarterly xlsx): "
            "commitment status per project unit, committed-capacity rollups per "
            "region, and the edition-over-edition Commitment Status diff "
            "(new commitments / de-commitments / announced withdrawals). "
            "AEMO's project classification, not dispatch or settlement data."
        ),
        "edition": meta.get("edition"),
        "source_url": meta.get("source_url"),
        "fetched_utc": meta.get("checked_utc"),
        "previous_edition": meta.get("previous_edition"),
        "rows": int(len(snapshot)),
        "counts_by_status": counts_by_status,
        "committed_statuses": sorted(COMMITTED_STATUSES),
        "withdrawn_statuses": sorted(WITHDRAWN_STATUSES),
        "regions": region_rollups,
        "diff": diff,
        "units": units,
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def publish_gen_info_json(payload: dict, docs_data_dir: str | Path) -> Path | None:
    """Write docs/data/gen_info.json through the semantic-diff publish gate.

    ``updated_utc`` is a data-as-of stamp, not a run-attempt record: an
    unchanged edition never rewrites the file (S3-12 semantics, same as
    market_daily.json and the per-DUID offer-curve files).
    """
    from .semantic_publish import write_json_if_facts_changed

    out_dir = Path(docs_data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / ARTIFACT_FILENAME
    if write_json_if_facts_changed(out_path, payload, stamp_keys=("updated_utc",)):
        logger.info(
            "Wrote %s (edition %s, %d units)",
            out_path, payload.get("edition"), payload.get("rows"),
        )
    return out_path


# ─── Per-generator doc block ───────────────────────────────────────────────

def attach_geninfo_doc(
    doc: dict,
    rows: pd.DataFrame | None,
    events: dict | None = None,
) -> None:
    """Attach ``doc['gen_info']`` for the GenInfo register rows of this DUID.

    DUID is blank on ~62% of register rows, so many DUIDs simply have no block
    — absence is the register's verdict, never a zero-filled stub.
    """
    if rows is None or (hasattr(rows, "empty") and rows.empty):
        return
    units = []
    for r in rows.to_dict("records"):
        unit = {
            "unit_id": _clean_json(r.get("unit_id")),
            "unit_name": _clean_json(r.get("unit_name")),
            "site_name": _clean_json(r.get("site_name")),
            "region": _clean_json(r.get("region")),
            "technology": _clean_json(r.get("technology")),
            "commitment_status": _clean_json(r.get("commitment_status")),
            "capacity_mw": _clean_json(r.get("capacity_mw")),
            "storage_mwh": _clean_json(r.get("storage_mwh")),
            "full_commercial_use_date": _clean_json(r.get("full_commercial_use_date")),
            "expected_closure_year": _clean_json(r.get("expected_closure_year")),
        }
        if events:
            hue = events.get(str(r.get("duid"))) if r.get("duid") else None
            if hue:
                unit["event"] = hue[0]
        units.append(unit)
    doc["gen_info"] = {
        "scope": "aemo_generation_information",
        "edition": _clean_json(rows["edition"].iloc[0]) if "edition" in rows.columns else None,
        "note": (
            "AEMO Generation Information register (quarterly) — AEMO's own "
            "project commitment classification, keyed on Gen Info Unit ID "
            "(DUID is blank for pre-connection projects)."
        ),
        "units": units,
    }


# ─── Lane orchestration ────────────────────────────────────────────────────

@dataclass
class GenInfoResult:
    status: str = STATUS_OK
    edition: str | None = None
    source_url: str | None = None
    route: str | None = None
    fetched: bool = False
    row_count: int = 0
    diff: dict | None = None
    snapshot: pd.DataFrame = field(default_factory=pd.DataFrame)
    events_by_duid: dict = field(default_factory=dict)
    artifact_path: Path | None = None
    note: str | None = None
    error: str | None = None

    @property
    def retained(self) -> bool:
        return self.status == STATUS_DEGRADED and not self.fetched


def _rev_token(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"[?&]rev=([0-9a-fA-F]+)", url)
    return match.group(1) if match else None


def _needs_fetch(
    prev_df: pd.DataFrame,
    meta: dict,
    edition: str,
    url: str,
) -> tuple[bool, str]:
    """Decide whether the edition workbook must be (re)downloaded.

    Skip rules keep the daily lane cheap and AEMO-friendly: same edition and
    same rev hash means byte-identical content; on the probe route (no rev) a
    HEAD comparison of content-length + last-modified stands in for the hash.
    """
    if prev_df is None or prev_df.empty or not meta.get("edition"):
        return True, "no stored baseline"
    if edition != meta.get("edition"):
        return True, f"new edition {edition} (stored {meta.get('edition')})"
    stored_url = meta.get("source_url") or ""
    if _rev_token(url) is None and _rev_token(stored_url) is None:
        headers = probe_edition_url(url)
        if headers is None:
            return True, "probe failed — refetching"
        same_size = headers.get("content_length") == meta.get("source_bytes")
        same_lm = bool(headers.get("last_modified")) and (
            headers.get("last_modified") == meta.get("source_last_modified")
        )
        if same_size and same_lm:
            return False, "same edition, source unchanged (size + last-modified match)"
        return True, "same edition, source republished (size/last-modified changed)"
    if _rev_token(url) != _rev_token(stored_url):
        return True, "same edition, rev changed (republication)"
    return False, "same edition, rev unchanged"


def run_geninfo_lane(
    data_dir: str | Path,
    docs_data_dir: str | Path,
    *,
    force: bool = False,
    today: date | None = None,
    bootstrap: bool = True,
) -> GenInfoResult:
    """Fetch the current GenInfo edition, diff it, publish the artifact.

    Never raises on an AEMO-side failure: the result carries the lane status
    (``ok`` / ``degraded`` with the last-known-good snapshot retained) so the
    daily pipeline records it in the run manifest and the continuity guard can
    refuse a publish that would erase populated blocks.
    """
    today = today or date.today()
    prev_df, meta = load_snapshot(data_dir)

    try:
        edition_info = discover_edition(today=today)
    except GenInfoError as e:  # defensive: discovery already catches its own
        edition_info = None
        logger.warning("GenInfo edition discovery failed: %s", e)

    if edition_info is None:
        status = STATUS_DEGRADED if not prev_df.empty else STATUS_ERROR
        error = (
            "no GenInfo edition discovered (landing page blocked and no "
            "quarterly workbook found by probe)"
        )
        if not prev_df.empty:
            logger.warning("%s — retaining edition %s", error, meta.get("edition"))
        return GenInfoResult(
            status=status,
            edition=meta.get("edition"),
            source_url=meta.get("source_url"),
            snapshot=prev_df,
            error=error,
            note="last-known-good snapshot retained" if not prev_df.empty else None,
        )

    edition, url, route = edition_info["edition"], edition_info["url"], edition_info["route"]
    needs_fetch, reason = (
        (True, "forced") if force else _needs_fetch(prev_df, meta, edition, url)
    )

    if not needs_fetch:
        logger.info("GenInfo %s: %s — no download needed", edition, reason)
        diff = None
        artifact = Path(docs_data_dir) / ARTIFACT_FILENAME
        return GenInfoResult(
            status=STATUS_OK,
            edition=edition,
            source_url=meta.get("source_url") or url,
            route=route,
            fetched=False,
            row_count=int(len(prev_df)),
            diff=diff,
            snapshot=prev_df,
            artifact_path=artifact if artifact.exists() else None,
            note=reason,
        )

    base = snapshot_dir(data_dir)
    dest = base / f"{FILENAME_PREFIX}{edition}.xlsx"
    try:
        fetch_meta = fetch_xlsx(url, dest)
        raw = parse_geninfo_workbook(dest)
        snapshot = normalize_geninfo(raw, edition)
    except Exception as e:
        status = STATUS_DEGRADED if not prev_df.empty else STATUS_ERROR
        logger.warning(
            "GenInfo fetch/parse failed for %s: %s: %s", edition, type(e).__name__, e,
        )
        return GenInfoResult(
            status=status,
            edition=meta.get("edition"),
            source_url=meta.get("source_url"),
            route=route,
            snapshot=prev_df,
            error=f"{edition}: {type(e).__name__}: {e}",
            note="last-known-good snapshot retained" if not prev_df.empty else None,
        )

    logger.info(
        "GenInfo %s fetched via %s: %d rows (%s)",
        edition, route, len(snapshot), reason,
    )
    _prune_raw_editions(base, keep=dest)

    # Diff baseline resolution (persisted, so a same-edition re-fetch keeps the
    # quarter-over-quarter view instead of diffing the edition against itself):
    #   1. stored content of an OLDER edition (the normal quarterly case:
    #      previous current-edition content becomes the new baseline);
    #   2. the stored baseline snapshot when the current edition is unchanged;
    #   3. a bootstrap fetch of the previous published edition (cold start).
    stored_edition = meta.get("edition")
    baseline_df, baseline_edition = load_baseline(data_dir)
    if stored_edition and stored_edition != edition and not prev_df.empty:
        new_baseline = prev_df  # the edition we already had becomes the baseline
        new_baseline_edition = stored_edition
    elif baseline_edition and baseline_edition != edition and not baseline_df.empty:
        new_baseline, new_baseline_edition = baseline_df, baseline_edition
    else:
        new_baseline, new_baseline_edition = pd.DataFrame(), None
        if bootstrap:
            try:
                older = discover_previous_edition(edition_info, today=today)
            except GenInfoError:
                older = None
            if older is not None:
                boot_dest = base / f"{FILENAME_PREFIX}{older['edition']}.xlsx"
                try:
                    fetch_xlsx(older["url"], boot_dest)
                    new_baseline = normalize_geninfo(
                        parse_geninfo_workbook(boot_dest), older["edition"],
                    )
                    new_baseline_edition = older["edition"]
                    logger.info(
                        "GenInfo baseline bootstrapped from edition %s (%d rows)",
                        older["edition"], len(new_baseline),
                    )
                    boot_dest.unlink(missing_ok=True)  # parse-and-slice: raw transient
                except Exception as e:
                    logger.warning(
                        "GenInfo baseline bootstrap failed: %s: %s", type(e).__name__, e,
                    )
                    new_baseline, new_baseline_edition = pd.DataFrame(), None
            else:
                logger.warning(
                    "GenInfo baseline bootstrap: no previous edition reachable — "
                    "publishing baseline-only artifact",
                )

    diff = diff_commitment_status(new_baseline, snapshot)

    # Same-edition republication: also report what changed within the edition
    # since the previously stored content (AEMO republishes inside a quarter).
    if stored_edition == edition and not prev_df.empty:
        revision = diff_commitment_status(prev_df, snapshot)
        if any(revision["counts"].values()):
            diff["same_edition_revision"] = {
                "counts": revision["counts"],
                "new_commitments": revision["new_commitments"],
                "withdrawals": revision["withdrawals"],
                "decommitments": revision["decommitments"],
                "commissioned": revision["commissioned"],
                "status_changes": revision["status_changes"],
            }
            logger.info(
                "GenInfo %s republication: %d change(s) since the stored content",
                edition, sum(revision["counts"].values()),
            )

    previous_edition = new_baseline_edition
    new_meta = {
        "edition": edition,
        "previous_edition": previous_edition,
        "source_url": url,
        "route": route,
        "checked_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rows": int(len(snapshot)),
        "fingerprint": snapshot_fingerprint(snapshot),
        "source_bytes": fetch_meta.get("content_length"),
        "source_last_modified": fetch_meta.get("last_modified"),
    }
    save_snapshot(data_dir, snapshot, new_meta)
    save_baseline(data_dir, new_baseline)

    payload = build_gen_info_payload(snapshot, new_meta, diff)
    artifact = publish_gen_info_json(payload, docs_data_dir)

    counts = diff["counts"]
    logger.info(
        "GenInfo %s: %d units; vs %s — %d new commitment(s), %d withdrawal(s), "
        "%d de-commitment(s), %d commissioned, %d new unit(s), %d removed",
        edition, len(snapshot), diff.get("compared_to_edition"),
        counts["new_commitments"], counts["withdrawals"], counts["decommitments"],
        counts["commissioned"], counts["new_units"], counts["removed_units"],
    )

    return GenInfoResult(
        status=STATUS_OK,
        edition=edition,
        source_url=url,
        route=route,
        fetched=True,
        row_count=int(len(snapshot)),
        diff=diff,
        snapshot=snapshot,
        events_by_duid=events_by_duid(diff),
        artifact_path=artifact,
        note=reason,
    )


# ─── CLI (manual/quarterly run; the daily lane calls run_geninfo_lane) ─────

def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="AEMO Generation Information quarterly fetcher + diff",
    )
    parser.add_argument("--data-dir", default=config.DATA_DIR)
    parser.add_argument("--docs-data-dir", default=config.DOCS_DATA_DIR)
    parser.add_argument("--force", action="store_true",
                        help="Re-download the workbook even when the rev is unchanged")
    parser.add_argument("--no-bootstrap", action="store_true",
                        help="Do not fetch the previous edition for a first-run baseline diff")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    result = run_geninfo_lane(
        args.data_dir, args.docs_data_dir,
        force=args.force, bootstrap=not args.no_bootstrap,
    )
    summary = {
        "status": result.status,
        "edition": result.edition,
        "source_url": result.source_url,
        "route": result.route,
        "fetched": result.fetched,
        "rows": result.row_count,
        "artifact": str(result.artifact_path) if result.artifact_path else None,
        "diff_counts": (result.diff or {}).get("counts"),
        "compared_to_edition": (result.diff or {}).get("compared_to_edition"),
        "note": result.note,
        "error": result.error,
    }
    print(json.dumps(summary, indent=2))
    return 0 if result.status == STATUS_OK else 1


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(_main())
