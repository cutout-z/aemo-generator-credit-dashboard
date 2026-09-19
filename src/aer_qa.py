"""AER quarterly market-statistics QA lane — cross-check only, no dashboard UI.

The AER re-publishes a small quarterly CSV suite (volume-weighted average spot
prices per region, counts of 30-minute prices below $0 / above $5,000, total
FCAS costs) roughly six-to-eight weeks after quarter end. There is no API and no
machine-readable index: the chart pages link the CSVs.

This lane ingests that suite and cross-checks it against our own derived
quarterly aggregates (``docs/data/market_quarterly.json``) with QED-style
warn-bands. It is a **QA process**: divergences are recorded as pass/warn per
series in ``docs/data/aer_qa.json`` and in the daily run manifest. There is
deliberately no chart, no panel and no ``index.html`` change — per the owner's
product decision (2026-09-19), AER data informs data quality, it is not a
dashboard surface. Warns never fail the run; only a total inability to read both
the suite and the last-known-good cache degrades the lane.

Access notes (live-verified 2026-09-19 from the Mac checkout):

- The four chart pages (``/industry/registers/charts/<slug>``) each return HTTP
  200 with a ~2.4 KB bot-management interstitial (``bm-verify`` refresh meta) —
  the slug exists, the content does not. So the lane resolves a CSV href from the
  page when it can, and otherwise falls back to the last-verified static URL in
  ``config.AER_QA_SEED_URLS`` (route recorded per series, never silent).
- The static ``/sites/default/files/<YYYY-MM>/...CSV`` assets serve fine
  (HTTP 200, ``application/octet-stream``, 1.4–2.9 KB). Their shape is plain:
  a header row (``Quarter ending`` or ``Quarter``) then one row per quarter,
  region columns labelled ``"<State> ($ per megawatt hour)"`` /
  ``"<State> (Number of trading intervals)"``. A blank cell means "not
  published", never zero.
- Seed URLs are pinned to a specific edition folder (``2026-08``). A stale seed
  is detected, not compared blindly: the lane records the latest quarter the
  edition actually covers and reports ``awaiting_edition`` (no comparison, no
  warn) whenever an expected-newer quarter is absent from it.

Comparators (``config.AER_QA_BANDS``):

- ``band_contains`` (VWA spot price): AER's published quarterly regional VWA must
  land inside our derived quarterly price band ``[avg_vwap_low, avg_vwap_high]``
  (the bottom- and top-decile VWAPs), with a proportional slack. A regulator
  number outside both deciles means our price derivation has drifted.
- ``share_ratio`` (below-$0 count): AER's count of 30-minute trading intervals is
  converted to a share of the quarter's trading intervals (48/day) and compared
  ratio-wise against our ``neg_price_share`` (share of 5-minute dispatch
  intervals). Different denominators by construction — the band is wide and the
  verdict is a gross-divergence alarm, not an equality test. Counts below
  ``config.AER_QA_MIN_COUNT`` on both sides are ``below_noise_floor`` passes.
- Series with no counterpart in our artifact (``>$5,000`` counts, NEM FCAS cost
  totals) are ingested and published as reference values marked
  ``not_comparable`` with the reason — never dropped, never faked into a pass.

Machine-local cache: ``data/aer_qa/`` (parsed reference values + edition
metadata), never committed. Published artifact: ``docs/data/aer_qa.json``
through the S3-12 semantic-diff publish gate.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from . import config
from .market_factors import MARKET_QUARTERLY_JSON
from .run_status import STATUS_DEGRADED, STATUS_ERROR, STATUS_OK
from .semantic_publish import write_json_if_facts_changed

logger = logging.getLogger(__name__)

# ─── Series keys (must match config.AER_QA_SERIES) ──────────────────────────

SERIES_VWAP = "vwap_region_quarter"
SERIES_NEG_PRICE = "neg_price_count"
SERIES_HIGH_PRICE = "high_price_count_5000"
SERIES_FCAS_COST = "fcas_total_cost"

COMPARATOR_BAND = "band_contains"
COMPARATOR_SHARE = "share_ratio"

OUTCOME_PASS = "pass"
OUTCOME_WARN = "warn"
OUTCOME_SKIP = "skip"
OUTCOME_NOT_COMPARABLE = "not_comparable"
OUTCOME_AWAITING_EDITION = "awaiting_edition"
OUTCOME_BELOW_FLOOR = "below_noise_floor"

# AER column labels are human ("New South Wales ($ per megawatt hour)"). Map the
# label (parenthetical stripped, lowercased) to our region codes.
AER_REGION_CODES = {
    "queensland": "QLD1",
    "new south wales": "NSW1",
    "victoria": "VIC1",
    "south australia": "SA1",
    "tasmania": "TAS1",
}

QUARTER_LABEL_RE = re.compile(r"^(\d{4})\s*Q([1-4])$", re.IGNORECASE)
HEADER_HINT_RE = re.compile(r"^quarter", re.IGNORECASE)
# Bot-management interstitial marker: the page is a redirect stub, not content.
BOT_WALL_MARKERS = ("bm-verify", "incapsula", "just a moment")

ARTIFACT_FILENAME = "aer_qa.json"
CACHE_DIRNAME = "aer_qa"
VALUES_FILENAME = "aer_qa_reference.feather"
META_FILENAME = "aer_qa_meta.json"

ROUTE_PAGE = "page_scrape"
ROUTE_SEED = "seed_url"
ROUTE_CACHE = "cache"

MAX_CHECKS = 240
MAX_REFERENCE_QUARTERS = 8

REFERENCE_COLUMNS = ("series", "quarter", "region", "value")


class AerQaError(RuntimeError):
    """Base failure for the AER QA lane (fetch/parse)."""


class AerQaAccessError(AerQaError):
    """AER refused or walled the request — fall back to the seed, never hammer."""


# ─── Quarter arithmetic ────────────────────────────────────────────────────

def quarter_label(value) -> str | None:
    """Normalise an AER quarter cell to our ``YYYYQn`` label (None if unusable)."""
    if value is None:
        return None
    text = str(value).strip()
    match = QUARTER_LABEL_RE.match(text)
    if match:
        return f"{match.group(1)}Q{match.group(2)}"
    match = re.match(r"^(\d{4})-?Q([1-4])$", text, re.IGNORECASE)
    if match:
        return f"{match.group(1)}Q{match.group(2)}"
    return None


def quarter_sort_key(label: str) -> tuple[int, int]:
    match = QUARTER_LABEL_RE.match(str(label))
    if not match:
        raise AerQaError(f"not a quarter label: {label!r}")
    return int(match.group(1)), int(match.group(2))


def quarter_end(quarter: str) -> date:
    """Last calendar day of a ``YYYYQn`` quarter."""
    year, q = quarter_sort_key(quarter)
    if q == 1:
        return date(year, 3, 31)
    if q == 2:
        return date(year, 6, 30)
    if q == 3:
        return date(year, 9, 30)
    return date(year, 12, 31)


def quarter_days(quarter: str) -> int:
    """Calendar days in the quarter (the denominator for trading intervals)."""
    year, q = quarter_sort_key(quarter)
    start_month = 3 * (q - 1) + 1
    start = date(year, start_month, 1)
    end = quarter_end(quarter)
    return (end - start).days + 1


def expected_reference_quarter(today: date | None = None, *, lag_weeks: int | None = None) -> str:
    """Latest quarter the AER should have published by ``today``.

    The suite lands ~6–8 weeks after quarter end, so the reference quarter is the
    most recent one whose end + ``lag_weeks`` is not in the future. Used to tell
    "new quarter not published yet" (``awaiting_edition``, never a warn) apart
    from "the edition we hold is stale".
    """
    today = today or date.today()
    lag = config.AER_QA_PUBLISH_LAG_WEEKS if lag_weeks is None else lag_weeks
    for year in (today.year, today.year - 1):
        for q in (4, 3, 2, 1):
            label = f"{year}Q{q}"
            published_by = quarter_end(label).toordinal() + lag * 7
            if published_by <= today.toordinal():
                return label
    raise AerQaError(f"no reference quarter resolvable for {today}")


# ─── HTTP plumbing (same shape as the GenInfo / network-outage lanes) ───────

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
    """One HTTP call with bounded retries; 403 fails fast (never hammered)."""
    timeout = timeout or config.REQUEST_TIMEOUT
    last_error: Exception | None = None
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = _session().request(method, url, timeout=timeout, allow_redirects=True)
        except requests.RequestException as e:
            last_error = e
        else:
            if resp.status_code == 403:
                raise AerQaAccessError(f"HTTP 403 (block) for {url}")
            if resp.status_code < 400:
                return resp
            last_error = AerQaError(f"HTTP {resp.status_code} for {url}")
        if attempt < config.MAX_RETRIES - 1:
            time.sleep(config.RETRY_BACKOFF * (attempt + 1))
    raise AerQaError(f"request failed after {config.MAX_RETRIES} attempts: {last_error}")


def _fetch(url: str) -> tuple[bytes, dict]:
    """Default fetcher: GET the URL and return ``(body, http_meta)``."""
    resp = _request("GET", url)
    return resp.content, {
        "status": resp.status_code,
        "content_type": resp.headers.get("content-type"),
        "last_modified": resp.headers.get("last-modified"),
        "bytes": len(resp.content),
    }


def is_bot_wall(body: bytes, content_type: str | None = None) -> bool:
    """True when a response is a bot-management interstitial rather than content.

    Two shapes count as unusable: an explicit interstitial (``bm-verify`` and
    friends) and an HTML page that carries no CSV link at all — a chart page we
    cannot scrape yields no href, so it takes the seed-URL fallback path exactly
    like a walled one (and says so in the route note).
    """
    head = body[:8192].decode("utf-8", errors="replace").lower()
    if any(marker in head for marker in BOT_WALL_MARKERS):
        return True
    if "text/html" in (content_type or "").lower():
        return not _HREF_RE.search(head)
    return False


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


# ─── CSV href resolution (page scrape → seed fallback) ─────────────────────

_HREF_RE = re.compile(r"""href=["']([^"']+\.csv[^"']*)["']""", re.IGNORECASE)


def hrefs_in_html(html: str) -> list[str]:
    """CSV hrefs in a chart page, deduped, order preserved."""
    seen: dict[str, None] = {}
    for match in _HREF_RE.finditer(html):
        seen.setdefault(match.group(1), None)
    return list(seen)


def match_href(hrefs: list[str], tokens: tuple[str, ...]) -> str | None:
    """First href whose decoded, lowercased URL contains every match token."""
    from urllib.parse import unquote
    for href in hrefs:
        text = unquote(str(href)).lower()
        if all(token.lower() in text for token in tokens):
            return href
    return None


def resolve_csv_url(
    series_key: str,
    *,
    fetch=None,
    base_url: str | None = None,
) -> dict:
    """Resolve a series' static CSV URL: chart-page scrape, else last-verified seed.

    Returns ``{url, route, note}``. A walled/blocked/failed page is not an error:
    the AER CSVs are static assets and serve fine from networks where the CMS is
    behind a bot-management interstitial, so the seed URL is the documented
    fallback. The route is recorded so an operator can see which path produced
    the numbers.
    """
    spec = config.AER_QA_SERIES.get(series_key)
    if not spec:
        raise AerQaError(f"unknown AER QA series: {series_key}")
    fetch = fetch or _fetch
    base = (base_url or config.AER_BASE_URL).rstrip("/")
    page_url = base + spec["page"]
    note = None
    try:
        body, meta = fetch(page_url)
        content_type = meta.get("content_type") if isinstance(meta, dict) else None
        if not is_bot_wall(body, content_type):
            href = match_href(hrefs_in_html(body.decode("utf-8", errors="replace")),
                              tuple(spec.get("csv_match") or ()))
            if href:
                if href.startswith("http"):
                    return {"url": href, "route": ROUTE_PAGE, "note": None}
                if href.startswith("/"):
                    return {"url": base + href, "route": ROUTE_PAGE, "note": None}
                note = f"relative href without a leading slash ({href})"
            else:
                note = "chart page carried no matching CSV href"
        else:
            note = "chart page is a bot-management interstitial"
    except Exception as e:  # noqa: BLE001 — any page failure falls back to the seed
        note = f"chart page fetch failed: {type(e).__name__}: {e}"

    seed = config.AER_QA_SEED_URLS.get(series_key)
    if not seed:
        raise AerQaError(f"{series_key}: no seed URL and page route unusable ({note})")
    return {
        "url": seed,
        "route": ROUTE_SEED,
        "note": f"{note}; used last-verified seed URL" if note else "seed URL",
    }


# ─── CSV parsing ───────────────────────────────────────────────────────────

def _header_row_index(rows: list[list[str]]) -> int | None:
    for index, row in enumerate(rows):
        if row and HEADER_HINT_RE.match(str(row[0]).strip()):
            return index
    return None


def region_code_from_label(label: str) -> str | None:
    """``"New South Wales ($ per megawatt hour)"`` → ``"NSW1"`` (None if unknown)."""
    text = re.sub(r"\(.*?\)", "", str(label)).strip().lower()
    text = re.sub(r"\s+", " ", text)
    if text in AER_REGION_CODES:
        return AER_REGION_CODES[text]
    for name, code in AER_REGION_CODES.items():
        if text.startswith(name):
            return code
    return None


def _to_float(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_aer_csv(text: str, series_key: str) -> pd.DataFrame:
    """Parse an AER quarterly CSV into long ``(series, quarter, region, value)``.

    Handles the two published shapes (``Quarter ending`` for prices, ``Quarter``
    for counts/costs), trailing blank rows, blank cells (not published → dropped,
    never zero-filled) and a missing region column (NEM-wide series → region None).
    """
    reader = csv.reader(io.StringIO(text))
    rows = [row for row in reader if any(str(cell).strip() for cell in row)]
    header_index = _header_row_index(rows)
    if header_index is None:
        raise AerQaError(f"{series_key}: no quarter header row found")
    header = rows[header_index]
    columns = [region_code_from_label(cell) for cell in header[1:]]
    records: list[dict] = []
    for row in rows[header_index + 1:]:
        quarter = quarter_label(row[0] if row else None)
        if quarter is None:
            continue
        for offset, region in enumerate(columns):
            cell_index = offset + 1
            if cell_index >= len(row):
                break
            value = _to_float(row[cell_index])
            if value is None:
                continue
            records.append({
                "series": series_key,
                "quarter": quarter,
                "region": region,
                "value": value,
            })
    frame = pd.DataFrame(records, columns=list(REFERENCE_COLUMNS))
    logger.info(
        "AER %s: parsed %d value(s) across %d quarter(s)",
        series_key, len(frame),
        frame["quarter"].nunique() if not frame.empty else 0,
    )
    return frame


def latest_quarter(frame: pd.DataFrame) -> str | None:
    if frame is None or frame.empty:
        return None
    quarters = [q for q in frame["quarter"].unique() if q]
    if not quarters:
        return None
    return sorted(quarters, key=quarter_sort_key)[-1]


# ─── Comparators ───────────────────────────────────────────────────────────

def compare_band_contains(aer_value, ours_lo, ours_hi, *, tolerance_ratio: float) -> dict:
    """AER's single published value must sit in our derived ``[lo, hi]`` band.

    ``tolerance_ratio`` is proportional slack on the band width (``0.15`` = 15%),
    so a regulator value just outside a narrow band is not a false alarm.
    """
    if aer_value is None or ours_lo is None or ours_hi is None:
        return {"outcome": OUTCOME_SKIP, "detail": "value missing on one side"}
    lo, hi = float(min(ours_lo, ours_hi)), float(max(ours_lo, ours_hi))
    slack = abs(hi - lo) * float(tolerance_ratio)
    value = float(aer_value)
    if lo - slack <= value <= hi + slack:
        return {
            "outcome": OUTCOME_PASS,
            "detail": f"published {value:g} inside derived band "
                      f"[{lo:g}, {hi:g}] (slack {slack:.2f})",
        }
    return {
        "outcome": OUTCOME_WARN,
        "detail": f"published {value:g} OUTSIDE derived band "
                  f"[{lo:g}, {hi:g}] (slack {slack:.2f})",
    }


def compare_share_ratio(
    aer_count, aer_periods, our_share, *,
    ratio_min: float, ratio_max: float, noise_floor: int,
) -> dict:
    """Ratio-band the AER interval count (as a share) against our derived share.

    Denominators differ by construction (AER counts 30-minute trading intervals,
    our ``neg_price_share`` is the share of 5-minute dispatch intervals), so the
    band is wide and the verdict is a gross-divergence alarm. Counts below
    ``noise_floor`` on both sides are inconclusive rather than alarming: a
    5-interval quarter cannot support a ratio.
    """
    if aer_count is None or not aer_periods:
        return {"outcome": OUTCOME_SKIP, "detail": "count or denominator missing"}
    count = float(aer_count)
    periods = float(aer_periods)
    aer_share = count / periods
    our = None if our_share is None else float(our_share)
    our_count = None if our is None else our * periods
    if count < noise_floor and (our_count is None or our_count < noise_floor):
        return {
            "outcome": OUTCOME_BELOW_FLOOR,
            "detail": f"AER {count:g} and derived {0 if our_count is None else our_count:.0f} "
                      f"interval(s) both below the {noise_floor} noise floor — inconclusive",
        }
    if our is None:
        return {"outcome": OUTCOME_SKIP, "detail": "derived share missing"}
    if our <= 0:
        return {
            "outcome": OUTCOME_WARN,
            "detail": f"AER counts {count:g} interval(s) ({aer_share:.4%}) where our "
                      f"derived share is 0 — we are missing negative-price intervals",
        }
    if aer_share <= 0:
        return {
            "outcome": OUTCOME_WARN,
            "detail": f"AER publishes 0 intervals where our derived share is {our:.4%}",
        }
    ratio = our / aer_share
    detail = (f"derived share {our:.4%} vs AER {aer_share:.4%} "
              f"({count:g}/{periods:g}) — ratio {ratio:.2f}")
    if ratio_min <= ratio <= ratio_max:
        return {"outcome": OUTCOME_PASS, "detail": detail + " in band"}
    return {
        "outcome": OUTCOME_WARN,
        "detail": detail + f" OUTSIDE [{ratio_min}, {ratio_max}]",
    }


# ─── Cross-check engine ────────────────────────────────────────────────────

def load_our_quarterly(docs_data_dir: str | Path) -> dict[tuple[str, str], dict]:
    """Our derived region×quarter rows from ``market_quarterly.json``."""
    path = Path(docs_data_dir) / MARKET_QUARTERLY_JSON
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        logger.warning("market_quarterly.json unreadable for the AER check: %s", e)
        return {}
    rows = payload.get("rows") if isinstance(payload, dict) else None
    out: dict[tuple[str, str], dict] = {}
    for row in rows or []:
        quarter = quarter_label(row.get("quarter"))
        region = row.get("region")
        if quarter and region:
            out[(str(region), quarter)] = row
    return out


def _series_values(reference: pd.DataFrame, series_key: str, quarter: str) -> dict:
    """``{region: value}`` (region None → key ``"NEM"``) for one series+quarter."""
    if reference is None or reference.empty:
        return {}
    df = reference[(reference["series"] == series_key) & (reference["quarter"] == quarter)]
    out: dict[str, float] = {}
    for row in df.itertuples():
        out["NEM" if row.region is None or pd.isna(row.region) else str(row.region)] = float(row.value)
    return out


def build_checks(
    reference: pd.DataFrame,
    our_rows: dict[tuple[str, str], dict],
    *,
    reference_quarter: str | None,
    bands: dict | None = None,
) -> dict:
    """Per-series pass/warn/skip verdicts for the quarters we can compare.

    A quarter the edition does not cover (yet) is recorded as
    ``awaiting_edition`` — a publication-lag fact, never a data-quality warn.
    """
    bands = bands or config.AER_QA_BANDS
    our_quarters = sorted({q for (_r, q) in our_rows}, key=quarter_sort_key)
    out: dict[str, dict] = {}
    for series_key, spec in config.AER_QA_SERIES.items():
        band = bands.get(series_key) or {}
        comparator = band.get("comparator")
        quarters = sorted(
            {str(q) for q in (reference["quarter"].unique() if not reference.empty else [])}
            if series_key in set(reference["series"].unique() if not reference.empty else [])
            else set(),
            key=quarter_sort_key,
        )
        entry = {
            "what": spec.get("what"),
            "scope": spec.get("scope"),
            "unit": spec.get("unit"),
            "comparator": comparator,
            "checks": [],
            "counts": {OUTCOME_PASS: 0, OUTCOME_WARN: 0, OUTCOME_SKIP: 0,
                       OUTCOME_NOT_COMPARABLE: 0, OUTCOME_AWAITING_EDITION: 0,
                       OUTCOME_BELOW_FLOOR: 0},
        }

        def _add(check: dict) -> None:
            outcome = check.get("outcome")
            if outcome in entry["counts"]:
                entry["counts"][outcome] += 1
            if len(entry["checks"]) < MAX_CHECKS:
                entry["checks"].append(check)

        if comparator is None:
            _add({
                "series": series_key,
                "outcome": OUTCOME_NOT_COMPARABLE,
                "detail": band.get("reason") or "no counterpart in our quarterly artifact",
                "reference_quarters": quarters[-MAX_REFERENCE_QUARTERS:],
            })
            out[series_key] = entry
            continue

        if not quarters:
            _add({"series": series_key, "outcome": OUTCOME_SKIP,
                  "detail": "edition carried no values for this series"})
            out[series_key] = entry
            continue

        compared = False
        for quarter in our_quarters:
            if quarter not in quarters:
                if reference_quarter and quarter_sort_key(quarter) > quarter_sort_key(
                    latest_quarter(reference) or quarter
                ):
                    _add({
                        "series": series_key, "quarter": quarter,
                        "outcome": OUTCOME_AWAITING_EDITION,
                        "detail": f"edition covers through {latest_quarter(reference)}; "
                                  f"{quarter} not published yet (reference {reference_quarter})",
                    })
                continue
            values = _series_values(reference, series_key, quarter)
            periods = quarter_days(quarter) * config.AER_QA_TRADING_INTERVALS_PER_DAY
            for (region, row_quarter), row in our_rows.items():
                if row_quarter != quarter:
                    continue
                compared = True
                aer_value = values.get(region)
                check = {
                    "series": series_key, "quarter": quarter, "region": region,
                    "aer_value": None if aer_value is None else float(aer_value),
                }
                if comparator == COMPARATOR_BAND:
                    verdict = compare_band_contains(
                        aer_value, row.get("avg_vwap_low"), row.get("avg_vwap_high"),
                        tolerance_ratio=float(band.get("tolerance_ratio", 0.0)),
                    )
                    check["ours"] = {
                        "avg_vwap_low": row.get("avg_vwap_low"),
                        "avg_vwap_high": row.get("avg_vwap_high"),
                    }
                elif comparator == COMPARATOR_SHARE:
                    verdict = compare_share_ratio(
                        aer_value, periods, row.get("neg_price_share"),
                        ratio_min=float(band.get("ratio_min", 0.0)),
                        ratio_max=float(band.get("ratio_max", 1e9)),
                        noise_floor=int(band.get("noise_floor", 0)),
                    )
                    check["ours"] = {
                        "neg_price_share": row.get("neg_price_share"),
                        "aer_periods": periods,
                    }
                else:
                    verdict = {"outcome": OUTCOME_SKIP,
                               "detail": f"unknown comparator {comparator!r}"}
                check.update(verdict)
                _add(check)
        if not compared and not entry["checks"]:
            _add({
                "series": series_key, "outcome": OUTCOME_AWAITING_EDITION,
                "detail": "no quarter in the edition overlaps our quarterly artifact",
            })
        out[series_key] = entry
    return out


def _findings(checks: dict) -> list[str]:
    """Operator-facing warn lines (the only thing the run surfaces as a flag)."""
    findings: list[str] = []
    for series_key, entry in checks.items():
        for check in entry["checks"]:
            if check.get("outcome") != OUTCOME_WARN:
                continue
            where = " ".join(str(part) for part in (
                check.get("quarter"), check.get("region"),
            ) if part)
            findings.append(f"AER QA {series_key} {where}: {check.get('detail')}".strip())
    return findings


# ─── Cache + payload ───────────────────────────────────────────────────────

def cache_dir(data_dir: str | Path) -> Path:
    return Path(data_dir) / CACHE_DIRNAME


def load_cache(data_dir: str | Path) -> tuple[pd.DataFrame, dict]:
    """Last-known-good parsed reference values + edition metadata."""
    base = cache_dir(data_dir)
    frame = pd.DataFrame(columns=list(REFERENCE_COLUMNS))
    meta: dict = {}
    values_path = base / VALUES_FILENAME
    if values_path.exists():
        try:
            frame = pd.read_feather(values_path)
        except Exception as e:  # noqa: BLE001 — cache corruption is not fatal
            logger.warning("AER QA reference cache unreadable (%s) — treating as absent", e)
            frame = pd.DataFrame(columns=list(REFERENCE_COLUMNS))
    meta_path = base / META_FILENAME
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, ValueError) as e:
            logger.warning("AER QA cache metadata unreadable: %s", e)
            meta = {}
    return frame, meta


def save_cache(data_dir: str | Path, frame: pd.DataFrame, meta: dict) -> Path:
    base = cache_dir(data_dir)
    base.mkdir(parents=True, exist_ok=True)
    path = base / VALUES_FILENAME
    frame.to_feather(path)
    (base / META_FILENAME).write_text(json.dumps(meta, indent=1, sort_keys=True))
    return path


def _reference_payload(
    reference: pd.DataFrame, *, max_quarters: int = MAX_REFERENCE_QUARTERS,
) -> dict:
    """Recent published reference values (bounded) for the artifact."""
    if reference is None or reference.empty:
        return {}
    quarters = sorted({str(q) for q in reference["quarter"].unique()},
                      key=quarter_sort_key)[-max_quarters:]
    out: dict[str, dict] = {}
    for series_key in config.AER_QA_SERIES:
        series_values = {}
        for quarter in quarters:
            values = _series_values(reference, series_key, quarter)
            if values:
                series_values[quarter] = {k: round(v, 4) for k, v in sorted(values.items())}
        if series_values:
            out[series_key] = series_values
    return out


def build_aer_qa_payload(
    checks: dict,
    meta: dict,
    reference: pd.DataFrame,
    *,
    reference_quarter: str | None,
) -> dict:
    """Compact QA artifact: edition identity, pass/warn counts, warn findings."""
    totals = {OUTCOME_PASS: 0, OUTCOME_WARN: 0, OUTCOME_SKIP: 0,
              OUTCOME_NOT_COMPARABLE: 0, OUTCOME_AWAITING_EDITION: 0,
              OUTCOME_BELOW_FLOOR: 0}
    for entry in checks.values():
        for outcome, count in entry["counts"].items():
            totals[outcome] = totals.get(outcome, 0) + count
    findings = _findings(checks)
    edition_quarter = latest_quarter(reference)
    awaiting = bool(
        edition_quarter and reference_quarter
        and quarter_sort_key(edition_quarter) < quarter_sort_key(reference_quarter)
    )
    payload = {
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "qa_only": True,
        "note": ("QA cross-check only — no dashboard panel or chart consumes this "
                 "file (owner decision 2026-09-19)."),
        "edition": {
            "latest_quarter": edition_quarter,
            "reference_quarter_expected": reference_quarter,
            "awaiting_edition": awaiting,
            "source_urls": meta.get("source_urls", {}),
            "routes": meta.get("routes", {}),
            "content_sha256": meta.get("content_sha256", {}),
        },
        "summary": {
            "series": len(checks),
            "checks": sum(totals.values()),
            "passed": totals[OUTCOME_PASS],
            "warned": totals[OUTCOME_WARN],
            "not_comparable": totals[OUTCOME_NOT_COMPARABLE],
            "awaiting_edition": totals[OUTCOME_AWAITING_EDITION],
            "below_noise_floor": totals[OUTCOME_BELOW_FLOOR],
            "skipped": totals[OUTCOME_SKIP],
            "findings": len(findings),
        },
        "findings": findings,
        "series": checks,
        "reference_values": _reference_payload(reference),
        "source_status": {
            "status": meta.get("status") or STATUS_OK,
            "error": meta.get("error"),
        },
    }
    return payload


def publish_aer_qa_json(payload: dict, docs_data_dir: str | Path) -> Path | None:
    """Publish docs/data/aer_qa.json through the semantic-diff gate (QA only)."""
    out_dir = Path(docs_data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / ARTIFACT_FILENAME
    wrote = write_json_if_facts_changed(path, payload)
    if wrote:
        logger.info("Published AER QA artifact to %s", path)
    return path if wrote else None


# ─── Lane orchestration ────────────────────────────────────────────────────

@dataclass
class AerQaResult:
    status: str = STATUS_OK
    fetched: bool = False
    edition_quarter: str | None = None
    reference_quarter: str | None = None
    checks: dict = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    payload: dict | None = None
    artifact_path: Path | None = None
    note: str | None = None
    error: str | None = None

    @property
    def retained(self) -> bool:
        return self.status == STATUS_DEGRADED and not self.fetched


def fetch_edition(*, fetch=None) -> tuple[pd.DataFrame, dict, list[str]]:
    """Fetch + parse every configured series. Returns ``(frame, meta, problems)``.

    Per-series failures are collected, not raised: the suite is four independent
    static files, so one 404 must not blank the whole lane. A series that fails
    contributes no values (its checks then record ``awaiting_edition``/``skip``)
    and a problem line for the operator.
    """
    fetch = fetch or _fetch
    frames: list[pd.DataFrame] = []
    problems: list[str] = []
    source_urls: dict[str, str] = {}
    routes: dict[str, str] = {}
    hashes: dict[str, str] = {}
    fetched_any = False
    for series_key in config.AER_QA_SERIES:
        try:
            resolved = resolve_csv_url(series_key, fetch=fetch)
            body, http_meta = fetch(resolved["url"])
            content_type = http_meta.get("content_type") if isinstance(http_meta, dict) else None
            if is_bot_wall(body, content_type):
                raise AerQaError("response is not a CSV (bot-management interstitial)")
            frame = parse_aer_csv(body.decode("utf-8", errors="replace"), series_key)
        except Exception as e:  # noqa: BLE001 — one series must not kill the lane
            problems.append(f"{series_key}: {type(e).__name__}: {e}")
            logger.warning("AER QA series %s unavailable: %s", series_key, e)
            continue
        source_urls[series_key] = resolved["url"]
        routes[series_key] = resolved["route"]
        hashes[series_key] = sha256_hex(body)
        if resolved.get("note"):
            problems.append(f"{series_key}: {resolved['note']}")
        if not frame.empty:
            frames.append(frame)
            fetched_any = True
    frame = (
        pd.concat(frames, ignore_index=True) if frames
        else pd.DataFrame(columns=list(REFERENCE_COLUMNS))
    )
    meta = {
        "source_urls": source_urls,
        "routes": routes,
        "content_sha256": hashes,
        "fetched": fetched_any,
        "checked_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "problems": problems,
    }
    return frame, meta, problems


def run_aer_qa_lane(
    data_dir: str | Path,
    docs_data_dir: str | Path,
    *,
    today: date | None = None,
    force: bool = False,
    fetch=None,
) -> AerQaResult:
    """Cross-check the newest AER edition against our quarterly artifact.

    Never raises on an AER-side failure: the result carries the lane status so
    the daily pipeline records it in the run manifest. Warns never fail the run —
    the band verdicts are the product, and a QA lane that can redden a pipeline
    gets switched off rather than fixed. ``degraded`` means "no fresh edition and
    nothing cached"; ``error`` means "no data at all".
    """
    data_dir, docs_data_dir = Path(data_dir), Path(docs_data_dir)
    cached_frame, cached_meta = load_cache(data_dir)
    try:
        reference_quarter = expected_reference_quarter(today)
    except AerQaError as e:
        logger.warning("AER QA reference-quarter resolution failed: %s", e)
        reference_quarter = None

    frame = pd.DataFrame(columns=list(REFERENCE_COLUMNS))
    meta: dict = {}
    problems: list[str] = []
    fetched = False
    note = None
    error = None

    try:
        fresh, fetch_meta, problems = fetch_edition(fetch=fetch)
    except Exception as e:  # noqa: BLE001 — defensive: fetch_edition collects its own
        fresh, fetch_meta, problems = pd.DataFrame(columns=list(REFERENCE_COLUMNS)), {}, [
            f"{type(e).__name__}: {e}",
        ]
        logger.warning("AER QA edition fetch failed outright: %s", e)

    unchanged = bool(
        fresh.empty and not force and not cached_frame.empty
        and fetch_meta.get("content_sha256")
        and fetch_meta.get("content_sha256") == cached_meta.get("content_sha256")
    )
    if not fresh.empty:
        cached_hashes = cached_meta.get("content_sha256") or {}
        unchanged = bool(
            not force and cached_hashes
            and all(fetch_meta["content_sha256"].get(k) == v for k, v in cached_hashes.items())
            and set(fetch_meta["content_sha256"]) == set(cached_hashes)
        )

    if not fresh.empty:
        frame, meta, fetched = fresh, fetch_meta, True
        if unchanged:
            meta["note"] = "edition unchanged (content hashes match the cache)"
            note = "edition unchanged (content hashes match the cache)"
        save_cache(data_dir, frame, meta)
    elif not cached_frame.empty:
        frame = cached_frame
        meta = dict(cached_meta)
        meta["status"] = STATUS_DEGRADED
        meta["error"] = "; ".join(problems) or "no series fetched"
        note = "last-known-good AER edition retained"
    else:
        meta = {
            "status": STATUS_ERROR,
            "error": "; ".join(problems) or "no series fetched and no cached edition",
            "issues": problems,
        }

    checks = build_checks(frame, load_our_quarterly(docs_data_dir), reference_quarter=reference_quarter)
    warnings = _findings(checks)
    payload = build_aer_qa_payload(checks, meta, frame, reference_quarter=reference_quarter)
    artifact = publish_aer_qa_json(payload, docs_data_dir)

    if frame.empty:
        status = STATUS_ERROR
        error = meta.get("error") or "no AER edition available"
    elif not fetched:
        status = STATUS_DEGRADED
        error = meta.get("error")
    else:
        status = STATUS_OK

    edition_quarter = latest_quarter(frame)
    summary = payload["summary"]
    if note is None:
        note = (
            f"edition through {edition_quarter}: {summary['passed']} pass / "
            f"{summary['warned']} warn / {summary['not_comparable']} reference-only "
            f"across {summary['series']} series"
        )
    if warnings:
        note += f"; {len(warnings)} warn-band finding(s)"
    logger.info("AER QA lane: %s", note)

    return AerQaResult(
        status=status,
        fetched=fetched,
        edition_quarter=edition_quarter,
        reference_quarter=reference_quarter,
        checks=checks,
        warnings=warnings,
        payload=payload,
        artifact_path=artifact or (Path(docs_data_dir) / ARTIFACT_FILENAME),
        note=note,
        error=error,
    )


# ─── CLI (manual run; the daily lane calls run_aer_qa_lane) ────────────────

def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="AER quarterly market-statistics QA cross-check (QA only, no chart)",
    )
    parser.add_argument("--data-dir", default=None, help="machine-local cache dir")
    parser.add_argument("--docs-data-dir", default=None, help="published docs/data dir")
    parser.add_argument("--force", action="store_true", help="ignore edition-change detection")
    parser.add_argument("--verbose", action="store_true", help="log every check detail")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(name)s: %(message)s")
    root = Path(__file__).resolve().parent.parent
    data_dir = Path(args.data_dir) if args.data_dir else root / config.DATA_DIR
    docs_data_dir = Path(args.docs_data_dir) if args.docs_data_dir else root / config.DOCS_DATA_DIR

    result = run_aer_qa_lane(data_dir, docs_data_dir, force=args.force)
    print(json.dumps({
        "status": result.status,
        "edition_quarter": result.edition_quarter,
        "reference_quarter": result.reference_quarter,
        "fetched": result.fetched,
        "summary": (result.payload or {}).get("summary"),
        "findings": result.warnings,
        "note": result.note,
        "error": result.error,
        "artifact": str(result.artifact_path) if result.artifact_path else None,
    }, indent=1))
    return 0 if result.status == STATUS_OK else 1


if __name__ == "__main__":
    raise SystemExit(_main())
