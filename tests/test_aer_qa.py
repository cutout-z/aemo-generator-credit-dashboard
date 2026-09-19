"""Tests for the AER quarterly market-statistics QA lane.

Hermetic: canned AER CSV bytes via an injected fetcher + tmp dirs only — no
network, no repo writes. The real parse / compare / cache / publish code paths
run; only the HTTP boundary (``aer_qa._fetch``) is replaced.

The lane is deliberately QA-only: a test asserts the publish writes nothing but
``docs/data/aer_qa.json`` (no ``index.html``, no chart payload).
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src import aer_qa as aq
from src.aer_qa import (
    ARTIFACT_FILENAME,
    OUTCOME_AWAITING_EDITION,
    OUTCOME_BELOW_FLOOR,
    OUTCOME_NOT_COMPARABLE,
    OUTCOME_PASS,
    OUTCOME_WARN,
    AerQaError,
    build_checks,
    compare_band_contains,
    compare_share_ratio,
    expected_reference_quarter,
    fetch_edition,
    hrefs_in_html,
    is_bot_wall,
    latest_quarter,
    load_cache,
    match_href,
    parse_aer_csv,
    quarter_days,
    quarter_end,
    quarter_label,
    region_code_from_label,
    run_aer_qa_lane,
)
from src.run_status import STATUS_DEGRADED, STATUS_ERROR, STATUS_OK

VWA_CSV = (
    "Quarter ending,\"Queensland ($ per megawatt hour)\","
    "\"New South Wales ($ per megawatt hour)\","
    "\"Victoria ($ per megawatt hour)\","
    "\"South Australia ($ per megawatt hour)\","
    "\"Tasmania ($ per megawatt hour)\"\r\n"
    "2026 Q1,69,81,50,144,95\r\n"
    "2026 Q2,69,78,60,95,87\r\n"
    "2026 Q3,,,,\r\n"          # edition not yet covering Q3 → blank cells
    ",,,,,\r\n"                # trailing blank row
)

NEG_CSV = (
    "Quarter,\"Queensland (Number of trading intervals)\","
    "\"New South Wales (Number of trading intervals)\","
    "\"Victoria (Number of trading intervals)\","
    "\"South Australia (Number of trading intervals)\","
    "\"Tasmania (Number of trading intervals)\"\r\n"
    "2026 Q1,389,241,1080,1258,5\r\n"
    "2026 Q2,350,118,913,847,8\r\n"
)

HI_CSV = (
    "Quarter,\"Queensland (Number of trading intervals)\","
    "\"New South Wales (Number of trading intervals)\"\r\n"
    "2026 Q2,0,0\r\n"
)

FCAS_CSV = (
    "Quarter,Total FCAS Costs ($m)\r\n"
    "2026 Q1,13.63700475\r\n"
    "2026 Q2,9.158\r\n"
)

BOT_WALL = (
    "<!DOCTYPE html><html><head> <meta charset=\"utf-8\"> "
    "<meta http-equiv=\"refresh\" content=\"5; URL='/x?bm-verify=AAQAAAAO_____3QZ6f'\">"
    "</head><body></body></html>"
).encode()

PAGE_HTML = b"""<html><body>
<a href="/sites/default/files/2026-08/AER_Spot%20prices_Quarterly%20VWA%20spot%20prices%20DATA_2_20260807084204.CSV">Download CSV</a>
<a href="/sites/default/files/2026-08/AER_Spot%20prices_Quarterly%20count%20of%20spot%20prices%20below%20%240%20DATA_2_20260807084210.CSV">Download CSV</a>
</body></html>"""

SERIES_BODIES = {
    "vwap_region_quarter": VWA_CSV.encode(),
    "neg_price_count": NEG_CSV.encode(),
    "high_price_count_5000": HI_CSV.encode(),
    "fcas_total_cost": FCAS_CSV.encode(),
}

OUR_ROWS = [
    {"region": "NSW1", "quarter": "2026Q2", "avg_vwap_low": 31.2,
     "avg_vwap_high": 118.92, "neg_price_share": 0.03, "days_covered": 91},
    {"region": "QLD1", "quarter": "2026Q2", "avg_vwap_low": 17.72,
     "avg_vwap_high": 114.29, "neg_price_share": 0.07, "days_covered": 91},
    {"region": "SA1", "quarter": "2026Q2", "avg_vwap_low": 20.32,
     "avg_vwap_high": 272.8, "neg_price_share": 0.21, "days_covered": 91},
    {"region": "VIC1", "quarter": "2026Q2", "avg_vwap_low": 13.0,
     "avg_vwap_high": 109.5, "neg_price_share": 0.21, "days_covered": 91},
    {"region": "TAS1", "quarter": "2026Q2", "avg_vwap_low": 65.63,
     "avg_vwap_high": 118.29, "neg_price_share": 0.0, "days_covered": 91},
    # Quarter the AER edition does not cover yet (2026 Q3 blank in the VWA CSV).
    {"region": "NSW1", "quarter": "2026Q3", "avg_vwap_low": 39.06,
     "avg_vwap_high": 127.61, "neg_price_share": 0.01, "days_covered": 32},
]


def _fetcher(*, csv_bodies=None, page_body=PAGE_HTML, seed_only=True):
    """Injected HTTP boundary: chart pages wall/link, static CSVs serve bytes.

    CSV lookups key off the URL's distinctive tokens (the page-resolved hrefs and
    the seed URLs share them), so both routes serve the same canned bytes.
    """
    bodies = {**SERIES_BODIES, **(csv_bodies or {})}

    def which(url: str) -> str:
        text = url.lower()
        if "vwa" in text:
            return "vwap_region_quarter"
        if "below" in text:
            return "neg_price_count"
        if "above" in text or "5000" in text:
            return "high_price_count_5000"
        if "fcas" in text:
            return "fcas_total_cost"
        raise AssertionError(f"unexpected CSV url {url}")

    def fake_fetch(url: str):
        if url.lower().endswith(".csv"):
            body = bodies[which(url)]
            return body, {"status": 200, "content_type": "application/octet-stream",
                          "bytes": len(body)}
        return page_body, {"status": 200, "content_type": "text/html"}

    return fake_fetch


def _docs_dir(tmp_path: Path) -> Path:
    docs = tmp_path / "docs" / "data"
    docs.mkdir(parents=True, exist_ok=True)
    (docs / "market_quarterly.json").write_text(json.dumps({"rows": OUR_ROWS}))
    return docs


# ─── Quarter helpers ───────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("2026 Q2", "2026Q2"), ("2026Q2", "2026Q2"), ("2026 q1", "2026Q1"),
    ("2026 Q3 ", "2026Q3"), (None, None), ("", None), ("not a quarter", None),
])
def test_quarter_label_normalisation(raw, expected):
    assert quarter_label(raw) == expected


def test_quarter_end_and_days():
    assert quarter_end("2026Q1") == date(2026, 3, 31)
    assert quarter_end("2026Q2") == date(2026, 6, 30)
    assert quarter_end("2026Q3") == date(2026, 9, 30)
    assert quarter_end("2026Q4") == date(2026, 12, 31)
    assert quarter_days("2026Q2") == 91
    assert quarter_days("2024Q1") == 91  # leap February
    assert quarter_days("2026Q3") == 92


def test_expected_reference_quarter_respects_publish_lag():
    # Q2 ends 30 Jun; +8 weeks = 25 Aug.
    assert expected_reference_quarter(date(2026, 8, 24)) == "2026Q1"
    assert expected_reference_quarter(date(2026, 8, 25)) == "2026Q2"
    assert expected_reference_quarter(date(2026, 9, 19)) == "2026Q2"
    # Q3 ends 30 Sep; +8 weeks = 25 Nov.
    assert expected_reference_quarter(date(2026, 11, 25)) == "2026Q3"


# ─── CSV parsing ───────────────────────────────────────────────────────────

def test_region_label_mapping():
    assert region_code_from_label("New South Wales ($ per megawatt hour)") == "NSW1"
    assert region_code_from_label("Queensland (Number of trading intervals)") == "QLD1"
    assert region_code_from_label("South Australia") == "SA1"
    assert region_code_from_label("Total FCAS Costs ($m)") is None


def test_parse_vwap_csv_maps_regions_and_drops_blanks():
    frame = parse_aer_csv(VWA_CSV, "vwap_region_quarter")
    assert set(frame.columns) == {"series", "quarter", "region", "value"}
    assert frame["quarter"].nunique() == 2  # the blank 2026 Q3 row contributes nothing
    nsw = frame[(frame.quarter == "2026Q2") & (frame.region == "NSW1")].value.iloc[0]
    assert nsw == 78.0
    assert region_code_from_label("Tasmania ($ per megawatt hour)") == "TAS1"
    assert frame[(frame.quarter == "2026Q1") & (frame.region == "SA1")].value.iloc[0] == 144.0
    assert latest_quarter(frame) == "2026Q2"


def test_parse_single_region_series_uses_none_region():
    frame = parse_aer_csv(FCAS_CSV, "fcas_total_cost")
    assert frame["region"].isna().all()
    assert frame[frame.quarter == "2026Q2"].value.iloc[0] == pytest.approx(9.158)


def test_parse_count_csv_keeps_zero_counts():
    frame = parse_aer_csv(HI_CSV, "high_price_count_5000")
    assert len(frame) == 2
    assert frame.value.tolist() == [0.0, 0.0]  # a published 0 is a fact, not missing


def test_parse_rejects_csv_without_quarter_header():
    with pytest.raises(AerQaError):
        parse_aer_csv("foo,bar\n1,2\n", "vwap_region_quarter")


# ─── Route resolution ──────────────────────────────────────────────────────

def test_bot_wall_detection():
    assert is_bot_wall(BOT_WALL, "text/html")
    assert is_bot_wall(b"<html><body>hi</body></html>", "text/html")
    assert not is_bot_wall(VWA_CSV.encode(), "application/octet-stream")


def test_hrefs_and_token_match():
    hrefs = hrefs_in_html(PAGE_HTML.decode())
    assert len(hrefs) == 2
    assert match_href(hrefs, ("vwa spot prices", "data")).endswith("20260807084204.CSV")
    assert match_href(hrefs, ("below", "$0", "data")).endswith("20260807084210.CSV")
    assert match_href(hrefs, ("above", "$5000")) is None


def test_resolve_uses_page_href_when_not_walled():
    resolved = aq.resolve_csv_url("vwap_region_quarter", fetch=_fetcher())
    assert resolved["route"] == aq.ROUTE_PAGE
    assert resolved["url"].endswith("20260807084204.CSV")
    assert resolved["note"] is None


def test_resolve_falls_back_to_seed_when_page_is_walled():
    fetcher = _fetcher(page_body=BOT_WALL)
    resolved = aq.resolve_csv_url("vwap_region_quarter", fetch=fetcher)
    assert resolved["route"] == aq.ROUTE_SEED
    assert resolved["url"] == aq.config.AER_QA_SEED_URLS["vwap_region_quarter"]
    assert "bot-management" in resolved["note"]


def test_resolve_falls_back_to_seed_when_page_fetch_raises():
    def boom(url):
        raise AerQaError("HTTP 403 (block)")

    resolved = aq.resolve_csv_url("neg_price_count", fetch=boom)
    assert resolved["route"] == aq.ROUTE_SEED
    assert "403" in resolved["note"]


# ─── Comparators ───────────────────────────────────────────────────────────

def test_band_contains_pass_warn_and_slack():
    ok = compare_band_contains(78, 31.2, 118.92, tolerance_ratio=0.15)
    assert ok["outcome"] == OUTCOME_PASS
    warn = compare_band_contains(500, 31.2, 118.92, tolerance_ratio=0.15)
    assert warn["outcome"] == OUTCOME_WARN and "OUTSIDE" in warn["detail"]
    # 120 is above the band but inside the 15% slack (band width 87.7 → 13.2).
    assert compare_band_contains(120, 31.2, 118.92, tolerance_ratio=0.15)["outcome"] == OUTCOME_PASS
    assert compare_band_contains(120, 31.2, 118.92, tolerance_ratio=0.0)["outcome"] == OUTCOME_WARN


def test_band_contains_skips_missing_values():
    assert compare_band_contains(None, 1, 2, tolerance_ratio=0.1)["outcome"] == "skip"
    assert compare_band_contains(1, None, None, tolerance_ratio=0.1)["outcome"] == "skip"


def test_share_ratio_band_and_noise_floor():
    periods = quarter_days("2026Q2") * 48  # 4368 trading intervals
    ok = compare_share_ratio(350, periods, 0.07, ratio_min=0.5, ratio_max=2.0, noise_floor=50)
    assert ok["outcome"] == OUTCOME_PASS
    warn = compare_share_ratio(847, periods, 0.02, ratio_min=0.5, ratio_max=2.0, noise_floor=50)
    assert warn["outcome"] == OUTCOME_WARN and "OUTSIDE" in warn["detail"]
    floor = compare_share_ratio(8, periods, 0.0, ratio_min=0.5, ratio_max=2.0, noise_floor=50)
    assert floor["outcome"] == OUTCOME_BELOW_FLOOR
    # A real count on the AER side with zero on ours is a gap, not "inconclusive".
    gap = compare_share_ratio(200, periods, 0.0, ratio_min=0.5, ratio_max=2.0, noise_floor=50)
    assert gap["outcome"] == OUTCOME_WARN and "missing negative-price intervals" in gap["detail"]


# ─── Cross-check engine ────────────────────────────────────────────────────

def _reference_frame():
    frames = [parse_aer_csv(body.decode(), key) for key, body in SERIES_BODIES.items()]
    return pd.concat(frames, ignore_index=True)


def test_build_checks_counts_and_reference_only_series():
    our_rows = {(r["region"], r["quarter"]): r for r in OUR_ROWS}
    checks = build_checks(_reference_frame(), our_rows, reference_quarter="2026Q2")
    vwap = checks["vwap_region_quarter"]
    assert vwap["counts"][OUTCOME_PASS] == 5
    assert vwap["counts"][OUTCOME_AWAITING_EDITION] == 1  # 2026Q3 not in the edition
    neg = checks["neg_price_count"]
    assert neg["counts"][OUTCOME_PASS] == 4       # NSW/QLD/SA/VIC within band
    assert neg["counts"][OUTCOME_BELOW_FLOOR] == 1  # TAS: 8 intervals, ours 0
    for key in ("high_price_count_5000", "fcas_total_cost"):
        assert checks[key]["counts"][OUTCOME_NOT_COMPARABLE] == 1
        assert checks[key]["checks"][0]["detail"]  # reason published, never silent


def test_build_checks_marks_a_divergence_as_warn():
    our_rows = {(r["region"], r["quarter"]): dict(r) for r in OUR_ROWS}
    our_rows[("NSW1", "2026Q2")]["neg_price_share"] = 0.60  # 22x the AER share
    checks = build_checks(_reference_frame(), our_rows, reference_quarter="2026Q2")
    warns = [c for c in checks["neg_price_count"]["checks"] if c["outcome"] == OUTCOME_WARN]
    assert len(warns) == 1 and warns[0]["region"] == "NSW1"


def test_build_checks_empty_reference_skips_cleanly():
    our_rows = {(r["region"], r["quarter"]): r for r in OUR_ROWS}
    empty = pd.DataFrame(columns=list(aq.REFERENCE_COLUMNS))
    checks = build_checks(empty, our_rows, reference_quarter="2026Q2")
    # No reference values: comparable series report a skip, never a fabricated pass.
    assert checks["vwap_region_quarter"]["counts"][OUTCOME_PASS] == 0
    assert checks["vwap_region_quarter"]["checks"][-1]["outcome"] in {
        "skip", OUTCOME_AWAITING_EDITION,
    }


# ─── Lane ──────────────────────────────────────────────────────────────────

def test_run_lane_is_green_and_publishes_qa_artifact_without_ui(tmp_path):
    docs = _docs_dir(tmp_path)
    data = tmp_path / "data"
    result = run_aer_qa_lane(data, docs, today=date(2026, 9, 19), fetch=_fetcher())

    assert result.status == STATUS_OK
    assert result.fetched is True
    assert result.edition_quarter == "2026Q2"
    assert result.reference_quarter == "2026Q2"
    payload = json.loads((docs / ARTIFACT_FILENAME).read_text())
    assert payload["qa_only"] is True
    assert payload["summary"]["warned"] == 0
    assert payload["summary"]["not_comparable"] == 2
    assert payload["edition"]["routes"]["vwap_region_quarter"] == aq.ROUTE_PAGE
    assert payload["reference_values"]["vwap_region_quarter"]["2026Q2"]["NSW1"] == 78.0
    # QA only: nothing else is written into docs/ (no chart, no panel, no index).
    assert sorted(p.name for p in docs.iterdir()) == ["aer_qa.json", "market_quarterly.json"]
    # Cache is machine-local and holds the parsed reference values.
    cache, meta = load_cache(data)
    assert len(cache) == len(_reference_frame())
    assert meta["source_urls"]["fcas_total_cost"] == aq.config.AER_QA_SEED_URLS["fcas_total_cost"]


def test_run_lane_warn_does_not_fail_the_run(tmp_path):
    docs = _docs_dir(tmp_path)
    rows = [dict(r) for r in OUR_ROWS]
    rows[0]["neg_price_share"] = 0.60
    (docs / "market_quarterly.json").write_text(json.dumps({"rows": rows}))
    result = run_aer_qa_lane(tmp_path / "data", docs, today=date(2026, 9, 19), fetch=_fetcher())

    assert result.status == STATUS_OK           # a QA warn is a finding, not a failure
    assert result.warnings and "OUTSIDE" in result.warnings[0]
    payload = json.loads((docs / ARTIFACT_FILENAME).read_text())
    assert payload["summary"]["warned"] == 1
    assert len(payload["findings"]) == 1


def test_run_lane_unchanged_edition_reuses_cache(tmp_path):
    docs = _docs_dir(tmp_path)
    data = tmp_path / "data"
    first = run_aer_qa_lane(data, docs, today=date(2026, 9, 19), fetch=_fetcher())
    assert first.fetched is True
    artifact = docs / ARTIFACT_FILENAME
    before = artifact.read_text()

    second = run_aer_qa_lane(data, docs, today=date(2026, 9, 19), fetch=_fetcher())
    assert second.fetched is True
    assert "edition unchanged" in (second.note or "")
    assert second.status == STATUS_OK
    # Semantic-diff publish gate: identical facts do not bump the artifact.
    assert artifact.read_text() == before


def test_run_lane_uses_cache_when_every_series_fails(tmp_path):
    docs = _docs_dir(tmp_path)
    data = tmp_path / "data"
    run_aer_qa_lane(data, docs, today=date(2026, 9, 19), fetch=_fetcher())

    def dead(url):
        raise AerQaError("HTTP 503")

    result = run_aer_qa_lane(data, docs, today=date(2026, 9, 19), fetch=dead)
    assert result.status == STATUS_DEGRADED
    assert result.fetched is False
    assert result.retained is True
    assert result.edition_quarter == "2026Q2"     # last-known-good edition still checked
    assert "last-known-good" in (result.note or "")


def test_run_lane_errors_when_nothing_is_available(tmp_path):
    docs = _docs_dir(tmp_path)

    def dead(url):
        raise AerQaError("HTTP 503")

    result = run_aer_qa_lane(tmp_path / "data", docs, today=date(2026, 9, 19), fetch=dead)
    assert result.status == STATUS_ERROR
    assert result.error
    payload = json.loads((docs / ARTIFACT_FILENAME).read_text())
    assert payload["summary"]["checks"] == 4  # four series, all reporting no reference data


def test_run_lane_records_awaiting_edition_when_seed_is_stale(tmp_path):
    """A stale seed (edition through 2026Q1) is reported, never compared blindly."""
    docs = _docs_dir(tmp_path)
    stale = {
        "vwap_region_quarter": VWA_CSV.replace("2026 Q2,69,78,60,95,87\r\n", "").encode(),
        "neg_price_count": NEG_CSV.replace("2026 Q2,350,118,913,847,8\r\n", "").encode(),
        "high_price_count_5000": HI_CSV.replace("2026 Q2,0,0\r\n", "").encode(),
        "fcas_total_cost": FCAS_CSV.replace("2026 Q2,9.158\r\n", "").encode(),
    }
    result = run_aer_qa_lane(
        tmp_path / "data", docs, today=date(2026, 9, 19),
        fetch=_fetcher(csv_bodies=stale),
    )
    assert result.edition_quarter == "2026Q1"
    payload = json.loads((docs / ARTIFACT_FILENAME).read_text())
    assert payload["edition"]["awaiting_edition"] is True
    vwap = payload["series"]["vwap_region_quarter"]
    assert vwap["counts"][OUTCOME_AWAITING_EDITION] >= 1
    assert vwap["counts"][OUTCOME_WARN] == 0      # a lag is not a data-quality warn


def test_partial_series_failure_still_checks_the_rest(tmp_path):
    docs = _docs_dir(tmp_path)

    def flaky(url):
        if "FCAS" in url:
            raise AerQaError("HTTP 500")
        return _fetcher()(url)

    result = run_aer_qa_lane(tmp_path / "data", docs, today=date(2026, 9, 19), fetch=flaky)
    assert result.status == STATUS_OK
    assert "fcas_total_cost" not in result.payload["edition"]["source_urls"]
    assert result.payload["summary"]["passed"] > 0


def test_fetch_edition_reports_bot_walled_csv_as_a_problem(tmp_path):
    def walled_csv(url):
        if url.endswith(".CSV") or url.endswith(".csv"):
            return BOT_WALL, {"status": 200, "content_type": "text/html"}
        return BOT_WALL, {"status": 200, "content_type": "text/html"}

    frame, meta, problems = fetch_edition(fetch=walled_csv)
    assert frame.empty
    assert len(problems) == len(aq.config.AER_QA_SERIES)
    assert meta["fetched"] is False


def test_edition_hash_is_recorded_for_change_detection(tmp_path):
    docs = _docs_dir(tmp_path)
    data = tmp_path / "data"
    result = run_aer_qa_lane(data, docs, today=date(2026, 9, 19), fetch=_fetcher())
    hashes = result.payload["edition"]["content_sha256"]
    assert hashes["vwap_region_quarter"] == hashlib.sha256(VWA_CSV.encode()).hexdigest()
    _, meta = load_cache(data)
    assert meta["content_sha256"] == hashes
    assert Path(data / "aer_qa" / "aer_qa_reference.feather").exists()


def test_reference_payload_is_bounded(tmp_path):
    reference = _reference_frame()
    payload = aq._reference_payload(reference, max_quarters=1)
    assert set(payload["vwap_region_quarter"]) == {"2026Q2"}

    our_rows = {(r["region"], r["quarter"]): r for r in OUR_ROWS}
    checks = build_checks(reference, our_rows, reference_quarter="2026Q2")
    full = aq.build_aer_qa_payload(checks, {}, reference, reference_quarter="2026Q2")
    assert full["summary"]["checks"] == sum(
        entry["counts"][outcome]
        for entry in checks.values() for outcome in entry["counts"]
    )
