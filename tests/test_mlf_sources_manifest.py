"""MLF inputs are run_status sources; a cache fallback is degraded (audit 2026-10-07, S2-6).

A failed MLF Tracker download used the stale summary.csv with only a log
warning, and a failed DUDETAILSUMMARY download used the old cache (or let
revenue fall back to the per-FY factor). Neither was in run_status.json, so
revenue could be valued on stale loss factors with every check green.
"""

from datetime import datetime

import requests

from src import fetch_mlf as fm
from src import loss_factors as lf
from src import main as pipeline
from src.run_status import STATUS_DEGRADED, STATUS_OK, build_manifest
from tests.test_loss_factors import QPSFB1_ROWS, FakeResponse, _archive

SUMMARY = "DUID,CONNECTIONPOINTID,FY25-26,FY26-27,FY27-28 (Draft)\nGEN1,CP1,0.98,0.97,0.96\n"


class _Resp:
    status_code = 200
    text = SUMMARY
    content = SUMMARY.encode()
    headers = {"Last-Modified": "Tue, 06 Oct 2026 01:43:00 GMT"}

    def raise_for_status(self):
        pass


class TestMlfTracker:
    def test_download_reports_newest_fy_and_last_modified(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fm.requests, "get", lambda *a, **k: _Resp())
        report = {}
        fm.fetch_mlf_data(str(tmp_path), force=True, report=report)
        assert report["source"] == "download"
        assert report["newest_fy"] == "FY26-27" and report["draft_fy"] == "FY27-28"
        lane = pipeline._mlf_tracker_lane(report)
        rec = lane.manifest_record()
        assert lane.status == STATUS_OK and rec["asof_month"] == "FY26-27"
        assert rec["details"]["last_modified"] == "Tue, 06 Oct 2026 01:43:00 GMT"

    def test_failed_download_with_stale_cache_is_degraded(self, tmp_path, monkeypatch):
        (tmp_path / "mlf_tracker_summary.csv").write_text(SUMMARY)

        def fail(*a, **k):
            raise requests.ConnectionError("GitHub DNS failure")
        monkeypatch.setattr(fm.requests, "get", fail)
        monkeypatch.setattr("time.sleep", lambda s: None)
        report = {}
        fm.fetch_mlf_data(str(tmp_path), force=True, report=report)
        assert report["source"] == "stale_cache"
        lane = pipeline._mlf_tracker_lane(report)
        assert lane.status == STATUS_DEGRADED and lane.retained
        assert "GitHub DNS failure" in lane.error

    def test_unforced_cache_read_is_not_a_fallback(self, tmp_path):
        (tmp_path / "mlf_tracker_summary.csv").write_text(SUMMARY)
        report = {}
        fm.fetch_mlf_data(str(tmp_path), force=False, report=report)
        lane = pipeline._mlf_tracker_lane(report)
        assert report["source"] == "cache" and lane.status == STATUS_OK


class TestLossFactors:
    def _serve(self, monkeypatch, available, fail=()):
        def fake_get(url, **kw):
            for label in fail:
                if label in url:
                    raise requests.ConnectionError("nemweb timeout")
            for label in available:
                if label in url:
                    return FakeResponse(200, _archive(QPSFB1_ROWS).encode())
            return FakeResponse(404)
        monkeypatch.setattr(lf.requests, "get", fake_get)

    def test_newest_archive_is_ok_with_its_month(self, tmp_path, monkeypatch):
        self._serve(monkeypatch, ["202608010000"])
        report = {}
        lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 5), report=report)
        lane = pipeline._loss_factor_lane(report)
        assert lane.status == STATUS_OK and lane.manifest_record()["asof_month"] == "2026-08"

    def test_recent_cache_with_next_archive_not_out_is_ok(self, tmp_path, monkeypatch):
        self._serve(monkeypatch, ["202608010000"])
        lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 5))
        report = {}
        lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 6), report=report)
        assert report["source"] == "cache"
        assert pipeline._loss_factor_lane(report).status == STATUS_OK

    def test_failed_download_falling_back_to_cache_is_degraded(self, tmp_path, monkeypatch):
        self._serve(monkeypatch, ["202608010000"])
        lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 5))
        self._serve(monkeypatch, [], fail=["202609010000"])
        report = {}
        lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 6), report=report)
        lane = pipeline._loss_factor_lane(report)
        assert lane.status == STATUS_DEGRADED and "nemweb timeout" in lane.error
        assert lane.manifest_record()["asof_month"] == "2026-08"

    def test_old_cache_with_nothing_newer_is_degraded(self, tmp_path, monkeypatch):
        self._serve(monkeypatch, ["202608010000"])
        lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 5))
        self._serve(monkeypatch, [])
        report = {}
        lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2027, 3, 1), report=report)
        lane = pipeline._loss_factor_lane(report)
        assert lane.status == STATUS_DEGRADED and "2026-08" in lane.error

    def test_nothing_available_is_degraded(self, tmp_path, monkeypatch):
        self._serve(monkeypatch, [])
        report = {}
        assert lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 5), report=report) is None
        lane = pipeline._loss_factor_lane(report)
        assert lane.status == STATUS_DEGRADED and "per-FY MLF" in lane.error


def test_both_sources_appear_in_the_manifest():
    lanes = {
        "mlf_tracker": pipeline._mlf_tracker_lane({"source": "download", "newest_fy": "FY26-27"}),
        "loss_factors": pipeline._loss_factor_lane({"source": "download", "archive_month": "2026-08"}),
    }
    sources = build_manifest(lanes)["sources"]
    assert sources["mlf_tracker"]["asof_month"] == "FY26-27"
    assert sources["loss_factors"]["asof_month"] == "2026-08"
