"""One lifecycle contract for the optional lanes (network outages, generator information, AER QA).

Each lane degrades and retains its last-known-good output when the source fails, and errors on a cold start
with nothing to retain. The three lanes used to repeat that as hand-written tests in their own files; here it is
one parametrised contract, and each lane's own assertions are kept as its `extra_*` checks. Lane-specific
behaviour (parse failures, unchanged-source skips, semantic gates) stays in the lane's own test file.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

import test_aer_qa as aer_t
import test_geninfo as gen_t
import test_network_outages as net_t
from test_network_outages import context_files  # noqa: F401  (fixture used by the network-outages lane)
from src import aer_qa, geninfo, network_outages as no
from src.run_status import STATUS_DEGRADED, STATUS_ERROR

TODAY = date(2026, 9, 19)


class NetworkOutages:
    def install(self, monkeypatch, tmp_path, context_files):
        net_t._install_fake_source(monkeypatch, tmp_path, context_files=context_files)

    def run(self, tmp_path, day):
        return net_t.run_network_outages_lane(
            tmp_path / "data", tmp_path / "docs" / "data", months_back=1, today=TODAY)

    def break_source(self, monkeypatch):
        monkeypatch.setattr(no, "probe_month", lambda y, m: None)

    def published(self, tmp_path):
        return (tmp_path / "docs" / "data" / no.ARTIFACT_FILENAME).read_text()

    def check_degraded(self, result, tmp_path, published_before):
        assert result.error
        assert result.month == "2026-08"
        # Last-known-good artifact untouched.
        assert self.published(tmp_path) == published_before

    def check_cold(self, result, tmp_path):
        assert result.retained is False


class GenInfo:
    def install(self, monkeypatch, tmp_path, context_files):
        gen_t._install_fake_source(monkeypatch, tmp_path)

    def run(self, tmp_path, day):
        return gen_t.run_geninfo_lane(
            tmp_path / "data", tmp_path / "docs" / "data", today=date(2026, 9, 19 + day))

    def break_source(self, monkeypatch):
        monkeypatch.setattr(geninfo, "discover_edition", lambda today=None: None)

    def published(self, tmp_path):
        return None

    def check_degraded(self, result, tmp_path, published_before):
        assert len(result.snapshot) == 2  # last-known-good kept for the attach layer

    def check_cold(self, result, tmp_path):
        assert result.snapshot.empty


class AerQa:
    def __init__(self):
        self.fetch = aer_t._fetcher()

    def install(self, monkeypatch, tmp_path, context_files):
        self.fetch = aer_t._fetcher()

    def run(self, tmp_path, day):
        return aer_t.run_aer_qa_lane(
            tmp_path / "data", aer_t._docs_dir(tmp_path), today=TODAY, fetch=self.fetch)

    def break_source(self, monkeypatch):
        def dead(url):
            raise aer_t.AerQaError("HTTP 503")
        self.fetch = dead

    def published(self, tmp_path):
        return None

    def check_degraded(self, result, tmp_path, published_before):
        assert result.fetched is False
        assert result.edition_quarter == "2026Q2"     # last-known-good edition still checked
        assert "last-known-good" in (result.note or "")

    def check_cold(self, result, tmp_path):
        assert result.error
        payload = json.loads((aer_t._docs_dir(tmp_path) / aer_qa.ARTIFACT_FILENAME).read_text())
        assert payload["summary"]["checks"] == 4  # four series, all reporting no reference data


@pytest.fixture(params=[NetworkOutages, GenInfo, AerQa], ids=lambda c: c.__name__)
def lane(request):
    return request.param()


def test_source_failure_after_a_good_run_degrades_and_retains(lane, tmp_path, monkeypatch, context_files):
    lane.install(monkeypatch, tmp_path, context_files)
    lane.run(tmp_path, 0)
    published = lane.published(tmp_path)

    lane.break_source(monkeypatch)
    result = lane.run(tmp_path, 1)

    assert result.status == STATUS_DEGRADED
    assert result.retained is True
    lane.check_degraded(result, tmp_path, published)


def test_source_failure_on_a_cold_start_is_an_error(lane, tmp_path, monkeypatch, context_files):
    lane.break_source(monkeypatch)
    result = lane.run(tmp_path, 1)

    assert result.status == STATUS_ERROR
    lane.check_cold(result, tmp_path)
