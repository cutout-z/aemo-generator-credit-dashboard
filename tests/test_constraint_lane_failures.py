"""Constraint-lane failures are recorded and fail the constraints run (audit 2026-10-07).

S1-1: the constraints lane's first run (2026-10-06) could not load GENCONDATA /
SPDCONNECTIONPOINTCONSTRAINT, recorded no reason, kept constraint data that
ended 2026-03, and exited 0. S2-4: a DISPATCHCONSTRAINT month that failed read
as an empty month. S2-5: the two reference tables were cached forever.
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd
import pytest
from nemosis.custom_errors import NoDataToReturn

from src import download_constraints as dc
from src import main as pipeline
from src.run_status import (
    STATUS_DEGRADED, STATUS_OK, STATUS_SKIPPED, LaneRun, build_manifest,
)


# Recent effective dates: a table whose newest date is months old is treated
# as an incomplete download (download_constraints._stale_reason).
_RECENT = (pd.Timestamp.now().normalize() - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
_OLDER = (pd.Timestamp.now().normalize() - pd.Timedelta(days=60)).strftime("%Y-%m-%d")


def _gencon_raw():
    return pd.DataFrame({
        "GENCONID": ["X", "X"], "EFFECTIVEDATE": [_OLDER, _RECENT],
        "VERSIONNO": [1, 1], "DESCRIPTION": ["old", "new"], "REASON": ["", ""],
        "LIMITTYPE": ["", ""],
    })


def _spdcp_raw():
    return pd.DataFrame({
        "CONNECTIONPOINTID": ["CP_A"], "EFFECTIVEDATE": [_RECENT], "VERSIONNO": [1],
        "GENCONID": ["X"], "FACTOR": [1.0], "BIDTYPE": ["ENERGY"],
    })


def _compiler(calls, tables=None, fail=None):
    """Fake NEMOSIS: serve canned frames per table, or raise ``fail``."""
    tables = tables or {"GENCONDATA": _gencon_raw, "SPDCONNECTIONPOINTCONSTRAINT": _spdcp_raw}

    def fake(**kw):
        calls.append(kw["table_name"])
        if fail is not None:
            raise fail
        return tables[kw["table_name"]]()
    return fake


def _age(path, days):
    t = time.time() - days * 86400
    os.utime(path, (t, t))


class TestReferenceTables:
    def test_failed_pull_carries_the_exception_text(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(dc, "dynamic_data_compiler",
                            _compiler(calls, fail=ConnectionError("nemweb DNS failure")))
        errors = []
        out = dc.fetch_gencondata(str(tmp_path), errors=errors)
        assert out.empty
        assert errors and "GENCONDATA" in errors[0] and "nemweb DNS failure" in errors[0]

    def test_empty_pull_is_an_error_too(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(dc, "dynamic_data_compiler", _compiler(
            calls, tables={"SPDCONNECTIONPOINTCONSTRAINT": pd.DataFrame}))
        errors = []
        assert dc.fetch_spdconnectionpointconstraint(str(tmp_path), errors=errors).empty
        assert "SPDCONNECTIONPOINTCONSTRAINT" in errors[0] and "no rows" in errors[0]

    def test_fresh_cache_is_not_re_pulled(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(dc, "dynamic_data_compiler", _compiler(calls))
        dc.fetch_gencondata(str(tmp_path), max_age_days=25)
        assert calls == ["GENCONDATA"]
        _age(tmp_path / dc.GENCONDATA_CACHE, 10)
        dc.fetch_gencondata(str(tmp_path), max_age_days=25)
        assert calls == ["GENCONDATA"]

    def test_cache_older_than_25_days_is_re_pulled(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(dc, "dynamic_data_compiler", _compiler(calls))
        dc.fetch_spdconnectionpointconstraint(str(tmp_path), max_age_days=dc.REFERENCE_MAX_AGE_DAYS)
        _age(tmp_path / dc.SPDCP_CACHE, 26)
        dc.fetch_spdconnectionpointconstraint(str(tmp_path), max_age_days=dc.REFERENCE_MAX_AGE_DAYS)
        assert calls == ["SPDCONNECTIONPOINTCONSTRAINT"] * 2
        assert dc.reference_cache_date(tmp_path / dc.SPDCP_CACHE) == datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def test_failed_refresh_keeps_the_stale_cache_and_says_so(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(dc, "dynamic_data_compiler", _compiler(calls))
        first = dc.fetch_gencondata(str(tmp_path))
        _age(tmp_path / dc.GENCONDATA_CACHE, 40)
        monkeypatch.setattr(dc, "dynamic_data_compiler",
                            _compiler(calls, fail=RuntimeError("archive 404")))
        errors = []
        out = dc.fetch_gencondata(str(tmp_path), max_age_days=25, errors=errors)
        assert out.equals(first)
        assert "archive 404" in errors[0] and "using the cache written" in errors[0]

    def test_without_max_age_the_cache_is_reused_as_before(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(dc, "dynamic_data_compiler", _compiler(calls))
        dc.fetch_gencondata(str(tmp_path))
        _age(tmp_path / dc.GENCONDATA_CACHE, 400)
        dc.fetch_gencondata(str(tmp_path))
        assert calls == ["GENCONDATA"]


def test_month_fetch_no_longer_swallows_errors(tmp_path, monkeypatch):
    def boom(**kw):
        raise OSError("disk full")
    monkeypatch.setattr(dc, "dynamic_data_compiler", boom)
    with pytest.raises(OSError):
        dc.fetch_binding_constraints_month(2026, 8, str(tmp_path))


class TestConstraintStep:
    def _setup(self, tmp_path, monkeypatch, month_fn, ref_fail=None):
        calls = []
        monkeypatch.setattr(dc, "dynamic_data_compiler", _compiler(calls, fail=ref_fail))
        monkeypatch.setattr(pipeline, "fetch_binding_constraints_month", month_fn)
        monkeypatch.setattr(pipeline, "_months_to_process",
                            lambda n, full: [(2026, 7), (2026, 8), (2026, 9)])
        monkeypatch.setattr(pipeline, "aggregate_constraints_month",
                            lambda d, s, g, cp, y, m: pd.DataFrame(
                                {"duid": ["A"], "month": [f"{y}-{m:02d}"],
                                 "constraint_id": ["X"], "hours_bound": [1.0]}))

    def _binding(self):
        return pd.DataFrame({"SETTLEMENTDATE": [pd.Timestamp("2026-08-01 00:05")],
                             "CONSTRAINTID": ["X"], "MARGINALVALUE": [5.0]})

    def test_a_month_that_raised_is_in_the_errors(self, tmp_path, monkeypatch):
        def month_fn(y, m, cache, rebuild=False):
            if m == 8:
                raise ValueError("corrupt parquet")
            if m == 9:
                raise NoDataToReturn("not published")
            return self._binding()
        self._setup(tmp_path, monkeypatch, month_fn)
        frame, errors = pipeline._run_constraint_step(
            tmp_path, {}, months_back=2, full_refresh=False, today=datetime(2026, 10, 7))
        assert frame["month"].tolist() == ["2026-07"]
        assert len(errors) == 1 and "2026-08" in errors[0] and "corrupt parquet" in errors[0]

    def test_no_archive_for_a_long_finished_month_is_an_error(self, tmp_path, monkeypatch):
        def month_fn(y, m, cache, rebuild=False):
            raise NoDataToReturn("no files")
        self._setup(tmp_path, monkeypatch, month_fn)
        _, errors = pipeline._run_constraint_step(
            tmp_path, {}, months_back=2, full_refresh=False, today=datetime(2026, 10, 7))
        # Jul and Aug ended > 35 days before 7 Oct; Sep is not due yet.
        assert [e[:7] for e in errors] == ["2026-07", "2026-08"]

    def test_reference_failure_reason_reaches_the_lane_error(self, tmp_path, monkeypatch):
        self._setup(tmp_path, monkeypatch, lambda *a, **k: self._binding(),
                    ref_fail=ConnectionError("Name or service not known"))
        frame, errors = pipeline._run_constraint_step(
            tmp_path, {}, months_back=2, full_refresh=False, today=datetime(2026, 10, 7))
        assert frame.empty
        joined = "; ".join(errors)
        assert "Name or service not known" in joined
        assert "GENCONDATA or SPDCONNECTIONPOINTCONSTRAINT unavailable" in joined
        gen = pd.DataFrame({"duid": ["A"], "month": ["2026-08"]})
        lane = pipeline._constraint_lane(frame, gen, skipped=False, error=joined)
        assert lane.status == STATUS_DEGRADED and "Name or service not known" in lane.error


class TestRunExit:
    def _lane(self, status, error=None):
        lane = LaneRun(source="constraints")
        lane.status, lane.error = status, error
        return lane

    def test_requested_and_not_ok_fails_the_run(self):
        msg = pipeline._constraint_run_failure(
            self._lane(STATUS_DEGRADED, "constraint data ends 2026-03, 5 months behind"),
            skip_constraints=False)
        assert msg and "degraded" in msg and "2026-03" in msg

    def test_ok_constraints_pass(self):
        assert pipeline._constraint_run_failure(self._lane(STATUS_OK), skip_constraints=False) is None

    def test_skipping_lanes_never_fail_on_constraints(self):
        # The daily lane: --skip-constraints, constraint data 5 months behind.
        cons = pd.DataFrame({"duid": ["A"], "month": ["2026-03"]})
        gen = pd.DataFrame({"duid": ["A"], "month": ["2026-08"]})
        lane = pipeline._constraint_lane(cons, gen, skipped=True)
        assert lane.status == STATUS_DEGRADED
        assert pipeline._constraint_run_failure(lane, skip_constraints=True) is None
        assert pipeline._constraint_run_failure(self._lane(STATUS_SKIPPED), skip_constraints=True) is None


def test_manifest_records_the_reference_cache_dates(tmp_path):
    pd.DataFrame({"a": [1]}).to_feather(tmp_path / dc.GENCONDATA_CACHE)
    cons = pd.DataFrame({"duid": ["A"], "month": ["2026-08"]})
    lane = pipeline._constraint_lane(cons, cons, skipped=True, reference_dir=tmp_path)
    rec = build_manifest({"constraints": lane})["sources"]["constraints"]
    assert rec["details"]["gencondata_cache_date"] == dc.reference_cache_date(tmp_path / dc.GENCONDATA_CACHE)
    assert rec["details"]["spdcp_cache_date"] is None
    # Lanes without details keep the old record shape.
    assert "details" not in LaneRun(source="x").manifest_record()
