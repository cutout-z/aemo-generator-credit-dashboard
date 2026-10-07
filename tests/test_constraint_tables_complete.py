"""A NEMOSIS compile that lost its later monthly files is not cached or used.

On 2026-10-06 nemweb throttled the constraints lane part-way through, NEMOSIS
compiled what it had, and the NAS cached a SPDCONNECTIONPOINTCONSTRAINT table
ending 2018-11-30: March 2026 then mapped 64 of its 628 binding constraints.
"""

import pandas as pd

from src import download_constraints as dc


def _spdcp(newest):
    return pd.DataFrame({
        "CONNECTIONPOINTID": ["NCP1", "NCP2"], "EFFECTIVEDATE": ["2018-01-01", newest],
        "VERSIONNO": [1, 1], "GENCONID": ["C1", "C2"], "FACTOR": [1.0, 1.0], "BIDTYPE": ["ENERGY", "ENERGY"],
    })


def _gencon(newest):
    return pd.DataFrame({"GENCONID": ["C1", "C2"], "EFFECTIVEDATE": ["2018-01-01", newest],
                         "VERSIONNO": [1, 1], "DESCRIPTION": ["a", "b"], "REASON": ["", ""], "LIMITTYPE": ["", ""]})


def test_stale_reason_flags_a_table_that_stops_months_before_today():
    today = pd.Timestamp("2026-10-07")
    assert dc._stale_reason(_spdcp("2018-11-30"), "T", today=today)
    assert dc._stale_reason(_spdcp("2026-08-31"), "T", today=today) is None
    assert dc._stale_reason(pd.DataFrame(), "T", today=today)


def test_a_truncated_compile_is_neither_cached_nor_returned(tmp_path, monkeypatch):
    monkeypatch.setattr(dc, "dynamic_data_compiler", lambda **k: _spdcp("2018-11-30"))
    assert dc.fetch_spdconnectionpointconstraint(str(tmp_path)).empty
    assert not (tmp_path / "spdcp_constraint_versions.feather").exists()
    monkeypatch.setattr(dc, "dynamic_data_compiler", lambda **k: _gencon("2018-11-30"))
    assert dc.fetch_gencondata(str(tmp_path)).empty
    assert not (tmp_path / "gencondata.feather").exists()


def test_a_truncated_cache_is_fetched_again(tmp_path, monkeypatch):
    stale = _spdcp("2018-11-30"); stale["EFFECTIVEDATE"] = pd.to_datetime(stale["EFFECTIVEDATE"])
    stale.to_feather(tmp_path / "spdcp_constraint_versions.feather")
    recent = pd.Timestamp.now().normalize() - pd.Timedelta(days=30)
    calls = []
    monkeypatch.setattr(dc, "dynamic_data_compiler", lambda **k: calls.append(k) or _spdcp(str(recent.date())))
    out = dc.fetch_spdconnectionpointconstraint(str(tmp_path))
    assert calls and pd.to_datetime(out["EFFECTIVEDATE"]).max() == recent
    assert pd.read_feather(tmp_path / "spdcp_constraint_versions.feather")["EFFECTIVEDATE"].max() == recent


def test_a_complete_cache_is_used_without_fetching(tmp_path, monkeypatch):
    recent = pd.Timestamp.now().normalize() - pd.Timedelta(days=30)
    fresh = _gencon(str(recent.date())); fresh["EFFECTIVEDATE"] = pd.to_datetime(fresh["EFFECTIVEDATE"])
    fresh.to_feather(tmp_path / "gencondata.feather")
    monkeypatch.setattr(dc, "dynamic_data_compiler", lambda **k: (_ for _ in ()).throw(AssertionError("fetched")))
    assert len(dc.fetch_gencondata(str(tmp_path))) == 2
