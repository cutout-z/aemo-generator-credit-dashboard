"""End-to-end smoke of ``src.main.main()`` with the whole fetch layer faked.

Runs the real pipeline entry point in-process for one synthetic month, with
``config.DATA_DIR`` / ``config.DOCS_DATA_DIR`` redirected into ``tmp_path`` and
every network path replaced:

* nemosis ``dynamic_data_compiler`` (SCADA / DISPATCHPRICE / DISPATCHLOAD) is
  patched in the download modules, so the real fetch wrappers, aggregation,
  daily/FCAS caches, market-factor build, freshness guards, JSON writers and
  run-status manifest all execute;
* the Registration List, MLF Tracker and DUDETAILSUMMARY downloads, which have
  no nemosis seam, are replaced at the ``src.main`` names;
* a socket guard fails the test on any real connection attempt.

NOT covered (flagged off or out of reach without editing src/): the
constraint lane, the BIDPEROFFER/BIDDAYOFFER FCAS / offer-factor / offer-curve
lanes, the geninfo / network-outage / AER QA lanes (all skipped through their
own ``--skip-*`` flags), and the processed-cache snapshot
(``--no-processed-cache-snapshot``; covered by test_processed_cache.py).
"""

from __future__ import annotations

import json
import socket
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src import config
from src import download_dispatch, download_scada, main as pipeline

SKIP_FLAGS = [
    "--skip-constraints", "--skip-offer-factors", "--skip-fcas-factors",
    "--skip-geninfo", "--skip-network-outages", "--skip-aer-qa",
    "--no-processed-cache-snapshot", "--months-back", "0",
]
REGIONS = ["NSW1", "QLD1"]
DUIDS = {"SOLAR1": ("NSW1", "Solar", 100.0), "WIND1": ("QLD1", "Wind", 80.0)}


def _month() -> tuple[int, int]:
    months = pipeline._months_to_process(0, False)
    assert len(months) == 1
    return months[0]


def _intervals(year: int, month: int) -> pd.DatetimeIndex:
    start = pd.Timestamp(year=year, month=month, day=1)
    return pd.date_range(
        start + pd.Timedelta(minutes=5), start + pd.offsets.MonthBegin(1), freq="5min",
    )


def _fake_compiler(**kw):
    table = kw["table_name"]
    year, month = _month()
    ts = _intervals(year, month)
    rng = np.random.default_rng(7)
    if table == "DISPATCH_UNIT_SCADA":
        return pd.DataFrame([
            {"SETTLEMENTDATE": t, "DUID": d, "SCADAVALUE": float(10 + (i % 50))}
            for d in DUIDS for i, t in enumerate(ts)
        ])
    if table == "DISPATCHPRICE":
        rows = []
        for r in REGIONS:
            rrp = 50 + 40 * np.sin(np.arange(len(ts)) / 40.0) + rng.normal(0, 5, len(ts))
            df = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": r, "RRP": rrp,
                               "INTERVENTION": 0})
            for c in ("RAISE6SEC", "RAISE60SEC", "RAISE5MIN", "RAISEREG",
                      "LOWER6SEC", "LOWER60SEC", "LOWER5MIN", "LOWERREG"):
                df[f"{c}RRP"] = 1.5
            rows.append(df)
        return pd.concat(rows, ignore_index=True)
    if table == "DISPATCHLOAD":
        return pd.DataFrame([
            {"SETTLEMENTDATE": t, "DUID": d, "AVAILABILITY": 90.0,
             "INTERVENTION": 0, "UIGF": float("nan")}
            for d in DUIDS for t in ts
        ])
    raise AssertionError(f"unexpected nemosis table in offline e2e: {table}")


def _generators() -> pd.DataFrame:
    return pd.DataFrame([
        {"DUID": d, "STATION_NAME": f"{d} Station", "REGION": reg,
         "FUEL_CATEGORY": fuel, "CAPACITY_MW": cap, "TECHNOLOGY": fuel,
         "FUEL_SOURCE": fuel, "DISPATCH_TYPE": "Generator", "MARKET": "NEM",
         "CONNECTION_POINT": ""}
        for d, (reg, fuel, cap) in DUIDS.items()
    ])


@pytest.fixture
def pipeline_run(tmp_path, monkeypatch):
    data_dir, docs_dir = tmp_path / "data", tmp_path / "docs" / "data"
    # Re-root EVERY repo-relative path constant (DATA_DIR, DOCS_DATA_DIR,
    # GENERATORS_JSON_DIR, INDEX_JSON, caches...) into tmp_path, so the run can
    # never write to the real data/ or docs/data/.
    for name, val in list(vars(config).items()):
        if isinstance(val, str) and val.startswith(("data/", "docs/")) or val in ("data", "docs"):
            monkeypatch.setattr(config, name, str(tmp_path / val))
    assert Path(config.DATA_DIR) == data_dir and Path(config.DOCS_DATA_DIR) == docs_dir
    # build_market_factors lists DISPATCHPRICE months from the raw cache names.
    year, month = _month()
    cache = data_dir / "nemosis_cache"
    cache.mkdir(parents=True)
    (cache / f"PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#{year}{month:02d}01000000.parquet").touch()

    for mod in (download_scada, download_dispatch):
        monkeypatch.setattr(mod, "dynamic_data_compiler", _fake_compiler)
    monkeypatch.setattr(pipeline, "fetch_generators", lambda *a, **k: _generators())
    fy = year if month >= 7 else year - 1
    mlf = pd.DataFrame({
        "DUID": list(DUIDS), "fy_label": [f"FY{fy % 100}-{(fy + 1) % 100}"] * 2,
        "fy_start_year": [fy] * 2, "mlf": [0.98, 1.01],
    })
    monkeypatch.setattr(
        pipeline, "fetch_mlf_data",
        lambda *a, report=None, **k: (mlf, None, None, {d: f"CP_{d}" for d in DUIDS}),
    )
    monkeypatch.setattr(pipeline, "fetch_loss_factor_periods", lambda *a, **k: None)

    def no_network(*a, **k):
        raise AssertionError("network connection attempted in offline e2e")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr("sys.argv", ["src.main", *SKIP_FLAGS])
    pipeline.main()
    return data_dir, docs_dir, f"{year}-{month:02d}"


def test_main_runs_offline_and_writes_expected_files(pipeline_run):
    data_dir, docs_dir, _ = pipeline_run
    for f in ("monthly_aggregates.feather", "daily_aggregates.feather",
              "fcas_aggregates.feather", "market_factors_daily.feather"):
        assert (data_dir / f).stat().st_size > 0, f
    for f in ("index.json", "market_daily.json", "run_status.json"):
        assert (docs_dir / f).stat().st_size > 0, f
    assert {p.stem for p in (docs_dir / "generators").glob("*.json")} == set(DUIDS)


def test_monthly_aggregates_schema_and_values(pipeline_run):
    data_dir, _, label = pipeline_run
    m = pd.read_feather(data_dir / "monthly_aggregates.feather")
    assert set(m["duid"]) == set(DUIDS) and set(m["month"]) == {label}
    key = ["generation_mwh", "revenue_aud", "capacity_factor", "avg_rrp", "captured_price"]
    assert not m[key].isna().any().any()
    assert (m["generation_mwh"] > 0).all()
    # Independent oracle: the fake SCADA is 10 + (i % 50) MW per 5-min interval.
    n = len(_intervals(*_month()))
    expected = sum(10 + (i % 50) for i in range(n)) / 12.0
    assert m.set_index("duid")["generation_mwh"].to_dict() == {
        d: pytest.approx(expected, rel=1e-3) for d in DUIDS}
    assert m["capacity_factor"].between(0, 1.1).all()
    d = pd.read_feather(data_dir / "daily_aggregates.feather")
    assert set(d["duid"]) == set(DUIDS) and len(d) >= 28 * len(DUIDS)
    assert not d["daily_capacity_factor"].isna().any()


def test_generator_json_matches_feather(pipeline_run):
    data_dir, docs_dir, label = pipeline_run
    m = pd.read_feather(data_dir / "monthly_aggregates.feather").set_index("duid")
    index = json.loads((docs_dir / "index.json").read_text())
    assert index, "index.json is empty"
    for duid, (region, fuel, cap) in DUIDS.items():
        doc = json.loads((docs_dir / "generators" / f"{duid}.json").read_text())
        assert {"duid", "station_name", "region", "fuel_category",
                "capacity_mw", "monthly"} <= set(doc)
        assert doc["duid"] == duid and doc["region"] == region
        assert doc["capacity_mw"] == cap
        mon = doc["monthly"]
        assert mon["months"] == [label]
        assert mon["generation_mwh"] == [pytest.approx(m.loc[duid, "generation_mwh"], abs=1)]
        assert mon["revenue_aud"] == [pytest.approx(m.loc[duid, "revenue_aud"], abs=1)]
        assert mon["capacity_factor"][0] is not None


def test_market_and_manifest_outputs(pipeline_run):
    _, docs_dir, _ = pipeline_run
    market = json.loads((docs_dir / "market_daily.json").read_text())
    regions = market["regions"]
    assert set(REGIONS) <= set(regions)
    for r in REGIONS:
        e = regions[r]
        assert e["dates"] and len(e["dates"]) == len(e["spread_decile"])
        assert all(v is not None for v in e["spread_decile"])
    manifest = json.loads((docs_dir / "run_status.json").read_text())
    from src.run_status import validate_manifest
    validate_manifest(manifest)
    assert manifest["outputs"]["generator_files"] == len(DUIDS)
    assert manifest["guards"]["monthly_freshness"] == "pass"
