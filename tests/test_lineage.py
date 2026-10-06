"""Renamed / converted / merged units keep their history (audit 2026-10, M8).

Aggregation dropped every SCADA DUID that is not in the current Registration
List, so HPR1 (Hornsdale Power Reserve, bidirectional since 12 Sep 2024) had
no history before 2024-09: HPRG1's 2021-09..2024-09 discharge was gone. The
same happened to WKIEWA2 when WKIEWA1 became the aggregated units 1-4.
"""

import json

import pandas as pd

from src.aggregate import aggregate_month, aggregate_month_daily
from src.generate_json import generate_all
from src.lineage import (
    merge_lineage_daily,
    merge_lineage_monthly,
    successors,
    with_predecessor_units,
)


def _generators():
    return pd.DataFrame({
        "DUID": ["HPR1", "BW01"],
        "STATION_NAME": ["Hornsdale Power Reserve", "Bayswater"],
        "REGION": ["SA1", "NSW1"],
        "FUEL_CATEGORY": ["Battery", "Fossil"],
        "CAPACITY_MW": [150.0, 660.0],
        "TECHNOLOGY": ["Battery and Inverter", "Steam"],
        "CONNECTION_POINT": ["SMTL3H", "NBAY1"],
    })


def test_only_listed_successors_of_unlisted_predecessors_link():
    assert successors(_generators()) == {"HPRG1": "HPR1"}
    # A predecessor still in the list is its own unit, not folded.
    g = pd.concat([_generators(), _generators().iloc[[0]].assign(DUID="HPRG1")])
    assert successors(g) == {}


def test_predecessor_scada_is_aggregated_under_its_own_duid():
    ts = pd.date_range("2024-08-10 18:05", periods=12, freq="5min")
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": "HPRG1", "SCADAVALUE": 60.0})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "SA1", "RRP": 200.0})
    out = aggregate_month(scada, prices, None, with_predecessor_units(_generators()), {}, 2024, 8)
    assert out["duid"].tolist() == ["HPRG1"]
    row = out.iloc[0]
    assert row["generation_mwh"] == 60.0 and row["revenue_aud"] == 12000
    assert pd.isna(row["capacity_factor"])  # its own capacity is not known here
    # Without the lineage the row vanished (no region for an unlisted DUID).
    assert aggregate_month(scada, prices, None, _generators(), {}, 2024, 8).empty


def _monthly():
    return pd.DataFrame({
        "duid": ["HPRG1", "HPRG1", "HPR1", "HPR1", "BW01"],
        "month": ["2024-08", "2024-09", "2024-09", "2024-10", "2024-09"],
        "generation_mwh": [3000.0, 1000.0, 2000.0, 3100.0, 400000.0],
        "revenue_aud": [300000.0, 100000.0, 300000.0, 310000.0, 2e7],
        "capacity_factor": [None, None, 0.0185, 0.0278, 0.84],
        "captured_price": [102.0, 103.0, 155.0, 104.0, 50.0],
        "avg_rrp": [90.0, 95.0, 95.0, 80.0, 60.0],
        "price_capture_ratio": [1.1333, 1.0842, 1.6316, 1.3, 0.8333],
        "revenue_mlf_status": ["exact", "exact", "exact", "exact", "exact"],
        "revenue_mlf_value": [0.9657, 0.9657, 0.9625, 0.9625, 0.96],
    })


def test_predecessor_months_fold_into_the_successor():
    out = merge_lineage_monthly(_monthly(), _generators())
    hpr = out[out["duid"] == "HPR1"].set_index("month")
    assert list(hpr.index) == ["2024-08", "2024-09", "2024-10"]
    assert "HPRG1" not in set(out["duid"])
    # Aug 2024: HPRG1 alone, CF against HPR1's 150 MW: 3000 / (150 x 744).
    assert hpr.loc["2024-08", "capacity_factor"] == round(3000 / (150 * 744), 4)
    assert hpr.loc["2024-08", "source_duids"] == "HPRG1"
    # Sep 2024: both DUIDs ran; energies and revenue sum, price is gen-weighted.
    sep = hpr.loc["2024-09"]
    assert sep["generation_mwh"] == 3000.0 and sep["revenue_aud"] == 400000
    assert sep["captured_price"] == round((103 * 1000 + 155 * 2000) / 3000, 2)
    assert sep["capacity_factor"] == round(3000 / (150 * 720), 4)
    assert sep["source_duids"] == "HPR1+HPRG1"
    # Oct 2024: the successor's own month is published exactly as aggregated.
    assert hpr.loc["2024-10", "capacity_factor"] == 0.0278
    assert hpr.loc["2024-10", "source_duids"] == "HPR1"
    # Units outside the lineage are untouched.
    assert out[out["duid"] == "BW01"]["capacity_factor"].tolist() == [0.84]


def test_successor_json_carries_the_merged_history_and_no_predecessor_file(tmp_path):
    gens = _generators()
    out_dir = tmp_path / "generators"
    generate_all(gens, monthly_aggregates=merge_lineage_monthly(_monthly(), gens),
                 output_dir=str(out_dir))
    doc = json.loads((out_dir / "HPR1.json").read_text())
    assert doc["monthly"]["months"] == ["2024-08", "2024-09", "2024-10"]
    assert doc["monthly"]["source_duids"] == ["HPRG1", "HPR1+HPRG1", "HPR1"]
    assert doc["predecessors"] == ["HPRG1"]
    assert not (out_dir / "HPRG1.json").exists()
    index = json.loads((tmp_path / "index.json").read_text())
    assert "HPRG1" not in {e["duid"] for e in index}


def test_daily_rows_fold_and_recompute_cf():
    daily = pd.DataFrame({
        "duid": ["HPRG1", "HPR1", "HPR1"], "date": ["2024-09-12", "2024-09-12", "2024-09-13"],
        "daily_generation_mwh": [100.0, 260.0, 360.0], "daily_capacity_factor": [None, 0.0722, 0.1],
        "intervals_observed": [140, 148, 288], "intervals_expected": [288, 288, 288],
    })
    out = merge_lineage_daily(daily, _generators())
    assert out["duid"].tolist() == ["HPR1", "HPR1"]
    first = out.iloc[0]
    assert first["daily_generation_mwh"] == 360.0
    assert first["daily_capacity_factor"] == 0.1
    assert first["intervals_observed"] == 288


def test_daily_aggregation_keeps_predecessor_rows():
    ts = pd.date_range("2024-08-10 00:05", periods=288, freq="5min")
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": "HPRG1", "SCADAVALUE": 10.0})
    out = aggregate_month_daily(scada, with_predecessor_units(_generators()), 2024, 8)
    assert set(out["duid"]) == {"HPRG1"}
