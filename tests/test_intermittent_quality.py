import pandas as pd

from src.aggregate import aggregate_month
from src.download_intermittent import _summarise_quality


def test_summarise_quality_counts_elav_good_intervals_only():
    raw = pd.DataFrame(
        {
            "DUID": ["GEN1", "GEN1", "GEN1", "GEN2"],
            "SCADA_TYPE": ["ELAV", "ELAV", "MW", "ELAV"],
            "SCADA_QUALITY": ["Good", "Bad", "Good", "Good"],
        }
    )

    summary = _summarise_quality(raw, 2026, 3, "test")

    gen1 = summary.set_index("DUID").loc["GEN1"]
    assert gen1["total_intervals"] == 2
    assert gen1["good_intervals"] == 1
    assert "GEN2" in set(summary["DUID"])


def test_aggregate_month_accepts_quality_summary_for_curtailment_split():
    ts = pd.to_datetime(["2026-03-01 00:05:00", "2026-03-01 00:10:00"])
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": ["GEN1", "GEN1"], "SCADAVALUE": [5.0, 5.0]})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": ["NSW1", "NSW1"], "RRP": [100.0, 100.0]})
    dispatchload = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": ["GEN1", "GEN1"], "AVAILABILITY": [10.0, 10.0]})
    generators = pd.DataFrame(
        {
            "DUID": ["GEN1"],
            "REGION": ["NSW1"],
            "CAPACITY_MW": [10.0],
            "FUEL_CATEGORY": ["Solar"],
        }
    )
    intermittent = pd.DataFrame({"DUID": ["GEN1"], "total_intervals": [4], "good_intervals": [3]})

    result = aggregate_month(scada, prices, dispatchload, generators, {}, 2026, 3, intermittent)
    row = result.iloc[0]

    assert row["curtailment_pct"] == 0.5
    assert row["grid_curtailment_pct"] == 0.375
    assert row["mechanical_curtailment_pct"] == 0.125
