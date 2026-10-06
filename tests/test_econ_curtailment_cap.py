"""Economic curtailment is part of the total shortfall and never exceeds it.

Audit 2026-10 (M5): 1,663 published month rows had econ_curtailment_pct above
curtailment_pct (MIDDLSF1 Dec 2024: total 0%, economic 20%). Output above
AVAILABILITY in positive-price intervals netted the total shortfall down while
the negative-price shortfall, the economic numerator, was untouched.
"""

import pandas as pd

from src.aggregate import aggregate_month
from src.generate_json import _aggregate_station_monthly


def _gens(duid="SF1"):
    return pd.DataFrame({
        "DUID": [duid], "REGION": ["NSW1"], "CAPACITY_MW": [100.0],
        "FUEL_CATEGORY": ["Solar"],
    })


def _month(duid="SF1"):
    ts = pd.date_range("2024-12-10 10:05", periods=4, freq="5min")
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": duid,
                          # bid off at negative prices, above availability after
                          "SCADAVALUE": [0.0, 0.0, 90.0, 90.0]})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "NSW1",
                           "RRP": [-30.0, -30.0, 60.0, 60.0]})
    load = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": duid,
                         "AVAILABILITY": [40.0, 40.0, 50.0, 50.0]})
    return scada, prices, load


def test_unit_economic_curtailment_is_capped_at_the_total_shortfall():
    scada, prices, load = _month()
    row = aggregate_month(scada, prices, load, _gens(), {}, 2024, 12).iloc[0]
    # Total: 1 - 180/180 = 0. Uncapped economic: (80 - 0) / 180 = 0.444.
    assert row["curtailment_pct"] == 0.0
    assert row["econ_curtailment_pct"] == 0.0


def test_unit_economic_curtailment_below_the_total_is_unchanged():
    scada, prices, load = _month()
    scada.loc[2:, "SCADAVALUE"] = 50.0  # no overshoot
    row = aggregate_month(scada, prices, load, _gens(), {}, 2024, 12).iloc[0]
    # Total 1 - 100/180 = 0.4444; economic 80/180 = 0.4444.
    assert row["curtailment_pct"] == 0.4444
    assert row["econ_curtailment_pct"] == 0.4444


def test_station_economic_curtailment_is_capped_at_the_station_total():
    rows = pd.DataFrame({
        "duid": ["A", "B"], "month": ["2024-12", "2024-12"],
        "generation_mwh": [100.0, 100.0], "revenue_aud": [0.0, 0.0],
        "capacity_factor": [0.1, 0.1],
        "curtailment_pct": [0.0, 0.0],
        "curtailment_actual_mwh": [100.0, 100.0],
        "curtailment_potential_mwh": [100.0, 100.0],
        # legacy uncapped unit rows
        "econ_curtailment_pct": [0.2, 0.2],
    })
    out = _aggregate_station_monthly(rows, 200.0, "Solar", {"A": 100.0, "B": 100.0})
    assert out["curtailment_pct"] == [0.0]
    assert out["econ_curtailment_pct"] == [0.0]


def test_published_unit_json_caps_rows_aggregated_before_the_fix(tmp_path):
    import json

    from src.generate_json import generate_generator_json

    monthly = pd.DataFrame({
        "month": ["2024-11", "2024-12"], "generation_mwh": [10.0, 10.0],
        "revenue_aud": [1.0, 1.0], "capacity_factor": [0.1, 0.1],
        "curtailment_pct": [0.05, 0.0],
        "econ_curtailment_pct": [0.01, 0.1999],
    })
    path = generate_generator_json("MIDDLSF1", {"fuel_category": "Solar"}, monthly,
                                   output_dir=str(tmp_path))
    doc = json.loads(path.read_text())
    assert doc["monthly"]["econ_curtailment_pct"] == [0.01, 0.0]
