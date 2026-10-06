"""Station capacity factor uses the capacity of members that report (M3).

Snuggery Power Station registers 126 MW but only SNUG1 (63 MW) sends SCADA;
dividing by all 126 MW halved its published CF. Kareeya (-7.7%) and Eildon
(-3.6%) were understated the same way.
"""

import json
import math

import pandas as pd

from src.generate_json import generate_all


def _snuggery():
    return pd.DataFrame({
        "DUID": ["SNUG1", "SNUG2", "SNUG3", "SNUGNL1"],
        "STATION_NAME": ["Snuggery Power Station"] * 4,
        "REGION": ["SA1"] * 4,
        "FUEL_CATEGORY": ["Fossil"] * 4,
        "CAPACITY_MW": [63.0, 21.0, 21.0, 21.0],
        "TECHNOLOGY": ["OCGT"] * 4,
        "CONNECTION_POINT": [""] * 4,
    })


def _write(tmp_path, monthly, daily=None):
    out = tmp_path / "generators"
    generate_all(_snuggery(), monthly_aggregates=monthly, daily_aggregates=daily,
                 output_dir=str(out))
    return json.loads((out / "station_Snuggery_Power_Station.json").read_text())


def test_monthly_cf_counts_only_reporting_members(tmp_path):
    monthly = pd.DataFrame({
        "duid": ["SNUG1"], "month": ["2026-08"], "generation_mwh": [4687.2],
        "revenue_aud": [1.0], "capacity_factor": [0.1],
    })
    doc = _write(tmp_path, monthly)
    # 4687.2 MWh / (63 MW x 744 h) = 0.1; over all 126 MW it read 0.05.
    assert doc["monthly"]["capacity_factor"] == [0.1]
    assert doc["monthly"]["capacity_basis_mw"] == [63.0]
    assert doc["capacity_mw"] == 126.0  # registered total is unchanged


def test_daily_cf_counts_only_reporting_members(tmp_path):
    monthly = pd.DataFrame({
        "duid": ["SNUG1"], "month": ["2026-08"], "generation_mwh": [1.0],
        "revenue_aud": [1.0], "capacity_factor": [0.0],
    })
    daily = pd.DataFrame({
        "duid": ["SNUG1", "SNUG1", "SNUG2"], "date": ["2026-08-01", "2026-08-02", "2026-08-02"],
        "daily_generation_mwh": [151.2, 151.2, 50.4],
        "daily_capacity_factor": [0.1, 0.1, 0.1],
        "intervals_observed": [288, 288, 288], "intervals_expected": [288, 288, 288],
    })
    doc = _write(tmp_path, monthly, daily)
    cf = doc["daily"]["capacity_factor"]
    # Day 1: 151.2 / (63 x 24) = 0.1. Day 2 (SNUG2 reports too): 201.6 / (84 x 24) = 0.1.
    assert math.isclose(cf[0], 0.1) and math.isclose(cf[1], 0.1)
