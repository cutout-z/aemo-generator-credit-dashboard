"""A station's fuel is its dominant fuel by capacity (audit 2026-10, M2).

Moorabool Wind Farm (312 MW wind + 2 MW battery) published as "Battery"
because the first-listed unit was the battery: curtailment panels hidden,
"not LGC-eligible", "charging cost not netted". 11 stations mix fuels.
"""

import json

import pandas as pd

from src.generate_json import generate_all, station_fuel_mix


def _moorabool():
    return pd.DataFrame({
        "DUID": ["MOORABS1", "MOORAWF1"],
        "STATION_NAME": ["Moorabool Wind Farm ", "Moorabool Wind Farm "],
        "REGION": ["VIC1", "VIC1"],
        "FUEL_CATEGORY": ["Battery", "Wind"],
        "CAPACITY_MW": [2.0, 312.0],
        "TECHNOLOGY": ["Battery and Inverter", "Wind - Onshore"],
        "CONNECTION_POINT": ["", "VMRB"],
    })


def test_dominant_fuel_by_capacity_not_first_listed():
    fuel, tech, mix = station_fuel_mix(_moorabool())
    assert fuel == "Wind"
    assert tech == "Wind - Onshore"
    assert mix == {"Wind": 312.0, "Battery": 2.0}


def test_hybrid_station_doc_and_index(tmp_path):
    monthly = pd.DataFrame({
        "duid": ["MOORAWF1"], "month": ["2026-08"], "generation_mwh": [90000.0],
        "revenue_aud": [5e6], "capacity_factor": [0.39],
        "curtailment_pct": [0.1], "curtailment_actual_mwh": [90000.0],
        "curtailment_potential_mwh": [100000.0], "econ_curtailment_pct": [0.04],
        "captured_price": [55.0], "avg_rrp": [80.0], "price_capture_ratio": [0.6875],
    })
    out = tmp_path / "generators"
    generate_all(_moorabool(), monthly_aggregates=monthly, output_dir=str(out))
    doc = json.loads((out / "station_Moorabool_Wind_Farm.json").read_text())
    assert doc["fuel_category"] == "Wind"
    assert doc["lgc_eligible"] is True
    assert doc["fuel_mix"] == {"Wind": 312.0, "Battery": 2.0}
    assert doc["monthly"]["curtailment_pct"] == [0.1]
    assert doc["monthly"]["econ_curtailment_pct"] == [0.04]
    index = json.loads((tmp_path / "index.json").read_text())
    entry = next(e for e in index if e.get("type") == "station")
    assert entry["fuel_category"] == "Wind"


def test_single_fuel_station_has_no_mix():
    g = _moorabool().assign(FUEL_CATEGORY="Wind")
    fuel, _tech, mix = station_fuel_mix(g)
    assert fuel == "Wind" and mix == {"Wind": 314.0}
