"""PR1 (portfolio view): unit and station JSONs publish the energy behind
curtailment_pct, so a reader can sum any set of units into a portfolio as
1 − Σ curtailment_actual_mwh / Σ curtailment_potential_mwh.

Rebuilding the energy from the published (rounded) percentage is not enough:
a fully-curtailed month has no generation to divide back up, and months short
of availability coverage (Sep 2025) differ from generation/(1 − pct).
"""

import json

import pandas as pd
import pytest

from src.generate_json import generate_all, generate_generator_json
from tests.test_s304_curtailment_denominators import _coerce_num, _month_row


def test_unit_json_publishes_curtailment_energy(tmp_path):
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND1", "2026-01", 900.0, curt_pct=0.1, pot=1000.0, act=900.0),
        _month_row("WIND1", "2026-02", 0.0, curt_pct=1.0, pot=800.0, act=0.0),
        _month_row("WIND1", "2026-03", 500.0, curt_pct=None),
    ]))
    path = generate_generator_json("WIND1", {"fuel_category": "Wind"}, monthly,
                                   output_dir=str(tmp_path))
    doc = json.loads(path.read_text())["monthly"]
    assert doc["curtailment_actual_mwh"] == [900.0, 0.0, None]
    assert doc["curtailment_potential_mwh"] == [1000.0, 800.0, None]
    assert len(doc["curtailment_actual_mwh"]) == len(doc["months"])


def test_unit_json_legacy_rows_recover_energy_from_generation(tmp_path):
    """Rows cached before the v3 columns: potential = generation / (1 − pct),
    the same resolution the station and FY rollups use."""
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND1", "2026-01", 900.0, curt_pct=0.1),
    ]))
    path = generate_generator_json("WIND1", {"fuel_category": "Wind"}, monthly,
                                   output_dir=str(tmp_path))
    doc = json.loads(path.read_text())["monthly"]
    assert doc["curtailment_actual_mwh"] == [900.0]
    assert doc["curtailment_potential_mwh"] == [1000.0]


def test_station_energy_is_sum_of_members_and_reproduces_pct(tmp_path):
    generators = pd.DataFrame({
        "DUID": ["WIND_A", "WIND_B"],
        "STATION_NAME": ["Portfolio Check Wind", "Portfolio Check Wind"],
        "REGION": ["VIC1", "VIC1"],
        "FUEL_CATEGORY": ["Wind", "Wind"],
        "CAPACITY_MW": [100.0, 300.0],
        "TECHNOLOGY": ["Wind", "Wind"],
        "CONNECTION_POINT": ["CP_A", "CP_B"],
    })
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND_A", "2026-04", 1000.0, curt_pct=0.0, pot=1000.0, act=1000.0),
        _month_row("WIND_B", "2026-04", 0.0, curt_pct=1.0, pot=1000.0, act=0.0),
        _month_row("WIND_A", "2026-05", 700.0, curt_pct=0.3, pot=1000.0, act=700.0),
        _month_row("WIND_B", "2026-05", 2400.0, curt_pct=0.2, pot=3000.0, act=2400.0),
    ]))
    out = tmp_path / "generators"
    generate_all(generators, monthly_aggregates=monthly, output_dir=str(out))
    station = json.loads((out / "station_Portfolio_Check_Wind.json").read_text())["monthly"]
    assert station["curtailment_actual_mwh"] == [1000.0, 3100.0]
    assert station["curtailment_potential_mwh"] == [2000.0, 4000.0]

    # The portfolio identity: summing the unit files reproduces the station.
    units = [json.loads((out / f"{d}.json").read_text())["monthly"] for d in ("WIND_A", "WIND_B")]
    for i, pct in enumerate(station["curtailment_pct"]):
        act = sum(u["curtailment_actual_mwh"][i] for u in units)
        pot = sum(u["curtailment_potential_mwh"][i] for u in units)
        assert 1 - act / pot == pytest.approx(pct, abs=1e-4)
