import json

import pandas as pd

from src.generate_json import generate_all


def test_station_aggregation_includes_mechanical_and_constraints(tmp_path):
    generators = pd.DataFrame(
        {
            "DUID": ["GEN_A", "GEN_B"],
            "STATION_NAME": ["Two Unit Wind Farm", "Two Unit Wind Farm"],
            "REGION": ["QLD1", "QLD1"],
            "FUEL_CATEGORY": ["Wind", "Wind"],
            "CAPACITY_MW": [100.0, 300.0],
            "TECHNOLOGY": ["Wind", "Wind"],
            "CONNECTION_POINT": ["CP_A", "CP_B"],
        }
    )
    monthly = pd.DataFrame(
        {
            "duid": ["GEN_A", "GEN_B"],
            "month": ["2026-04", "2026-04"],
            "generation_mwh": [1000.0, 3000.0],
            "revenue_aud": [50000.0, 150000.0],
            "capacity_factor": [0.1, 0.2],
            "curtailment_pct": [0.2, 0.4],
            "grid_curtailment_pct": [0.1, 0.2],
            "mechanical_curtailment_pct": [0.1, 0.3],
            "econ_curtailment_pct": [0.02, 0.06],
            "captured_price": [50.0, 60.0],
            "avg_rrp": [55.0, 55.0],
            "price_capture_ratio": [0.9091, 1.0909],
        }
    )
    constraints = pd.DataFrame(
        {
            "duid": ["GEN_A", "GEN_B", "GEN_B"],
            "month": ["2026-04", "2026-04", "2026-04"],
            "constraint_id": ["C_SHARED", "C_SHARED", "C_B_ONLY"],
            "description": ["Shared constraint", "Shared constraint", "Unit B constraint"],
            "hours_bound": [10.0, 10.0, 5.0],
        }
    )

    generate_all(
        generators,
        monthly_aggregates=monthly,
        constraint_data=constraints,
        output_dir=str(tmp_path / "generators"),
    )

    with open(tmp_path / "generators" / "station_Two_Unit_Wind_Farm.json") as f:
        station = json.load(f)

    assert station["monthly"]["curtailment_pct"] == [0.35]
    assert station["monthly"]["grid_curtailment_pct"] == [0.175]
    assert station["monthly"]["mechanical_curtailment_pct"] == [0.25]
    assert station["monthly"]["econ_curtailment_pct"] == [0.05]

    top_by_id = {row["id"]: row for row in station["constraints"]["top_constraints"]}
    assert top_by_id["C_SHARED"]["total_hours"] == 10.0
    assert top_by_id["C_B_ONLY"]["total_hours"] == 5.0
    assert station["constraints"]["heatmap"]["constraints"]["C_SHARED"] == [10.0]
