"""Regional FCAS prices are labelled regional average everywhere (L7).

Unit docs carried scope "regional_average", but station docs and the CSV/XLSX
export published the same regional prices with no label.
"""

import json

import pandas as pd

from src.generate_json import generate_all
from tests.page_js import run


def test_station_fcas_block_is_scoped_regional_average(tmp_path):
    gens = pd.DataFrame({
        "DUID": ["A", "B"], "STATION_NAME": ["Two Unit", "Two Unit"],
        "REGION": ["QLD1", "QLD1"], "FUEL_CATEGORY": ["Wind", "Wind"],
        "CAPACITY_MW": [100.0, 100.0], "TECHNOLOGY": ["Wind", "Wind"],
        "CONNECTION_POINT": ["", ""],
    })
    monthly = pd.DataFrame({"duid": ["A", "B"], "month": ["2026-08", "2026-08"],
                            "generation_mwh": [1.0, 1.0], "revenue_aud": [1.0, 1.0],
                            "capacity_factor": [0.1, 0.1]})
    out = tmp_path / "generators"
    generate_all(gens, monthly_aggregates=monthly,
                 fcas_data={("QLD1", "2026-08"): {"Raise Reg": 12.3}}, output_dir=str(out))
    station = json.loads((out / "station_Two_Unit.json").read_text())
    unit = json.loads((out / "A.json").read_text())
    assert station["fcas"]["scope"] == unit["fcas"]["scope"] == "regional_average"
    assert station["fcas"]["services"] == {"Raise Reg": [12.3]}


def test_export_columns_say_regional_average():
    rows = run(["buildFCASRows"], """buildFCASRows({fcas: {months: ['2026-08'],
        services: {'Raise Reg': [12.3]}}})""")
    assert rows == [{"Month": "2026-08", "Raise Reg (regional average)": 12.3}]
