"""curtailment_by_fy.csv says which FY rows are partial (audit 2026-10, L3)."""

import pandas as pd

from src.generate_json import write_curtailment_by_fy


def test_partial_and_complete_fys_are_flagged(tmp_path):
    full = pd.date_range("2024-07-01", "2025-06-01", freq="MS").strftime("%Y-%m")
    part = ["2025-07", "2025-08"]
    months = list(full) + part
    df = pd.DataFrame({
        "duid": "SF1", "month": months, "generation_mwh": 100.0,
        "curtailment_pct": 0.1, "curtailment_actual_mwh": 90.0,
        "curtailment_potential_mwh": 100.0,
    })
    raw = pd.read_csv(write_curtailment_by_fy(df, str(tmp_path)))
    out = raw.set_index("fy_label")
    assert bool(out.loc["FY24-25", "fy_complete"]) is True
    assert bool(out.loc["FY25-26", "fy_complete"]) is False
    assert out.loc["FY25-26", "first_month"] == "2025-07"
    assert out.loc["FY25-26", "last_month"] == "2025-08"
    # The existing columns keep their names and order (the renewable
    # dashboard reads them by name).
    assert list(raw.columns[:7]) == [
        "duid", "fy_start", "fy_label", "curtailment_pct", "metric_version",
        "generation_mwh", "months_covered",
    ]
