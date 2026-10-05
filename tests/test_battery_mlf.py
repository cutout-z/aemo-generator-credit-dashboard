"""Battery (bidirectional) revenue is MLF-adjusted by its own FY factor.

Audit 2026-10 (H3): 26 batteries' settled revenue up to 2026-01 implies a
factor of exactly 1.000. Their export (generation) MLFs for FY24-25 are RESS1
0.8702, BHB1 0.8423 and RIVNB2 0.9048. Those come from DUDETAILSUMMARY
SECONDARY_TLF. The pre-fix MLF tracker carried the import factor for these
years instead: 0.8657 / 0.8284 / 0.8774. The cause was the pre-2026-04-21 MLF source (src/download_mlf.py),
which kept only DUDETAILSUMMARY rows with DISPATCHTYPE == "GENERATOR" and so
dropped every BIDIRECTIONAL unit. The tracker-based lookup used since then
resolves them; these tests pin that a battery's implied revenue factor equals
the factor of the FY each month falls in.
"""

import pandas as pd
import pytest

from src.aggregate import MLF_STATUS_EXACT, aggregate_month, build_mlf_lookup

# Export (generation-side) FY factors for three of the affected units.
FY_FACTORS = {
    ("RESS1", 2024): 0.8702, ("RESS1", 2025): 0.8781,
    ("BHB1", 2024): 0.8423, ("BHB1", 2025): 0.8999,
    ("RIVNB2", 2024): 0.9048, ("RIVNB2", 2025): 0.9173,
}


def _history():
    return pd.DataFrame({
        "DUID": [d for d, _ in FY_FACTORS],
        "fy_label": [f"FY{fy % 100:02d}-{(fy + 1) % 100:02d}" for _, fy in FY_FACTORS],
        "fy_start_year": [fy for _, fy in FY_FACTORS],
        "mlf": list(FY_FACTORS.values()),
    })


def _month(duid, year, month):
    ts = pd.date_range(f"{year}-{month:02d}-10 18:05", periods=24, freq="5min")
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": duid, "SCADAVALUE": 50.0})
    # Varying prices, so a factor of 1.0 cannot hide behind a flat price.
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "VIC1",
                           "RRP": [80.0 + 15 * i for i in range(len(ts))]})
    gens = pd.DataFrame({"DUID": [duid], "REGION": ["VIC1"], "CAPACITY_MW": [100.0],
                         "FUEL_CATEGORY": ["Battery"]})
    fy = year if month >= 7 else year - 1
    return aggregate_month(scada, prices, None, gens,
                           build_mlf_lookup(_history(), fy), year, month).iloc[0]


@pytest.mark.parametrize("duid", ["RESS1", "BHB1", "RIVNB2"])
@pytest.mark.parametrize("year,month,fy", [(2024, 11, 2024), (2025, 6, 2024),
                                           (2025, 7, 2025), (2026, 1, 2025)])
def test_battery_implied_factor_equals_fy_factor(duid, year, month, fy):
    row = _month(duid, year, month)
    implied = row["revenue_aud"] / (row["generation_mwh"] * row["captured_price"])
    assert implied == pytest.approx(FY_FACTORS[(duid, fy)], abs=5e-4)
    assert implied != pytest.approx(1.0, abs=5e-3)
    assert row["revenue_mlf_status"] == MLF_STATUS_EXACT
    assert row["revenue_mlf_value"] == FY_FACTORS[(duid, fy)]
