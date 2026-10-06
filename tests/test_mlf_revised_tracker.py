"""Dated factors apply when the tracker carries a mid-year revision (H5 follow-up).

The H5 fix trusted DUDETAILSUMMARY's dated factors only when the tracker's FY
value equalled the FY's OPENING factor. The tracker reports some units at
their revised value (QPSFB1 FY25-26: 0.9176, from 3 Feb 2026; it opened at
1.019), so for those units the dated factors were rejected and the revised
value was applied to the months before the revision: QPSFB1 Jan 2026,
LIMOSF11 1-28 Jul 2025, WDBESS1 1 Jul-7 Sep 2025, BROOKLYN to 18 Nov 2025.
"""

import pandas as pd

from src.aggregate import REVENUE_MLF_SOURCE_COL, REVENUE_MLF_VALUE_COL, aggregate_month, build_mlf_lookup
from src.loss_factors import fy_tlf_values
from tests.test_loss_factors import QPSFB1_ROWS, _archive, _history  # noqa: F401
from src.loss_factors import parse_dudetailsummary


def _periods():
    return parse_dudetailsummary(_archive(QPSFB1_ROWS))


def _row(stamp, year, month, tracker):
    ts = pd.to_datetime([stamp])
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": "QPSFB1", "SCADAVALUE": 60.0})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "QLD1", "RRP": 100.0})
    gens = pd.DataFrame({"DUID": ["QPSFB1"], "REGION": ["QLD1"], "CAPACITY_MW": [96.0],
                         "FUEL_CATEGORY": ["Solar"]})
    hist = _history([("QPSFB1", 2025, tracker)])
    return aggregate_month(scada, prices, None, gens, build_mlf_lookup(hist, 2025),
                           year, month, loss_factor_periods=_periods()).iloc[0]


def test_fy_factor_set_includes_the_revision():
    assert fy_tlf_values(_periods(), 2025) == {"QPSFB1": [0.9176, 1.019]}


def test_tracker_at_the_revised_value_still_uses_dated_factors():
    jan = _row("2026-01-25 12:00", 2026, 1, tracker=0.9176)
    # Before the revision the unit carried 1.019: $500 x 1.019, not x 0.9176.
    assert jan[REVENUE_MLF_VALUE_COL] == 1.019
    assert jan["revenue_aud"] == round(500 * 1.019)
    assert jan[REVENUE_MLF_SOURCE_COL] == "dudetailsummary"
    mar = _row("2026-03-10 12:00", 2026, 3, tracker=0.9176)
    assert mar[REVENUE_MLF_VALUE_COL] == 0.9176


def test_a_tracker_value_matching_no_dated_factor_is_still_kept():
    row = _row("2026-03-10 12:00", 2026, 3, tracker=0.95)
    assert row[REVENUE_MLF_VALUE_COL] == 0.95
    assert row[REVENUE_MLF_SOURCE_COL] == "mlf-tracker"
