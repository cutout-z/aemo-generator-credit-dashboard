import pandas as pd
import pytest

pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st  # noqa: E402

from src.interval_days import interval_calendar_day  # noqa: E402
from tests.hypothesis_profile import load_gate_profile  # noqa: E402

load_gate_profile()

# AEMO interval-END stamps sit on the 5-minute grid; the function shifts by one
# MINUTE (not one ns), which agrees with the ns rule for every grid stamp.
_ts = st.builds(
    lambda day, slot: pd.Timestamp("2000-01-01") + pd.Timedelta(days=day, minutes=5 * slot),
    st.integers(0, 36500),
    # bias to the midnight boundary slots (00:00, 00:05, 23:55) plus any slot
    st.one_of(st.sampled_from([0, 1, 287]), st.integers(0, 287)),
)


@given(st.lists(_ts, min_size=1, max_size=10))
def test_calendar_day_is_end_minus_one_ns_date(stamps):
    s = pd.Series(stamps)
    got = interval_calendar_day(s).tolist()
    assert got == [(t - pd.Timedelta(1, "ns")).date() for t in s]


def test_sub_minute_stamp_differs_from_ns_rule_by_design():
    """Off-grid stamps (never produced by AEMO) use the one-minute shift."""
    s = pd.Series([pd.Timestamp("2000-01-01 00:00:30")])
    assert interval_calendar_day(s).tolist() == [pd.Timestamp("1999-12-31").date()]
    assert (s[0] - pd.Timedelta(1, "ns")).date() == pd.Timestamp("2000-01-01").date()
