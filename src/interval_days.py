"""Interval-timestamp calendar-day semantics (S3-05).

NEMOSIS filters interval tables by ``(start, end]`` on the interval's
timestamp (nemosis/filters.py: filter_on_settlementdate / _interval_datetime),
so a month window ``[M/01 00:00, M+1/01 00:00]`` includes the interval that
ENDS at ``M+1/01 00:00:00`` — the 23:55–24:00 slice of M's final day — while
excluding the interval ending at ``M/01 00:00:00``.

AEMO's 5-minute interval tables (DISPATCH_UNIT_SCADA, DISPATCHLOAD and the
BIDPEROFFER_D volume lane) stamp each interval with its END time: a day's 288
intervals end at 00:05, 00:10, … 23:55 and at the NEXT day's 00:00. The
energy of the interval ending at midnight covers the preceding calendar
day's 23:55–24:00, so calendar-day assignment shifts midnight stamps back by
one minute. Day-keyed tables (BIDDAYOFFER_D, whose SETTLEMENTDATE names a
whole trading day) keep their date as-is — never feed those through
interval_calendar_day().
"""

from __future__ import annotations

import pandas as pd

# 24 h × 12 five-minute intervals/h = 288 intervals in a full day.
INTERVALS_PER_DAY = 24 * 12


def interval_calendar_day(ts: pd.Series) -> pd.Series:
    """Calendar day whose energy an interval-END timestamp belongs to.

    ``(end − 1 minute).date()``: the interval ending at 00:00:00 covers the
    previous day's final five minutes; every other ending time keeps its own
    date. Vectorised over a tz-naive (AEMO local time) datetime Series.
    """
    t = pd.to_datetime(ts)
    if t.empty:
        return t.dt.date
    return (t - pd.Timedelta(minutes=1)).dt.date


def interval_calendar_day_str(ts: pd.Series) -> pd.Series:
    """interval_calendar_day as ISO ``YYYY-MM-DD`` strings."""
    return interval_calendar_day(ts).astype(str)


def interval_month(ts: pd.Series) -> pd.Series:
    """Month period (``YYYY-MM``) an interval-END timestamp belongs to.

    Same midnight-shift rule as interval_calendar_day, applied at month
    granularity so an interval ending at the first instant of a month counts
    to the previous month (its energy is the prior day's final five minutes).
    """
    t = pd.to_datetime(ts)
    if t.empty:
        return t.dt.to_period("M").astype(str)
    return (t - pd.Timedelta(minutes=1)).dt.to_period("M").astype(str)
