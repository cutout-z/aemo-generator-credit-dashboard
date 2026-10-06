"""The daily "Last 12 Months" window is 12 months of data (audit 2026-10, L1).

The cut was today - 365 days, but the newest day trails today by the MMSDM
lag, so on 5 Oct 2026 the window held 2025-10-05..2026-08-31 (331 days).
"""

import pandas as pd

from src.main import trim_daily_window


def test_window_counts_back_from_the_newest_day():
    dates = pd.date_range("2025-06-01", "2026-08-31", freq="D").strftime("%Y-%m-%d")
    daily = pd.DataFrame({"duid": "BW01", "date": dates})
    kept = trim_daily_window(daily)
    assert kept["date"].min() == "2025-08-31"
    assert kept["date"].max() == "2026-08-31"
    assert len(kept) == 366
