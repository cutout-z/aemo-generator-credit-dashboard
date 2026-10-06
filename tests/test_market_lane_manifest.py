"""The market-spread lane is visible in the run manifest (audit 2026-10, M11).

run_status.json recorded market_factors as ok with asof_month null and 0
attempted months on every run, so a stalled market_daily.json was invisible.
"""

from datetime import datetime

import pandas as pd

from src import main as pipeline
from src.freshness import check_monthly_freshness
from src.run_status import STATUS_DEGRADED, STATUS_OK


def _market(last="2026-08-31"):
    dates = pd.date_range("2026-07-01", last, freq="D").strftime("%Y-%m-%d")
    return pd.DataFrame({"date": list(dates) * 2, "region": ["NSW1"] * len(dates) + ["SA1"] * len(dates)})


def _daily(last="2026-08-31"):
    return pd.DataFrame({"duid": ["A"], "date": [last]})


def test_fresh_market_lane_reports_coverage_and_as_of():
    lane = pipeline._market_lane(_market(), [(2026, 7), (2026, 8), (2026, 9)], _daily())
    rec = lane.manifest_record()
    assert rec["status"] == STATUS_OK
    assert rec["asof_month"] == "2026-08"
    assert rec["attempted_months"] == 3
    assert rec["months_with_data"] == 2


def test_stalled_market_spreads_degrade_the_lane():
    lane = pipeline._market_lane(_market(last="2026-07-31"), [(2026, 7)], _daily())
    assert lane.status == STATUS_DEGRADED
    assert "end 2026-07-31, 31 days before the daily data" in lane.error


def test_failed_market_build_is_degraded_with_its_error():
    lane = pipeline._market_lane(pd.DataFrame(), [], _daily(), error="boom")
    assert lane.status == STATUS_DEGRADED and lane.error.startswith("boom")


def test_monthly_freshness_returns_its_age_for_the_manifest():
    agg = pd.DataFrame({"month": ["2026-07", "2026-08"]})
    assert check_monthly_freshness(agg, now=datetime(2026, 10, 5)) == 35
