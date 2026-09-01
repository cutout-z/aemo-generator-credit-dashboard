"""Data freshness guards — fail loudly when the pipeline silently stops updating.

Motivation: the daily GitHub commit is NOT a freshness signal. The pipeline
re-processes the most recent ~2 months of the MMSDM monthly archive; if every
download fails (or AEMO republishes nothing), the pipeline still runs, still
tests (tests only check gaps BETWEEN existing months, not the presence of the
latest month), still commits identical or stale data. These guards assert the
data actually advanced.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

# AEMO publishes the MMSDM monthly archive ~2 weeks after month-end. The
# pipeline's own "latest" pointer is (now - 20 days). Allow one extra full
# month of publication delay before declaring staleness: on 1 Sep, the latest
# expected month is July (pointer month Aug is not yet published).
MONTHLY_MAX_LAG_DAYS = 75
# Daily aggregates are rebuilt from the same monthly archive, so they lag the
# same way (latest daily date ≈ end of the newest published month).
DAILY_MAX_LAG_DAYS = 60


def check_monthly_freshness(
    monthly_aggregates: pd.DataFrame,
    now: datetime | None = None,
    max_lag_days: int = MONTHLY_MAX_LAG_DAYS,
) -> None:
    """Assert monthly aggregates contain a reasonably recent month.

    Raises RuntimeError when the newest month in the data is older than
    max_lag_days behind (now - 20d), i.e. the pipeline has stopped ingesting
    new months. Raises ValueError when the frame is empty or has no month col.
    """
    if monthly_aggregates is None or monthly_aggregates.empty or "month" not in monthly_aggregates.columns:
        raise ValueError("Monthly aggregates missing or lack 'month' column — cannot verify freshness")

    now = now or datetime.now()
    latest_month = sorted(monthly_aggregates["month"].dropna().unique())[-1]
    latest_ts = pd.Timestamp(latest_month) + pd.offsets.MonthEnd(0)
    lag_days = (now - latest_ts.to_pydatetime()).days

    if lag_days > max_lag_days:
        raise RuntimeError(
            f"Freshness guard FAILED: latest monthly aggregate is {latest_month} "
            f"({lag_days} days old, limit {max_lag_days}). The pipeline has not "
            "ingested a new month — check NEMWEB downloads before publishing stale data."
        )
    logger.info("Freshness guard: latest monthly aggregate %s (%d days old) — OK", latest_month, lag_days)


def check_daily_freshness(
    daily_aggregates: pd.DataFrame,
    now: datetime | None = None,
    max_lag_days: int = DAILY_MAX_LAG_DAYS,
) -> None:
    """Assert daily aggregates extend close to the newest published month."""
    if daily_aggregates is None or daily_aggregates.empty or "date" not in daily_aggregates.columns:
        logger.warning("Daily aggregates missing — skipping daily freshness check")
        return

    now = now or datetime.now()
    latest_date = pd.Timestamp(sorted(daily_aggregates["date"].dropna().unique())[-1])
    lag_days = (now - latest_date.to_pydatetime()).days

    if lag_days > max_lag_days:
        raise RuntimeError(
            f"Freshness guard FAILED: latest daily aggregate is {latest_date.date()} "
            f"({lag_days} days old, limit {max_lag_days}). Daily rebuilds have stalled."
        )
    logger.info("Freshness guard: latest daily aggregate %s (%d days old) — OK", latest_date.date(), lag_days)


def mac_side_staleness_check(
    processed_cache_dir: str,
    now: datetime | None = None,
) -> list[str]:
    """Mac-side post-pull check: is the published dashboard data stale?

    Reads the committed processed-cache feathers (the same data the dashboard
    serves). Returns a list of alert strings — empty when fresh. Intended for
    the aemo-dashboard-autopull hook; alerts print so the cron surface shows
    them.
    """
    from pathlib import Path

    alerts: list[str] = []
    cache = Path(processed_cache_dir)
    now = now or datetime.now()

    monthly_path = cache / "monthly_aggregates.feather"
    daily_path = cache / "daily_aggregates.feather"

    try:
        if monthly_path.exists():
            check_monthly_freshness(pd.read_feather(monthly_path), now=now)
        else:
            alerts.append(f"AEMO ALERT: {monthly_path} missing — cannot verify monthly freshness")
    except (RuntimeError, ValueError) as e:
        alerts.append(f"AEMO ALERT: {e}")

    try:
        if daily_path.exists():
            check_daily_freshness(pd.read_feather(daily_path), now=now)
        else:
            alerts.append(f"AEMO ALERT: {daily_path} missing — cannot verify daily freshness")
    except (RuntimeError, ValueError) as e:
        alerts.append(f"AEMO ALERT: {e}")

    return alerts
