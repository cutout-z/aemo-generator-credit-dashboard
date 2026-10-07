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

# AEMO publishes the MMSDM monthly archive 12-28 days after month end (2026:
# Feb 12 for January ... Sep 28 for August). Just before an archive lands, the
# newest month can therefore be one full month plus that lag old: up to
# 31 + 28 = 59 days (Sep 27, newest month July). 75 days leaves about two
# weeks of extra AEMO delay before the run refuses to publish; anything
# tighter fails runs on ordinary publication timing. The run manifest records
# the actual age (guards.monthly_age_days) so a creeping lag is visible
# before the guard trips.
MONTHLY_MAX_LAG_DAYS = 75
# Daily aggregates are rebuilt from the same monthly archive, so they lag the
# same way (latest daily date = last day of the newest published month) and
# get the same limit. 60 days left about one day beyond AEMO's worst observed
# lag (59 days), so a late archive would have failed the run on the daily
# guard alone (audit 2026-10-07, S3-5).
DAILY_MAX_LAG_DAYS = MONTHLY_MAX_LAG_DAYS


def check_monthly_freshness(
    monthly_aggregates: pd.DataFrame,
    now: datetime | None = None,
    max_lag_days: int = MONTHLY_MAX_LAG_DAYS,
) -> int:
    """Assert monthly aggregates contain a reasonably recent month.

    Raises RuntimeError when the newest month's last day is more than
    max_lag_days before ``now``, i.e. the pipeline has stopped ingesting new
    months. Raises ValueError when the frame is empty or has no month col.
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
    return lag_days


def check_daily_freshness(
    daily_aggregates: pd.DataFrame,
    now: datetime | None = None,
    max_lag_days: int = DAILY_MAX_LAG_DAYS,
) -> int | None:
    """Assert daily aggregates extend close to the newest published month."""
    if daily_aggregates is None or daily_aggregates.empty or "date" not in daily_aggregates.columns:
        logger.warning("Daily aggregates missing — skipping daily freshness check")
        return None

    now = now or datetime.now()
    latest_date = pd.Timestamp(sorted(daily_aggregates["date"].dropna().unique())[-1])
    lag_days = (now - latest_date.to_pydatetime()).days

    if lag_days > max_lag_days:
        raise RuntimeError(
            f"Freshness guard FAILED: latest daily aggregate is {latest_date.date()} "
            f"({lag_days} days old, limit {max_lag_days}). Daily rebuilds have stalled."
        )
    logger.info("Freshness guard: latest daily aggregate %s (%d days old) — OK", latest_date.date(), lag_days)
    return lag_days


def mac_side_staleness_check(
    processed_cache_dir: str,
    now: datetime | None = None,
) -> list[str]:
    """Mac-side post-pull check: is the published dashboard data stale?

    Reads the committed processed-cache feathers (the same data the dashboard
    serves). Returns a list of alert strings — empty when fresh. Intended for
    the aemo-dashboard-autopull hook; alerts print so the cron surface shows
    them.

    S3-08: also consumes the committed machine-readable run manifest
    (``docs/data/run_status.json``, written by the pipeline each publish) —
    any optional source lane that ended ``degraded``/``error`` adds an AEMO
    ALERT, so a source failure can never hide behind core data that still
    looks fresh. The manifest is a sibling of the processed-cache dir.
    """
    from pathlib import Path

    from .run_status import RUN_STATUS_FILENAME, manifest_alerts

    alerts: list[str] = []
    cache = Path(processed_cache_dir)
    now = now or datetime.now()

    manifest_path = cache.parent / RUN_STATUS_FILENAME
    alerts.extend(manifest_alerts(manifest_path))

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
