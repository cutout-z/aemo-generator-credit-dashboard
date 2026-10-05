"""Single-owner persistence for month-keyed factor histories (S3-07).

The factor builders (`fcas_factor.compute_fcas_factors`, the
`offer_curves.build_offer_*` fetchers/computers) RETURN facts and never
write a cache file. This module is the one place an accumulated
factor-history feather is read, merged and written, so a warm incremental
run can never overwrite the very file the caller is about to read back as
old history — the self-overwriting warm-merge failure mode S3-07 removed.

Merge semantics mirror the aggregate histories (main.py Step 3): a normal
run replaces only the months the freshly computed rows cover and preserves
every out-of-window month; a full refresh replaces the file wholesale.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _as_number(value) -> float:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return float("nan")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def month_completeness(
    frame: pd.DataFrame, cols: tuple[str, ...],
) -> dict[str, tuple]:
    """Per-month completeness score: the max of each ``cols`` column (bools as
    0/1, missing as -1), compared lexicographically. Absent columns score -1."""
    scores: dict[str, tuple] = {}
    for month, grp in frame.groupby("month", sort=False):
        score = []
        for col in cols:
            if col not in grp.columns:
                score.append(-1)
                continue
            vals = pd.to_numeric(grp[col].map(_as_number), errors="coerce")
            score.append(float(vals.max()) if vals.notna().any() else -1)
        scores[month] = tuple(score)
    return scores


def merge_month_rows(
    cache_path: str | Path,
    new_rows: pd.DataFrame,
    *,
    full_refresh: bool = False,
    label: str = "factor rows",
    completeness_cols: tuple[str, ...] | None = None,
    kept_months: list[str] | None = None,
) -> pd.DataFrame:
    """Merge freshly computed per-month rows into the cached history file.

    Reads the existing file FIRST (before any write), drops from it every
    month the new rows cover, concatenates the new rows, and writes the
    merged frame exactly once. Returns the merged frame (== file contents).

    An empty ``new_rows`` leaves the file untouched and returns an empty
    frame: a run with no new facts must not silently replace history with a
    partial window, and an absent factor block is the caller's rendering
    decision (S3-08), not something this helper papers over with stale reads.

    ``full_refresh=True`` replaces the file wholesale (deliberate audited
    historical rewrite) instead of merging.

    ``completeness_cols``: when given, a cached month whose
    :func:`month_completeness` score is HIGHER than the fresh month's is kept
    and the fresh rows for that month are dropped (ties go to the fresh rows).
    This is what stops a one-trading-day stub of a finished month — which a
    later month's fetch window used to produce — from replacing the full
    month in the offer-factor history. Months kept that way are appended to
    ``kept_months`` (when a list is passed) so the lane can report them.
    """
    cache_path = Path(cache_path)
    if new_rows is None or new_rows.empty:
        return pd.DataFrame()
    if "month" not in new_rows.columns:
        raise ValueError(
            f"{label}: new rows must carry a 'month' column "
            "(merge is by month membership)"
        )

    if cache_path.exists() and not full_refresh:
        existing = pd.read_feather(cache_path)
        if completeness_cols and not existing.empty:
            old_scores = month_completeness(existing, completeness_cols)
            new_scores = month_completeness(new_rows, completeness_cols)
            keep_old = sorted(
                m for m, s in new_scores.items()
                if m in old_scores and old_scores[m] > s
            )
            for m in keep_old:
                logger.warning(
                    "%s %s: fresh rows are less complete than the cached month "
                    "(%s < %s) — cached month kept",
                    label, m, new_scores[m], old_scores[m],
                )
            if kept_months is not None:
                kept_months.extend(keep_old)
            new_rows = new_rows[~new_rows["month"].isin(keep_old)]
        reprocessed = set(pd.unique(new_rows["month"]))
        existing = existing[~existing["month"].isin(reprocessed)]
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        merged = new_rows

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_feather(cache_path)
    logger.info("Saved %d %s rows to %s", len(merged), label, cache_path)
    return merged
