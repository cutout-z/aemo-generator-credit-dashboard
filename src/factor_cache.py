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


def merge_month_rows(
    cache_path: str | Path,
    new_rows: pd.DataFrame,
    *,
    full_refresh: bool = False,
    label: str = "factor rows",
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
        reprocessed = set(pd.unique(new_rows["month"]))
        existing = existing[~existing["month"].isin(reprocessed)]
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        merged = new_rows

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_feather(cache_path)
    logger.info("Saved %d %s rows to %s", len(merged), label, cache_path)
    return merged
