"""Semantic-diff publish gate (S3-12).

A no-change pipeline run must not bump publish timestamps: "updated_utc" in
market_daily.json and "updated" in the per-DUID daily offer-curve files are
DATA-AS-OF stamps on fact files, not run-attempt records. Rebuilding the same
facts at a later clock time produces byte-different JSON whose only difference
is the stamp — and on the NAS every such file is a git diff and a commit, i.e.
manufactured publishable churn for unchanged source data.

The gate: compare the candidate payload to the existing file with the stamp
keys EXCLUDED, and write only when the semantic facts differ (or the file is
missing/unreadable). An unchanged run leaves the file and its stamp untouched;
a changed run rewrites with a fresh stamp. Run-attempt state lives in the
separate run-status manifest (docs/data/run_status.json), which may advance
even when no fact file changes.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def facts_equal(new_payload: dict, old_payload: dict, stamp_keys=()) -> bool:
    """Compare two JSON payloads ignoring the given stamp keys.

    Comparison is order-independent (``sort_keys`` canonicalisation) and
    numeric-value based (``json.dumps``/``loads`` round trip), so identical
    facts from a later clock compare equal.
    """
    if isinstance(stamp_keys, str):
        stamp_keys = (stamp_keys,)
    stamp_keys = set(stamp_keys)

    def _facts(payload: dict) -> str:
        clean = {k: v for k, v in payload.items() if k not in stamp_keys}
        return json.dumps(clean, sort_keys=True, separators=(",", ":"))

    return _facts(new_payload) == _facts(old_payload)


def write_json_if_facts_changed(
    path: Path,
    payload: dict,
    *,
    stamp_keys=("updated_utc",),
) -> bool:
    """Write ``payload`` to ``path`` only when its facts differ from the file.

    Returns True when the file was written (facts changed / file absent /
    file unreadable), False when the existing file was left untouched because
    the semantic facts are unchanged.
    """
    path = Path(path)
    if path.exists():
        try:
            existing = json.loads(path.read_text())
            if isinstance(existing, dict) and facts_equal(payload, existing, stamp_keys):
                logger.info(
                    "%s unchanged (semantic diff) — stamp not bumped", path.name,
                )
                return False
        except (OSError, ValueError):
            logger.warning(
                "%s unreadable — rewriting", path.name,
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":")))
    return True
