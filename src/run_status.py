"""S3-08: run-status manifest + optional-source factor continuity.

The daily AEMO pipeline treats FCAS/offer data as *optional* lanes: a failure
there must not erase the published per-DUID factor panels (``fcas_participation``
/ ``offers`` / ``offer_curve``) while the core monthly/daily data still looks
fresh. This module provides:

- ``LaneRun`` — per-source run state (status, coverage, as-of, error) that the
  optional lanes record as they execute.
- ``read_last_good`` — load the last-known-good factor history from its cache
  file (the durable S3-07 snapshot member) when a lane cannot produce fresh
  facts. Retained blocks keep their own ``month`` as the as-of.
- ``check_factor_block_continuity`` — publication guard: reject a run that
  would erase a *populated* factor block without a documented reason. A lane
  whose source ran cleanly (``ok``) may attest that a unit genuinely has no
  rows (observed-zero / not-applicable); any other lane state (degraded,
  error, skipped) cannot, so a previously-populated block that the candidate
  frame cannot reproduce blocks the publish.
- manifest build/write/validate/load — a small machine-readable run record
  (``docs/data/run_status.json``) committed with each publish, consumed by the
  existing operator-status channel (the Mac-side staleness check in
  ``src/freshness.py`` surfaces degraded/error lanes as alerts; no new backend).

Status vocabulary (per lane): ``ok`` = fresh facts computed and persisted this
run; ``degraded`` = source failed or delivered nothing new and last-known-good
history was retained (blocks are kept, explicitly marked retained-stale) or the
run is otherwise partially failed; ``error`` = source failed with no last-known
good to retain (nothing was published that depended on it — the continuity
guard rejects when populated blocks would vanish); ``skipped`` = explicit
operator ``--skip-*`` flag, last-known-good retained when the cache exists.

Not-applicable vs observed-zero vs unknown live at the row level (a unit with
no rows in a cleanly-run lane never offered — absence is the source's verdict);
retained-stale is a lane-level state stamped onto the blocks it carried over.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

RUN_STATUS_FILENAME = "run_status.json"
MANIFEST_SCHEMA_VERSION = 1

STATUS_OK = "ok"
STATUS_DEGRADED = "degraded"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"
ALL_STATUSES = (STATUS_OK, STATUS_DEGRADED, STATUS_ERROR, STATUS_SKIPPED)

# Published block key -> the source lane/cache that feeds it.
FACTOR_LANES = {
    "fcas_participation": "fcas_factors",
    "offers": "offer_factors",
    "offer_curve": "offer_curves",
}

# A lane that is not 'ok' cannot attest that a unit's absence is real (the
# source did not run cleanly), so absence of a previously-populated block is
# only ever allowed for an 'ok' lane.
ABSENCE_ALLOWED_STATUSES = (STATUS_OK,)

_UTC = timezone.utc


def _now_utc() -> str:
    return datetime.now(_UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class LaneRun:
    """Record of one optional source lane's run state.

    ``frame`` is the final attach frame (fresh merge result, or the retained
    last-known-good history) — the exact frame the JSON writer will use. It is
    never serialized to the manifest; the manifest carries the machine-readable
    summary below it.
    """

    source: str
    block: str | None = None
    status: str = STATUS_OK
    frame: pd.DataFrame | None = None
    retained: bool = False
    attempted_months: int = 0
    months_with_data: int = 0
    error: str | None = None
    note: str | None = None

    def set_ok(self) -> None:
        self.status = STATUS_OK

    def asof_month(self) -> str | None:
        """Newest month present in the attach frame (the data's as-of)."""
        df = self.frame
        if df is None or df.empty or "month" not in df.columns:
            return None
        months = pd.unique(df["month"])
        if not len(months):
            return None
        return str(sorted(str(m) for m in months)[-1])

    def manifest_record(self) -> dict:
        return {
            "status": self.status,
            "retained_last_good": bool(self.retained),
            "attempted_months": int(self.attempted_months),
            "months_with_data": int(self.months_with_data),
            "asof_month": self.asof_month(),
            "error": self.error,
            "note": self.note,
        }


def read_last_good(cache_path: str | Path) -> pd.DataFrame:
    """Load the last-known-good factor history from its cache file.

    Returns an empty DataFrame when the file is missing or empty — the caller
    then knows there is nothing to retain (cold start). Logs the read so a
    degraded run's retention is visible in the pipeline log.
    """
    p = Path(cache_path)
    if not p.exists():
        logger.info("read_last_good: no cache at %s — nothing to retain", p)
        return pd.DataFrame()
    try:
        df = pd.read_feather(p)
    except Exception as e:  # corrupt/unreadable cache must not crash the fallback
        logger.warning("read_last_good: cache %s unreadable (%s) — nothing retained", p, e)
        return pd.DataFrame()
    if df.empty:
        logger.info("read_last_good: cache %s is empty — nothing to retain", p)
        return pd.DataFrame()
    logger.info("read_last_good: retained %d rows from %s", len(df), p)
    return df


def check_factor_block_continuity(
    gen_dir: str | Path,
    lanes: dict[str, LaneRun],
) -> list[str]:
    """Publication guard (S3-08): populated factor blocks must not vanish.

    Compares what is about to be published (the lanes' attach frames) against
    what the currently-published generator files carry. For every unit whose
    published doc has a populated ``fcas_participation``/``offers``/
    ``offer_curve`` block, the candidate frame for that lane must still have
    rows for that DUID — unless the lane ran cleanly (``ok``), in which case
    absence is the source's attested verdict (observed-zero / not-applicable).
    Any other lane state cannot explain a disappearance, so it is a violation:
    the run must be rejected rather than silently erase panels.

    Returns aggregated human-readable violations (empty == publication may
    proceed). Reads only unit files (station docs carry no factor blocks) and
    resolves each file back to its real DUID via the doc's ``duid`` field —
    filenames are sanitized and DUIDs may contain ``/`` or ``#``.
    """
    import collections

    gen_dir = Path(gen_dir)
    if not gen_dir.is_dir():
        return []

    present = {src: lane for src, lane in lanes.items() if lane is not None}
    if not present:
        return []
    by_block = {lane.block: lane for lane in present.values() if lane.block}
    counts: collections.Counter = collections.Counter()
    examples: dict = {}
    total_files = 0
    checked = 0

    for path in sorted(gen_dir.glob("*.json")):
        if path.name.startswith("station_"):
            continue
        total_files += 1
        try:
            old = json.loads(path.read_text())
        except (OSError, ValueError):
            continue  # unreadable old file: nothing authoritative to protect
        duid = old.get("duid")
        if not duid:
            continue
        for block_key, src in FACTOR_LANES.items():
            lane = by_block.get(block_key)
            if lane is None:
                continue
            old_block = old.get(block_key)
            if not (isinstance(old_block, dict) and old_block):
                continue  # nothing populated before — nothing can vanish
            frame = lane.frame
            has_rows = bool(
                frame is not None
                and not getattr(frame, "empty", True)
                and "duid" in getattr(frame, "columns", ())
                and (frame["duid"] == duid).any()
            )
            if has_rows:
                continue
            if lane.status in ABSENCE_ALLOWED_STATUSES:
                continue
            key = (block_key, src, lane.status)
            counts[key] += 1
            examples.setdefault(key, []).append(str(duid))
            checked += 1

    violations: list[str] = []
    for (block_key, src, status), n in counts.items():
        duids = ", ".join(examples[(block_key, src, status)][:5])
        more = f" (+{n - 5} more)" if n > 5 else ""
        violations.append(
            f"would erase populated '{block_key}' from {n} previously-populated "
            f"unit file(s) (e.g. {duids}{more}): source {src} is {status} with no "
            "retained rows — cannot attest the absence; re-run with the source "
            "healthy or restore its last-known-good cache before publishing"
        )
    if violations:
        logger.error("Factor-block continuity guard: %d violation(s)", len(violations))
    else:
        logger.info(
            "Factor-block continuity guard: %d unit files checked — no populated "
            "block would vanish", total_files,
        )
    return violations


def run_result(lanes: dict[str, LaneRun]) -> tuple[str, list[str]]:
    """Overall run result from the recorded lanes: error > degraded > ok."""
    problems: list[str] = []
    for lane in lanes.values():
        if lane is None:
            continue
        if lane.status == STATUS_ERROR:
            problems.append(f"{lane.source}: error — {lane.error or 'no details'}")
        elif lane.status == STATUS_DEGRADED:
            problems.append(
                f"{lane.source}: degraded — {lane.error or 'no fresh data this run'}"
                f"{' (retained last good through ' + str(lane.asof_month() or '?') + ')' if lane.retained else ''}"
            )
    if any(l.status == STATUS_ERROR for l in lanes.values() if l is not None):
        return STATUS_ERROR, problems
    if any(l.status == STATUS_DEGRADED for l in lanes.values() if l is not None):
        return STATUS_DEGRADED, problems
    return STATUS_OK, []


def build_manifest(
    lanes: dict[str, LaneRun],
    *,
    mode: str = "incremental",
    months_back: int | None = None,
    guards: dict | None = None,
    outputs: dict | None = None,
    started_utc: str | None = None,
) -> dict:
    """Assemble the validated machine-readable run record."""
    result, problems = run_result(lanes)
    sources = {
        lane.source: lane.manifest_record()
        for lane in lanes.values()
        if lane is not None
    }
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "run": {
            "started_utc": started_utc or _now_utc(),
            "finished_utc": _now_utc(),
            "mode": mode,
            "months_back": months_back,
            "result": result,
            "problems": problems,
        },
        "sources": sources,
        "guards": dict(guards or {}),
        "outputs": dict(outputs or {}),
    }


def validate_manifest(manifest: dict) -> list[str]:
    """Structural validation of a run-status manifest.

    Returns a list of problems; an empty list means the manifest is well-formed
    (schema_version supported, run block complete, every source record carries
    a valid status and the required fields).
    """
    problems: list[str] = []
    if not isinstance(manifest, dict):
        return ["manifest is not an object"]
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        problems.append(
            f"schema_version {manifest.get('schema_version')!r} unsupported "
            f"(expected {MANIFEST_SCHEMA_VERSION})"
        )
    run = manifest.get("run")
    if not isinstance(run, dict):
        problems.append("run block missing")
    else:
        for key in ("started_utc", "finished_utc", "result"):
            if key not in run:
                problems.append(f"run.{key} missing")
        if run.get("result") not in ALL_STATUSES:
            problems.append(f"run.result {run.get('result')!r} invalid")
    sources = manifest.get("sources")
    if not isinstance(sources, dict):
        problems.append("sources block missing")
    else:
        for name, rec in sources.items():
            if not isinstance(rec, dict):
                problems.append(f"sources.{name} is not an object")
                continue
            status = rec.get("status")
            if status not in ALL_STATUSES:
                problems.append(f"sources.{name}.status {status!r} invalid")
            for key in ("retained_last_good", "attempted_months", "months_with_data"):
                if key not in rec:
                    problems.append(f"sources.{name}.{key} missing")
    return problems


def write_run_manifest(manifest: dict, docs_data_dir: str | Path) -> Path:
    """Write the validated manifest to docs/data/run_status.json.

    Raises ValueError when the manifest fails structural validation — a corrupt
    run record must never reach the operator channel.
    """
    problems = validate_manifest(manifest)
    if problems:
        raise ValueError("refusing to write invalid run-status manifest: " + "; ".join(problems))
    out_dir = Path(docs_data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / RUN_STATUS_FILENAME
    out_path.write_text(json.dumps(manifest, indent=1, sort_keys=True))
    logger.info("Wrote run-status manifest to %s (result=%s)", out_path, manifest["run"]["result"])
    return out_path


def load_run_manifest(path: str | Path) -> dict | None:
    """Load + validate a run-status manifest for the operator channel.

    Returns None when the file is absent or fails validation — consumers treat
    an unreadable manifest as "no run record published" (never a crash and
    never a fabricated healthy state).
    """
    p = Path(path)
    if not p.exists():
        return None
    try:
        manifest = json.loads(p.read_text())
    except (OSError, ValueError) as e:
        logger.warning("run-status manifest %s unreadable: %s", p, e)
        return None
    if validate_manifest(manifest):
        logger.warning("run-status manifest %s failed validation", p)
        return None
    return manifest


def manifest_alerts(manifest_path: str | Path) -> list[str]:
    """Alert strings for the operator-status channel from a run manifest.

    Sources in ``degraded`` or ``error`` produce AEMO-ALERT lines (with the
    retained/as-of detail when last-known-good data was kept). ``skipped``
    lanes are visible in the manifest but are explicit operator intent and do
    not alert. Empty when the manifest is absent/healthy.
    """
    manifest = load_run_manifest(manifest_path)
    if manifest is None:
        return []
    alerts: list[str] = []
    for name, rec in sorted((manifest.get("sources") or {}).items()):
        status = rec.get("status")
        if status not in (STATUS_DEGRADED, STATUS_ERROR):
            continue
        detail = rec.get("error") or ""
        retained = rec.get("retained_last_good")
        asof = rec.get("asof_month")
        if status == STATUS_DEGRADED and retained and asof:
            detail = (detail + " " if detail else "") + (
                f"retaining last-known-good through {asof}"
            )
        base = f"AEMO ALERT: source {name} {status}"
        alerts.append(f"{base} — {detail}" if detail else base)
    return alerts
