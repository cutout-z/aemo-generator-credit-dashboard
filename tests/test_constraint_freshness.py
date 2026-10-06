"""Constraint data freshness is visible in the run manifest (audit 2026-10, M7).

Every lane passed --skip-constraints, so constraint_aggregates stopped at
2026-03 while generation reached 2026-08, and run_status.json said nothing.
"""

import pandas as pd

from src import main as pipeline
from src.run_status import STATUS_DEGRADED, STATUS_OK, STATUS_SKIPPED, build_manifest, manifest_alerts


def _frames(constraint_last="2026-03", gen_last="2026-08"):
    cons = pd.DataFrame({"duid": ["A", "A"], "month": ["2026-02", constraint_last],
                         "constraint_id": ["X", "X"], "hours_bound": [1.0, 1.0]})
    gen = pd.DataFrame({"duid": ["A", "A"], "month": ["2026-07", gen_last]})
    return cons, gen


def test_skipped_and_five_months_stale_is_degraded_with_the_as_of_month():
    cons, gen = _frames()
    lane = pipeline._constraint_lane(cons, gen, skipped=True)
    assert lane.status == STATUS_DEGRADED
    assert "ends 2026-03" in lane.error and "5 months behind" in lane.error
    rec = lane.manifest_record()
    assert rec["asof_month"] == "2026-03"


def test_stale_constraints_raise_an_operator_alert(tmp_path):
    cons, gen = _frames()
    manifest = build_manifest({"constraints": pipeline._constraint_lane(cons, gen, skipped=True)})
    path = tmp_path / "run_status.json"
    import json
    path.write_text(json.dumps(manifest))
    alerts = manifest_alerts(path)
    assert any("constraints degraded" in a and "2026-03" in a for a in alerts)


def test_current_constraints_are_ok_or_skipped():
    cons, gen = _frames(constraint_last="2026-07")
    assert pipeline._constraint_lane(cons, gen, skipped=False).status == STATUS_OK
    assert pipeline._constraint_lane(cons, gen, skipped=True).status == STATUS_SKIPPED


def test_incremental_constraint_months_catch_up_after_a_gap():
    window = [(2024, m) for m in range(10, 13)] + [(2025, m) for m in range(1, 13)] + \
             [(2026, m) for m in range(1, 10)]
    months = pipeline._constraint_months_to_process(window, "2026-03", 2)
    assert months == [(2026, m) for m in range(4, 10)]
    # Up to date: just the overlap window.
    assert pipeline._constraint_months_to_process(window, "2026-09", 2) == [(2026, 8), (2026, 9)]
    # Nothing cached: the whole window.
    assert pipeline._constraint_months_to_process(window, None, 2) == window
