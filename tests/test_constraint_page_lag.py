"""The constraint panel says when its data trails generation (audit 2026-10-07, S1-1).

The panel's window label read "Apr 2024 – Mar 2026" while every other chart ran
to Aug 2026, and the "Most binding system constraint" line had no window at
all. The summary box now dates that line and shows a "Constraint data through
<month>" warning pill when the window ends more than 2 months before the
newest generation month. The month comes from run_status.json
(sources.constraints.asof_month): a unit's own heatmap ends at its last
binding month, not at the end of the constraint data.
"""

from pathlib import Path

from tests.page_js import run

PAGE = Path(__file__).resolve().parent.parent / "docs" / "index.html"


def _lag(asof, gen_last):
    doc = "{monthly: {months: ['2026-01', '%s']}}" % gen_last
    return run(["constraintLagMonth"], f"constraintLagMonth({asof!r}, {doc})")


def test_lag_beyond_two_months_names_the_constraint_as_of_month():
    assert _lag("2026-03", "2026-08") == "2026-03"


def test_two_months_or_less_is_not_flagged():
    assert _lag("2026-06", "2026-08") == ""
    assert _lag("2025-12", "2026-02") == ""


def test_no_manifest_as_of_means_no_pill():
    # Never infer from the unit's own heatmap: it stops at the unit's last
    # binding month, not at the end of the constraint data.
    assert _lag("", "2026-08") == ""
    assert run(["constraintLagMonth"], "constraintLagMonth(null, {constraints: {heatmap: {months: ['2024-01']}}})") == ""


def test_summary_line_and_pill_are_wired():
    page = PAGE.read_text()
    assert "'<b>Most binding system constraint (through ' + (constraintWindowLabel(data)" in page
    assert "const lagMonth = constraintLagMonth(constraintsAsof, data);" in page
    assert "fetch('data/run_status.json')" in page
    assert "'Constraint data through ' + fmtMonthLong(lagMonth)" in page
