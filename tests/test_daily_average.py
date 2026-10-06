"""The daily-chart average counts zero-output days (audit 2026-10, L11).

It averaged only days with output, so an intermittently run unit read as its
running-day output: LNGS1 330 MWh/day against a true 6 MWh/day.
"""

from tests.page_js import function_source, run


def test_zero_days_count_and_missing_days_do_not():
    out = run(["dailyAverage"], "[dailyAverage([0, 0, 0, 90, null]), dailyAverage([]), dailyAverage([null])]")
    assert out == [22.5, 0, 0]


def test_daily_chart_uses_it_for_both_lines():
    src = function_source("renderCharts")
    assert "dailyAverage(genMwh)" in src and "dailyAverage(cfValues)" in src
    assert "v != null && v > 0" not in src
