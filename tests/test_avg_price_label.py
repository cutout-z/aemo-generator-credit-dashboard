"""The "regional average" is over the unit's own intervals; say so (L5).

aggregate_month takes mean(RRP) over the intervals a unit reported SCADA, so
units with gaps differ from the region (Aug 2026: ERB02 $72.14 vs $75.71 for
116 of 122 NSW units). The page called it the regional time-weighted average.
"""

from tests.page_js import run

ROOT = __import__("pathlib").Path(__file__).resolve().parent.parent


def test_export_header_names_the_basis():
    rows = run(["buildMonthlyRows"], """buildMonthlyRows({monthly: {
        months: ['2026-08'], generation_mwh: [1], revenue_aud: [1], capacity_factor: [0.1],
        captured_price: [70], avg_rrp: [72.14], price_capture_ratio: [0.97]}})""")
    assert rows[0]["Avg Regional RRP, unit intervals ($/MWh)"] == 72.14
    assert "Avg Regional RRP ($/MWh)" not in rows[0]


def test_price_capture_copy_names_the_basis():
    page = (ROOT / "docs" / "index.html").read_text()
    assert "'Regional average (unit intervals)'" in page
    assert "simple time-weighted regional average" not in page
    assert "over the intervals this unit reported SCADA" in page
