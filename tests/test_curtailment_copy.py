"""The curtailment copy names the baseline the code uses (audit 2026-10, M5).

aggregate_month divides by DISPATCHLOAD AVAILABILITY. The page's economic
curtailment formula and the README said UIGF, a different series.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _econ_panel() -> str:
    html = (ROOT / "docs" / "index.html").read_text()
    start = html.index('id="panelEconCurtailment"')
    return html[start:html.index('id="chartEconCurtailment"', start)]


def test_page_economic_curtailment_formula_uses_availability():
    panel = _econ_panel()
    formula = re.search(r'tooltip-formula">([^<]*(?:&lt;[^<]*)*)</div>', panel).group(1)
    assert "AVAILABILITY" in formula
    assert "UIGF" not in panel


def test_readme_curtailment_rows_use_availability():
    readme = (ROOT / "README.md").read_text()
    rows = [line for line in readme.splitlines()
            if line.startswith("| **") and "urtailment" in line]
    assert rows, "README methodology table lost its curtailment rows"
    for line in rows:
        assert "AVAILABILITY" in line, line
        assert "UIGF" not in line, line
