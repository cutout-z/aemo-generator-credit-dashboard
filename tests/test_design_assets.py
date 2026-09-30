"""The design layer's published assets match their sources.

GitHub Pages serves docs/ with no build step, so the compiled CSS and the chart-token bridge are
committed copies. scripts/build-css.sh writes both; this catches a source edit that was never
republished (the page would silently keep the old behaviour).
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_chart_tokens_published_copy_matches_source():
    src = ROOT / "assets" / "js" / "chart-tokens.js"
    out = ROOT / "docs" / "assets" / "js" / "chart-tokens.js"
    assert out.exists(), "docs/assets/js/chart-tokens.js missing — run ./scripts/build-css.sh"
    assert out.read_bytes() == src.read_bytes(), (
        "docs/assets/js/chart-tokens.js is stale — run ./scripts/build-css.sh and commit it"
    )


def test_compiled_css_is_published():
    css = ROOT / "docs" / "assets" / "app.css"
    assert css.exists() and css.stat().st_size > 0, "docs/assets/app.css missing — run ./scripts/build-css.sh"
