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


def test_page_has_no_raw_colours():
    """Rule 1 of design/design-tokens.md: colours come from the tokens, never literals in the page.
    A literal colour is also exactly what breaks the light/dark flip."""
    import re

    html = (ROOT / "docs" / "index.html").read_text()
    hexes = [m.group(0) for m in re.finditer(r"(?<![&\w])#[0-9a-fA-F]{3,8}\b", html)]
    funcs = re.findall(r"\b(?:rgba?|hsla?)\(", html)
    assert not hexes and not funcs, f"raw colours in docs/index.html: {hexes[:5]} {funcs[:5]}"
