"""Static guard on the page's own nav contract.

The page loads the shared aemo-dashboards nav.js. A removed tag or a mistyped data-current would pass every
other test here, since none of them loads the shared nav. This pins the tag docs/index.html sends: exactly one
script with the shared nav.js URL, and a data-current key the shared nav knows (tests/fixtures/nav_keys.txt).
aemo-dashboards' tests/fixtures/page_nav_keys.txt pins the same key from the other side.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NAV_URL = "https://cutout-z.github.io/aemo-dashboards/nav.js"
HTML = (ROOT / "docs" / "index.html").read_text()
KEYS = {
    line.strip()
    for line in (ROOT / "tests" / "fixtures" / "nav_keys.txt").read_text().splitlines()
    if line.strip() and not line.lstrip().startswith("#")
}


def _script_tags(html):
    return re.findall(r"<script\b[^>]*>", html, re.I)


def _attr(tag, name):
    m = re.search(rf"""\b{name}\s*=\s*(?:"([^"]*)"|'([^']*)')""", tag, re.I)
    return None if not m else (m.group(1) if m.group(1) is not None else m.group(2))


def test_exactly_one_shared_nav_script_tag():
    tags = [t for t in _script_tags(HTML) if _attr(t, "src") == NAV_URL]
    assert len(tags) == 1, f"expected one <script src={NAV_URL}>, found {len(tags)}"
    other = [t for t in _script_tags(HTML) if "nav.js" in (_attr(t, "src") or "") and _attr(t, "src") != NAV_URL]
    assert not other, f"a nav.js script with a different URL: {other}"


def test_data_current_is_this_dashboards_key():
    (tag,) = [t for t in _script_tags(HTML) if _attr(t, "src") == NAV_URL]
    current = _attr(tag, "data-current")
    assert current in KEYS, f"data-current={current!r} is not one of the nav keys {sorted(KEYS)}"
    assert current == "generator-credit", f"this page must send data-current='generator-credit', not {current!r}"
