"""README claims that drift from the code fail here (audit 2026-10, M12).

The README described a retired grid/mechanical curtailment split, a revenue
bar chart and a "Mechanical Outage" panel that do not exist, UIGF where the
code uses AVAILABILITY, an MLF range of 0.95-1.00 (FY26-27 runs 0.82-1.14),
a 6-bin price histogram (22 bins), "6 files, 56 tests" (47 test files) and
modules that no longer exist (download_mlf.py, download_draft_mlf.py).
"""

import re
from pathlib import Path

from src import config

ROOT = Path(__file__).resolve().parent.parent
README = (ROOT / "README.md").read_text()


def test_no_retired_curtailment_split_or_panels():
    for phrase in ("Grid Curtailment", "Mechanical Outage", "grid vs. mechanical",
                   "Implied 100% Merchant Revenue** — monthly bar chart"):
        assert phrase not in README, phrase


def test_no_hard_coded_test_counts():
    assert not re.search(r"\d+ tests\b", README)
    assert not re.search(r"\d+[- ]files?(?: test suite|,)", README)


def test_project_structure_names_existing_modules():
    named = set(re.findall(r"│   [├└]── (\w+\.py)", README))
    assert named, "project structure block lost"
    missing = [m for m in named if not (ROOT / "src" / m).exists()]
    assert not missing, missing
    undocumented = [p.name for p in (ROOT / "src").glob("*.py")
                    if p.name not in named and p.name not in {
                        "__init__.py", "download_intermittent.py", "factor_cache.py",
                        "interval_days.py", "semantic_publish.py"}]
    assert not undocumented, undocumented


def test_price_bin_count_matches_config():
    m = re.search(r"histogram across (\d+) bins", README)
    assert m and int(m.group(1)) == len(config.PRICE_BIN_LABELS)


def test_panel_list_matches_the_page():
    page = (ROOT / "docs" / "index.html").read_text()
    titles = set(re.findall(r"<h3>([^<]+)</h3>", page))
    block = README[README.index("### Panels (as on the page)"):README.index("## Running Locally")]
    listed = re.findall(r"^\d+\. \*\*([^*]+)\*\*", block, flags=re.M)
    assert listed
    for name in listed:
        if name == "KPI row":
            continue
        assert name in titles, name


def test_mlf_range_is_not_the_old_claim():
    assert "0.95–1.00" not in README
