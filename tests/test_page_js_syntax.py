"""Whole-page JS syntax smoke: ``node --check`` on every script the page runs.

``page_js`` extracts only named functions, so a SyntaxError elsewhere in
docs/index.html (or in the assets it loads) would blank the public page with
every other test green. This parses all of it: each inline ``<script>`` body
and each local ``<script src>`` asset. Remote scripts (plotly, sheetjs, nav.js)
are not fetched. Skipped visibly when node is absent (the NAS lane).
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
HTML = (DOCS / "index.html").read_text(encoding="utf-8")
SCRIPT_RE = re.compile(r"<script\b([^>]*)>(.*?)</script>", re.S | re.I)

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")


def _scripts():
    """(label, source, is_module) for inline scripts and local src files."""
    out = []
    for i, (attrs, body) in enumerate(SCRIPT_RE.findall(HTML)):
        src = re.search(r'\bsrc\s*=\s*"([^"]+)"', attrs)
        is_module = bool(re.search(r'\btype\s*=\s*"module"', attrs))
        if src:
            if re.match(r"[a-z]+://|//", src.group(1)):
                continue
            path = DOCS / src.group(1)
            out.append((src.group(1), path.read_text(encoding="utf-8"), is_module))
        elif body.strip():
            out.append((f"inline#{i}", body, is_module))
    return out


def _check(source: str, tmp_path: Path, is_module: bool) -> subprocess.CompletedProcess:
    f = tmp_path / ("s.mjs" if is_module else "s.js")
    f.write_text(source, encoding="utf-8")
    return subprocess.run(["node", "--check", str(f)], capture_output=True, text=True, timeout=30)


def test_page_has_scripts_to_check():
    labels = [label for label, _, _ in _scripts()]
    assert sum(l.startswith("inline#") for l in labels) >= 2
    assert "assets/js/portfolio.js" in labels and "assets/js/chart-tokens.js" in labels


def test_every_page_script_parses(tmp_path):
    failures = []
    for label, source, is_module in _scripts():
        r = _check(source, tmp_path, is_module)
        if r.returncode != 0:
            failures.append(f"{label}: {r.stderr.strip().splitlines()[:6]}")
    assert not failures, "SyntaxError in page JS:\n" + "\n".join(failures)


def test_syntax_check_detects_a_broken_script(tmp_path):
    """The checker itself must fail on a SyntaxError (guards a no-op smoke)."""
    assert _check("function ok() { return 1 }", tmp_path, False).returncode == 0
    assert _check("function broken( { return 1 }", tmp_path, False).returncode != 0
