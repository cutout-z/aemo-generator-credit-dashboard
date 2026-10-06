"""Run named functions from docs/index.html's inline script under Node.

The page is one static file with one inline <script>; its pure helpers can be
exercised without a browser by extracting their source and evaluating a
snippet with Node. Tests that use this skip when Node is not installed.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

PAGE = Path(__file__).resolve().parent.parent / "docs" / "index.html"


def function_source(name: str, html: str | None = None) -> str:
    """Source of ``function name(...) {...}`` (brace-matched, strings aware)."""
    html = html if html is not None else PAGE.read_text()
    m = re.search(r"\bfunction\s+" + re.escape(name) + r"\s*\(", html)
    if not m:
        raise LookupError(f"function {name} not found in index.html")
    i = html.index("{", m.end())
    depth, quote, j = 0, None, i
    while j < len(html):
        ch = html[j]
        if quote:
            if ch == "\\":
                j += 2
                continue
            if ch == quote:
                quote = None
        elif html.startswith("//", j):
            j = html.index("\n", j)
            continue
        elif html.startswith("/*", j):
            j = html.index("*/", j) + 2
            continue
        elif ch in "'\"`":
            quote = ch
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return html[m.start(): j + 1]
        j += 1
    raise ValueError(f"unbalanced braces in function {name}")


def run(functions: list[str], expression: str, prelude: str = ""):
    """Evaluate ``expression`` after defining ``functions``; return its JSON value."""
    node = shutil.which("node")
    if not node:
        pytest.skip("node not installed")
    src = "\n".join(function_source(f) for f in functions)
    script = f"{prelude}\n{src}\nprocess.stdout.write(JSON.stringify({expression}));"
    out = subprocess.run([node, "-e", script], capture_output=True, text=True, timeout=30)
    if out.returncode != 0:
        raise AssertionError(out.stderr)
    return json.loads(out.stdout)
