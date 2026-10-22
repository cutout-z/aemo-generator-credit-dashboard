"""Data commits are labelled with the NEM date, not the UTC date (code pass 2026-10-20, R3 L).

The daily lane runs at 05:15 AEST, which is 19:15 UTC the day before, so a
`date -u` label named the previous day. The label's command is evaluated here
under a stub `date` that answers as `date` would at that moment.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "deploy" / "run-update.sh"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")

# 2026-10-22 05:15 AEST == 2026-10-21 19:15 UTC.
_STUB_DATE = """#!/bin/sh
case " $* " in *" -u "*) echo 2026-10-21; exit 0;; esac
case "$TZ" in AEST-10) echo 2026-10-22;; *) echo 2026-10-21;; esac
"""


def _label_command() -> str:
    line = next(l for l in SCRIPT.read_text().splitlines() if l.startswith("git commit -m"))
    return re.search(r"\$\((.*)\)", line).group(1)


def test_commit_label_is_the_nem_date(tmp_path):
    stub = tmp_path / "date"
    stub.write_text(_STUB_DATE)
    stub.chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "TZ": "UTC"}
    out = subprocess.run(["bash", "-c", _label_command()], env=env,
                         capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "2026-10-22"
