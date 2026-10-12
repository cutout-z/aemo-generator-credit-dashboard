"""deploy/run-update.sh runs one lane at a time per clone (NAS incident 2026-10-12).

The daily/reference/constraints/mlf lanes share one clone, venv and nemosis
cache, and the NAS scheduler only locks per job name. The script now takes a
flock on the clone before any git or pipeline step; a second run waits, then
exits 75 without touching git. Needs util-linux flock (the NAS has it;
macOS does not, so the test skips there).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "deploy" / "run-update.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("flock") is None or shutil.which("bash") is None,
    reason="util-linux flock not installed",
)


def _env(tmp_path: Path, wait: int) -> dict:
    app = tmp_path / "app"
    (app / ".git").mkdir(parents=True)
    stub = tmp_path / "bin"
    stub.mkdir()
    # A git that only records it was called: the guard must stop before git.
    (stub / "git").write_text(f'#!/bin/sh\ntouch "{tmp_path}/git-called"\nexit 1\n')
    (stub / "git").chmod(0o755)
    return {
        **os.environ,
        "PATH": f"{stub}:{os.environ['PATH']}",
        "APP_DIR": str(app),
        "RUN_LOCK_WAIT_SECONDS": str(wait),
    }


def test_second_run_gives_up_without_touching_git(tmp_path):
    env = _env(tmp_path, wait=1)
    lock = Path(env["APP_DIR"]) / ".git" / "run-update.lock"
    holder = subprocess.Popen(["flock", str(lock), "sleep", "10"])
    try:
        time.sleep(0.5)
        run = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=30)
    finally:
        holder.kill()
        holder.wait()
    assert run.returncode == 75, run.stderr
    assert "SKIP" in run.stderr
    assert not (tmp_path / "git-called").exists()


def test_waiting_run_continues_once_lock_frees(tmp_path):
    env = _env(tmp_path, wait=20)
    lock = Path(env["APP_DIR"]) / ".git" / "run-update.lock"
    holder = subprocess.Popen(["flock", str(lock), "sleep", "2"])
    try:
        time.sleep(0.5)
        run = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60)
    finally:
        holder.wait()
    assert "Lock acquired; continuing." in run.stdout
    assert (tmp_path / "git-called").exists()  # proceeded to the git step
