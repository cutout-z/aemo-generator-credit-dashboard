"""deploy/run-update.sh runs one lane at a time per clone (NAS incident 2026-10-12).

The daily/reference/constraints/mlf lanes share one clone, venv and nemosis
cache, and the NAS scheduler only locks per job name. The script now takes a
flock on the clone before any git or pipeline step; a second run waits, then
exits 75 without touching git. Needs util-linux flock (the NAS has it;
macOS does not, so the test skips there).
"""

from __future__ import annotations

import fcntl
import os
import shutil
import subprocess
import threading
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


def _hold_lock(lock: Path):
    """Take the flock on `lock` in this process and return the open file. The lock is held when this
    returns (no start-up race with a child process), and closing the file releases it. flock(2) is the
    same lock the script's flock(1) takes."""
    handle = open(lock, "w")
    fcntl.flock(handle, fcntl.LOCK_EX)
    return handle


def test_second_run_gives_up_without_touching_git(tmp_path):
    env = _env(tmp_path, wait=1)
    lock = Path(env["APP_DIR"]) / ".git" / "run-update.lock"
    holder = _hold_lock(lock)
    try:
        run = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=30)
    finally:
        holder.close()
    assert run.returncode == 75, run.stderr
    assert "SKIP" in run.stderr
    assert not (tmp_path / "git-called").exists()


def test_waiting_run_continues_once_lock_frees(tmp_path):
    env = _env(tmp_path, wait=20)
    lock = Path(env["APP_DIR"]) / ".git" / "run-update.lock"
    holder = _hold_lock(lock)
    run = subprocess.Popen(["bash", str(SCRIPT)], env=env, stdout=subprocess.PIPE, text=True)
    try:
        # The run prints this line only after its non-blocking flock failed, i.e. once it is really waiting.
        # Reading it is the synchronisation; the deadline only bounds a hang.
        killer = threading.Timer(30, run.kill)
        killer.start()
        try:
            first = run.stdout.readline()
        finally:
            killer.cancel()
        assert "Another run holds" in first, f"run never reported waiting: {first!r}"
        holder.close()  # release: the waiting run now gets the lock
        rest, _ = run.communicate(timeout=60)
    finally:
        holder.close()
        if run.poll() is None:
            run.kill()
            run.wait()
    assert "Lock acquired; continuing." in rest
    assert (tmp_path / "git-called").exists()  # proceeded to the git step
