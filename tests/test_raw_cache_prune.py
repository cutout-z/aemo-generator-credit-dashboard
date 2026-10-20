"""deploy/prune-raw-cache.sh and its place in deploy/run-update.sh.

The nemosis cache kept every raw month CSV beside its parquet (~33 GB of a
~38 GB cache, 2026-10). nemosis reads the parquet when it exists, so the
CSV-duplicate pass deletes a CSV only once its parquet is known good. The mtime
pass must never take the GENCONDATA / SPDCONNECTIONPOINTCONSTRAINT month files
that every constraints refresh recompiles from 2009. The prune runs after the
publish (and on no-change days) and can never fail the run.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
PRUNE = ROOT / "deploy" / "prune-raw-cache.sh"
RUN_UPDATE = ROOT / "deploy" / "run-update.sh"

HOURS = 3600
DAYS = 24 * HOURS


def _age(path: Path, seconds: float) -> None:
    t = time.time() - seconds
    os.utime(path, (t, t))


def _csv(cache: Path, stem: str, age: float = 7 * HOURS) -> Path:
    p = cache / f"{stem}.CSV"
    p.write_text("I,DISPATCH,PRICE,1\nD,DISPATCH,PRICE,1,2026/01/01 00:05:00,NSW1,100\n" * 50)
    _age(p, age)
    return p


def _parquet(cache: Path, stem: str, rows: int = 3, cols=None, age: float = 7 * HOURS) -> Path:
    p = cache / f"{stem}.parquet"
    cols = cols or ["SETTLEMENTDATE", "REGIONID", "RRP"]
    pq.write_table(pa.table({c: list(range(rows)) for c in cols}), p)
    _age(p, age)
    return p


def _filler(cache: Path, n: int) -> None:
    """Parquets with no CSV sibling, so the cache looks populated."""
    for i in range(n):
        _parquet(cache, f"PUBLIC_DVD_FILLER_{i:04d}01010000")


def _prune(tmp_path: Path, **env) -> subprocess.CompletedProcess:
    full = {
        **os.environ,
        "APP_DIR": str(tmp_path / "app"),
        "PYTHON": sys.executable,
        "CACHE_ROOT": str(tmp_path / "data"),
        "NEMOSIS_CACHE_DIR": str(tmp_path / "data" / "nemosis_cache"),
        **{k: str(v) for k, v in env.items()},
    }
    run = subprocess.run(["bash", str(PRUNE)], env=full, capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stdout + run.stderr
    return run


@pytest.fixture
def cache(tmp_path: Path) -> Path:
    c = tmp_path / "data" / "nemosis_cache"
    c.mkdir(parents=True)
    return c


# --- CSV-duplicate pass -------------------------------------------------------

def test_deletes_csv_with_good_parquet(tmp_path, cache):
    stem = "PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#202601010000"
    csv = _csv(cache, stem)
    _parquet(cache, stem)
    _filler(cache, 1)
    run = _prune(tmp_path, PRUNE_MIN_PARQUETS=1)
    assert not csv.exists()
    assert (cache / f"{stem}.parquet").exists()
    assert "CSV prune: deleted 1 CSV files" in run.stdout


def test_keeps_csv_when_parquet_missing_empty_corrupt_or_rowless(tmp_path, cache):
    missing = _csv(cache, "PUBLIC_DVD_DISPATCHLOAD_202301010000")
    empty = _csv(cache, "PUBLIC_DVD_DISPATCHLOAD_202302010000")
    (cache / "PUBLIC_DVD_DISPATCHLOAD_202302010000.parquet").write_bytes(b"")
    corrupt = _csv(cache, "PUBLIC_DVD_DISPATCHLOAD_202303010000")
    good = _parquet(cache, "PUBLIC_DVD_DISPATCHLOAD_202303010000")
    good.write_bytes(good.read_bytes()[:-20])  # truncated mid-write: no footer
    rowless = _csv(cache, "PUBLIC_DVD_DISPATCHLOAD_202304010000")
    _parquet(cache, "PUBLIC_DVD_DISPATCHLOAD_202304010000", rows=0)
    _filler(cache, 3)
    run = _prune(tmp_path, PRUNE_MIN_PARQUETS=1)
    for p in (missing, empty, corrupt, rowless):
        assert p.exists(), p.name
    assert "deleted 0 CSV files" in run.stdout


def test_keeps_bidperoffer_csv_when_parquet_is_thin(tmp_path, cache):
    """A thin BIDPEROFFER_D parquet triggers the heal, which re-decodes the CSV."""
    thin = "PUBLIC_ARCHIVE#BIDPEROFFER_D#FILE01#202601010000"
    fat = "PUBLIC_ARCHIVE#BIDPEROFFER_D#FILE01#202602010000"
    from src.download_bids import BIDPEROFFER_REQUIRED_COLS

    thin_csv = _csv(cache, thin)
    _parquet(cache, thin, cols=["INTERVAL_DATETIME", "DUID", "BIDTYPE"])
    fat_csv = _csv(cache, fat)
    _parquet(cache, fat, cols=sorted(BIDPEROFFER_REQUIRED_COLS))
    _prune(tmp_path, PRUNE_MIN_PARQUETS=1)
    assert thin_csv.exists()
    assert not fat_csv.exists()


def test_required_cols_mirror_download_bids():
    from src.download_bids import BIDPEROFFER_REQUIRED_COLS

    text = PRUNE.read_text()
    for col in BIDPEROFFER_REQUIRED_COLS - {f"BANDAVAIL{i}" for i in range(1, 11)}:
        assert f'"{col}"' in text, col
    assert 'f"BANDAVAIL{i}" for i in range(1, 11)' in text


def test_keeps_young_csv(tmp_path, cache):
    stem = "PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#202609010000"
    csv = _csv(cache, stem, age=1 * HOURS)
    _parquet(cache, stem)
    run = _prune(tmp_path, PRUNE_MIN_PARQUETS=1)
    assert csv.exists()
    assert "1 younger than min age" in run.stdout


def test_refuses_on_near_empty_cache(tmp_path, cache):
    stem = "PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#202601010000"
    csv = _csv(cache, stem)
    _parquet(cache, stem)
    _filler(cache, 50)  # 51 parquets < default 100
    run = _prune(tmp_path)
    assert csv.exists()
    assert "WARN: CSV prune refused" in run.stdout


def test_refuses_when_cache_dir_missing(tmp_path):
    run = _prune(tmp_path)
    assert "WARN: CSV prune refused" in run.stdout
    assert "is missing" in run.stdout


def test_default_threshold_allows_a_populated_cache(tmp_path, cache):
    stem = "PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#202601010000"
    csv = _csv(cache, stem)
    _parquet(cache, stem)
    _filler(cache, 99)  # 100 parquets
    _prune(tmp_path)
    assert not csv.exists()


def test_respects_delete_cap(tmp_path, cache):
    csvs = []
    for m in range(1, 6):
        stem = f"PUBLIC_DVD_DISPATCHLOAD_2023{m:02d}010000"
        csvs.append(_csv(cache, stem))
        _parquet(cache, stem)
    run = _prune(tmp_path, PRUNE_MIN_PARQUETS=1, PRUNE_MAX_CSV_DELETE=2)
    assert sum(not p.exists() for p in csvs) == 2
    assert "hit PRUNE_MAX_CSV_DELETE=2; 3 more" in run.stdout


def test_csv_pass_never_touches_constraint_reference_tables(tmp_path, cache):
    kept = []
    for stem in (
        "PUBLIC_DVD_GENCONDATA_201001010000",
        "PUBLIC_ARCHIVE#GENCONDATA#FILE01#202501010000",
        "PUBLIC_DVD_SPDCONNECTIONPOINTCONSTRAINT_201001010000",
        "PUBLIC_ARCHIVE#SPDCONNECTIONPOINTCONSTRAINT#FILE01#202501010000",
    ):
        kept.append(_csv(cache, stem))
        _parquet(cache, stem)
    _prune(tmp_path, PRUNE_MIN_PARQUETS=1)
    for p in kept:
        assert p.exists(), p.name


def test_raw_csv_prune_off(tmp_path, cache):
    stem = "PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#202601010000"
    csv = _csv(cache, stem)
    _parquet(cache, stem)
    run = _prune(tmp_path, PRUNE_MIN_PARQUETS=1, RAW_CSV_PRUNE=0)
    assert csv.exists()
    assert "skipping CSV-duplicate prune" in run.stdout


def test_csv_pass_failure_does_not_stop_mtime_pass(tmp_path, cache):
    old = cache / "PUBLIC_DVD_DISPATCHPRICE_202001010000.parquet"
    old.write_bytes(b"x")
    _age(old, 200 * DAYS)
    run = _prune(tmp_path, PYTHON="/nonexistent/python")
    assert "WARN: CSV-duplicate prune failed" in run.stdout
    assert not old.exists()


# --- mtime pass ----------------------------------------------------------------

def test_mtime_pass_skips_gencondata_and_spdcp(tmp_path, cache):
    data = cache.parent
    protected = [
        cache / "PUBLIC_DVD_GENCONDATA_201001010000.parquet",
        cache / "PUBLIC_DVD_GENCONDATA_201001010000.CSV",
        cache / "PUBLIC_ARCHIVE#GENCONDATA#FILE01#202501010000.parquet",
        cache / "PUBLIC_DVD_SPDCONNECTIONPOINTCONSTRAINT_201001010000.parquet",
        cache / "PUBLIC_ARCHIVE#SPDCONNECTIONPOINTCONSTRAINT#FILE01#202501010000.parquet",
        data / "gencondata.feather",
        data / "spdcp_constraint_versions.feather",
    ]
    doomed = [
        cache / "PUBLIC_DVD_DISPATCHPRICE_202001010000.parquet",
        cache / "PUBLIC_ARCHIVE#DISPATCHLOAD#FILE01#202408010000.CSV",
        data / "intermittent_2024_01.feather",
    ]
    for p in protected + doomed:
        p.write_bytes(b"x")
        _age(p, 200 * DAYS)
    _prune(tmp_path, RAW_CSV_PRUNE=0)
    for p in protected:
        assert p.exists(), p.name
    for p in doomed:
        assert not p.exists(), p.name


# --- run-update.sh ordering ----------------------------------------------------

def _lane(tmp_path: Path, *, changed: bool, prune_rc: int = 0, prune_on: str = "1"):
    app = tmp_path / "app"
    (app / ".git").mkdir(parents=True)
    (app / "deploy").mkdir()
    log = tmp_path / "calls.log"
    stub = tmp_path / "bin"
    stub.mkdir()

    def script(path: Path, body: str) -> None:
        path.write_text("#!/bin/sh\n" + body)
        path.chmod(0o755)

    script(stub / "flock", "exit 0\n")
    script(
        stub / "git",
        f'echo "git $*" >> "{log}"\n'
        f'if [ "$1" = diff ]; then exit {0 if not changed else 1}; fi\n'
        "exit 0\n",
    )
    script(stub / "python", f'echo "python $*" >> "{log}"\nexit 0\n')
    script(app / "deploy" / "prune-raw-cache.sh", f'echo "prune" >> "{log}"\nexit {prune_rc}\n')
    env = {
        **os.environ,
        "PATH": f"{stub}:{os.environ['PATH']}",
        "APP_DIR": str(app),
        "PYTHON": str(stub / "python"),
        "RUN_RAW_CACHE_PRUNE": prune_on,
    }
    run = subprocess.run(["bash", str(RUN_UPDATE)], env=env, capture_output=True, text=True, timeout=60)
    calls = log.read_text().splitlines() if log.exists() else []
    return run, calls


def test_prune_runs_after_push(tmp_path):
    run, calls = _lane(tmp_path, changed=True)
    assert run.returncode == 0, run.stdout + run.stderr
    assert "git push origin main" in calls
    assert calls.index("prune") > calls.index("git push origin main")
    assert calls[-1] == "prune"


def test_prune_runs_on_no_change_day(tmp_path):
    run, calls = _lane(tmp_path, changed=False)
    assert run.returncode == 0, run.stdout + run.stderr
    assert "No publishable docs/data changes." in run.stdout
    assert "prune" in calls
    assert not any(c.startswith("git commit") for c in calls)


def test_prune_failure_is_not_fatal_and_publish_still_happens(tmp_path):
    run, calls = _lane(tmp_path, changed=True, prune_rc=3)
    assert run.returncode == 0, run.stdout + run.stderr
    assert "git push origin main" in calls
    assert "WARN: raw-cache prune failed (exit 3)" in run.stdout


def test_prune_off_skips_it(tmp_path):
    run, calls = _lane(tmp_path, changed=False, prune_on="0")
    assert run.returncode == 0
    assert "prune" not in calls
