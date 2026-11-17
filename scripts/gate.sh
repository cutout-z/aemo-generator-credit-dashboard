#!/bin/bash
# The merge gate: what has to pass before a change reaches main (the landing script runs
# `bash scripts/gate.sh` from a fresh worktree of the PR merged onto main).
#
#   bash scripts/gate.sh
#
# Setup (any failure prints `gate: setup failed: <why>` and exits 2 before a leg runs):
#   1. Restore data/ from the tracked processed cache (docs/data/processed-cache/**) with
#      src/processed_cache.restore_processed_cache: it fills gaps only, never overwrites a file
#      already in data/, and never writes into docs/. A fresh clone has no data/ (gitignored), so
#      without this tests/test_outputs.py fails on FileNotFoundError.
#   2. Build a venv at .venv/gate from requirements.txt (+ requirements-dev.txt if present): the
#      versions the NAS lane runs, not whatever the base python has. Reused while the
#      requirements files are unchanged; pip's normal cache keeps a rebuild near 20 s.
#   3. Serve docs/ and the repo root with `python3 -m http.server` on free ports (killed by PID on exit).
# Legs (all run even after one fails; each failure prints an id the landing script parses):
#   pytest   tests/ under the venv, `-rfEs` (FAILED/ERROR ids and skip reasons).
#   verify:verify-design.py        design checks in a real browser (proof page + #ADPBA1).
#   verify:verify-interactions.py  the interactions a change must not lose.
# The browser legs run under the base python3 (Playwright and its Chromium live there, not in
# requirements.txt).
#
# NEEDS THE NETWORK: a cold pip cache downloads the pinned wheels, and the page loads cdn.plot.ly,
# cdn.sheetjs.com and https://cutout-z.github.io/aemo-dashboards/nav.js. Offline, setup or the
# browser legs fail; they do not pass quietly.
#
# Overrides: GATE_PYTHON (base interpreter, default python3), GATE_VENV (default .venv/gate).
# Written for /bin/bash 3.2.

set -u
set -o pipefail
cd "$(dirname "$0")/.." || exit 2
ROOT=$(pwd)
PY=${GATE_PYTHON:-python3}
VENV=${GATE_VENV:-$ROOT/.venv/gate}
export PIP_DISABLE_PIP_VERSION_CHECK=1

TMP=$(mktemp -d "${TMPDIR:-/tmp}/aemo-credit-gate.XXXXXX") || { echo "gate: setup failed: mktemp"; exit 2; }
PIDS=""
cleanup() {
  for pid in $PIDS; do kill "$pid" 2>/dev/null; done
  rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

setup_fail() { echo "gate: setup failed: $*"; exit 2; }
prefix() { sed "s/^/$1| /"; }

# ── setup 1: data/ from the processed cache ──────────────────────────────────
t0=$SECONDS
"$PY" - <<'EOF' 2>&1 | prefix setup || setup_fail "restore data/ from docs/data/processed-cache"
import sys
from pathlib import Path
sys.path.insert(0, ".")
from src.processed_cache import SNAPSHOT_FILES, restore_processed_cache, snapshot_dir

docs = Path("docs/data")
missing = [n for n in SNAPSHOT_FILES if not (snapshot_dir(docs) / n).exists()]
if missing:
    sys.exit(f"tracked snapshot incomplete, missing docs/data/processed-cache/{missing}")
restored = restore_processed_cache(Path("data"), docs)
print(f"restored {len(restored)} file(s) into data/: {restored}")
absent = [n for n in SNAPSHOT_FILES if not (Path("data") / n).exists()]
if absent:
    sys.exit(f"data/ still lacks {absent} after restore")
EOF
echo "gate: setup data $((SECONDS - t0))s"

# ── setup 2: the pinned venv ─────────────────────────────────────────────────
t0=$SECONDS
REQS="requirements.txt"
[ -f requirements-dev.txt ] && REQS="$REQS requirements-dev.txt"
want=$( { "$PY" -c 'import sys; print(sys.version)'; cat $REQS; } | shasum | cut -d' ' -f1)
if [ ! -x "$VENV/bin/python" ] || [ "$(cat "$VENV/.gate-stamp" 2>/dev/null)" != "$want" ]; then
  rm -rf "$VENV"
  "$PY" -m venv "$VENV" 2>&1 | prefix setup || setup_fail "python3 -m venv $VENV"
  pipargs=""
  for r in $REQS; do pipargs="$pipargs -r $r"; done
  # shellcheck disable=SC2086
  "$VENV/bin/python" -m pip install -q $pipargs 2>&1 | prefix setup \
    || setup_fail "pip install $pipargs (needs the network on a cold pip cache)"
  echo "$want" > "$VENV/.gate-stamp"
fi
"$VENV/bin/python" -c 'import pandas, pyarrow, numpy, pytest
print("venv: pandas", pandas.__version__, "pyarrow", pyarrow.__version__, "numpy", numpy.__version__, "pytest", pytest.__version__)' 2>&1 | prefix setup \
  || setup_fail "venv imports"
echo "gate: setup venv $((SECONDS - t0))s"

# ── setup 3: browser prerequisites and servers on free ports ─────────────────
"$PY" -c 'import playwright.sync_api' 2>&1 | prefix setup || setup_fail "playwright missing from $PY"
command -v node >/dev/null 2>&1 || setup_fail "node not on PATH (tests/page_js.py would skip every page-JS test)"

serve() {  # serve <name> <dir>: start http.server on a free port, set PORT
  local log="$TMP/$1.log" i=0
  "$PY" -u -m http.server 0 --bind 127.0.0.1 --directory "$2" >"$log" 2>&1 &
  PIDS="$PIDS $!"
  PORT=""
  while [ $i -lt 100 ]; do
    PORT=$(sed -n 's/^Serving HTTP on [0-9.]* port \([0-9]*\).*/\1/p' "$log" | head -1)
    [ -n "$PORT" ] && return 0
    sleep 0.1; i=$((i + 1))
  done
  prefix setup <"$log"
  setup_fail "http.server for $2 did not start"
}
serve docs "$ROOT/docs"; DOCS_BASE="http://127.0.0.1:$PORT/"
serve root "$ROOT"; PROOF_BASE="http://127.0.0.1:$PORT/"
echo "gate: serving docs/ at $DOCS_BASE, repo root at $PROOF_BASE"

# ── legs ─────────────────────────────────────────────────────────────────────
FAILED=0

t0=$SECONDS
"$VENV/bin/python" -m pytest -q -rfEs -p no:cacheprovider tests
rc=$?
echo "gate: pytest exit $rc, $((SECONDS - t0))s"
if [ $rc -ne 0 ]; then
  FAILED=$((FAILED + 1))
  # rc 1 means test failures, already listed as FAILED/ERROR ids; anything else
  # (usage error, no tests collected, interrupted) gets the leg's own id.
  [ $rc -ne 1 ] && echo "   FAIL  pytest:tests"
fi

run_verify() {  # run_verify <script> <args...>
  local script=$1 t=$SECONDS
  shift
  "$PY" "scripts/$script" "$@" 2>&1 | prefix "$script"
  local rc=$?
  echo "gate: $script exit $rc, $((SECONDS - t))s"
  if [ $rc -ne 0 ]; then
    FAILED=$((FAILED + 1))
    echo "   FAIL  verify:$script"
  fi
}
run_verify verify-design.py --proof-base "$PROOF_BASE" --docs-base "$DOCS_BASE" --dashboard ADPBA1
run_verify verify-interactions.py --base "$DOCS_BASE"

if [ $FAILED -ne 0 ]; then
  echo "gate: FAILED $FAILED leg(s)"
  exit 1
fi
echo "gate: OK (pytest, verify-design.py, verify-interactions.py)"
