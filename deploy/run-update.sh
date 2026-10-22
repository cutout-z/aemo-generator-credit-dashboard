#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/workspace/repos/aemo-generator-credit-dashboard}"
PYTHON="${PYTHON:-${APP_DIR}/.venv/bin/python}"
PIPELINE_ARGS="${PIPELINE_ARGS:---months-back 2 --refresh-mlf}"
RUN_TESTS="${RUN_TESTS:-1}"
PUSH_CHANGES="${PUSH_CHANGES:-1}"
RUN_RAW_CACHE_PRUNE="${RUN_RAW_CACHE_PRUNE:-1}"
COMMIT_MESSAGE_PREFIX="${COMMIT_MESSAGE_PREFIX:-Update AEMO generator credit data}"

# One run per clone at a time. The daily, reference, constraints and mlf lanes
# all run this script on the SAME clone, venv and nemosis cache, but the NAS
# scheduler locks per job name, so they could overlap: on 2026-10-12 the
# Monday reference run started while the daily run was still going, and the
# two (13 GB + 3 GB RSS, both swapping) drove the NAS to load 50. A flock is
# released by the kernel when the holder dies, so it cannot go stale. A run
# that cannot get the lock waits, then gives up loudly (exit 75), never in
# parallel.
RUN_LOCK_FILE="${RUN_LOCK_FILE:-${APP_DIR}/.git/run-update.lock}"
RUN_LOCK_WAIT_SECONDS="${RUN_LOCK_WAIT_SECONDS:-7200}"
exec 9>"${RUN_LOCK_FILE}"
if ! flock -n 9; then
  echo "Another run holds ${RUN_LOCK_FILE}; waiting up to ${RUN_LOCK_WAIT_SECONDS}s."
  if ! flock -w "${RUN_LOCK_WAIT_SECONDS}" 9; then
    echo "SKIP: ${RUN_LOCK_FILE} still held after ${RUN_LOCK_WAIT_SECONDS}s; not running in parallel." >&2
    exit 75
  fi
  echo "Lock acquired; continuing."
fi

cd "${APP_DIR}"

git fetch origin main
git checkout main
# Self-heal: this clone only ever holds regeneratable pipeline data commits, so
# when GitHub main has been rewritten (force-push/rebase) a fast-forward becomes
# impossible. Reset onto the fetched remote instead of aborting — a bare
# `git pull --ff-only` under `set -e` exits 128 and stalls the lane.
if ! git pull --ff-only origin main; then
  echo "origin/main is not fast-forwardable (rewritten?) — resetting onto it."
  git reset --hard origin/main
fi

"${PYTHON}" -m src.main ${PIPELINE_ARGS}

if [[ "${RUN_TESTS}" == "1" ]]; then
  # Full suite: test_outputs (post-pipeline validation incl. freshness),
  # settled-history guard, station aggregation, processed cache, intermittent
  # quality. Previously only test_outputs.py ran; the other four existed but
  # were never executed by the daily lane.
  "${PYTHON}" -m pytest tests/ -v
fi

# Raw-cache prune runs AFTER the publish (and on no-change days, which are
# most days), still inside the flock, and can never fail the run: a prune
# error used to abort under `set -e` before `git add`, blocking the publish.
prune_raw_cache() {
  if [[ "${RUN_RAW_CACHE_PRUNE}" == "1" ]]; then
    APP_DIR="${APP_DIR}" PYTHON="${PYTHON}" "${APP_DIR}/deploy/prune-raw-cache.sh" \
      || echo "WARN: raw-cache prune failed (exit $?)"
  fi
}

git add docs/data/

if git diff --cached --quiet; then
  echo "No publishable docs/data changes."
  prune_raw_cache
  exit 0
fi

git config user.name "${GIT_AUTHOR_NAME:-aemo-nas-bot}"
git config user.email "${GIT_AUTHOR_EMAIL:-aemo-nas-bot@users.noreply.github.com}"
# Label with the NEM date (AEST, UTC+10 all year; a POSIX TZ, no tzdata needed).
# A UTC label read a day early: the 05:15 daily run is 19:15 UTC the day before.
git commit -m "${COMMIT_MESSAGE_PREFIX} $(TZ=AEST-10 date +%Y-%m-%d)"

if [[ "${PUSH_CHANGES}" == "1" ]]; then
  git push origin main
else
  echo "PUSH_CHANGES=0; commit created but not pushed."
fi

prune_raw_cache
