#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/workspace/repos/aemo-generator-credit-dashboard}"
PYTHON="${PYTHON:-${APP_DIR}/.venv/bin/python}"
PIPELINE_ARGS="${PIPELINE_ARGS:---months-back 2 --refresh-mlf}"
RUN_TESTS="${RUN_TESTS:-1}"
PUSH_CHANGES="${PUSH_CHANGES:-1}"
RUN_RAW_CACHE_PRUNE="${RUN_RAW_CACHE_PRUNE:-1}"
COMMIT_MESSAGE_PREFIX="${COMMIT_MESSAGE_PREFIX:-Update AEMO generator credit data}"

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

if [[ "${RUN_RAW_CACHE_PRUNE}" == "1" ]]; then
  "${APP_DIR}/deploy/prune-raw-cache.sh"
fi

git add docs/data/

if git diff --cached --quiet; then
  echo "No publishable docs/data changes."
  exit 0
fi

git config user.name "${GIT_AUTHOR_NAME:-aemo-nas-bot}"
git config user.email "${GIT_AUTHOR_EMAIL:-aemo-nas-bot@users.noreply.github.com}"
git commit -m "${COMMIT_MESSAGE_PREFIX} $(date -u +%Y-%m-%d)"

if [[ "${PUSH_CHANGES}" == "1" ]]; then
  git push origin main
else
  echo "PUSH_CHANGES=0; commit created but not pushed."
fi
