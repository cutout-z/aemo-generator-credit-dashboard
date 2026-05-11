#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/aemo-generator-credit-dashboard}"
PYTHON="${PYTHON:-${APP_DIR}/.venv/bin/python}"
PIPELINE_ARGS="${PIPELINE_ARGS:---months-back 2 --refresh-mlf}"
RUN_TESTS="${RUN_TESTS:-1}"
PUSH_CHANGES="${PUSH_CHANGES:-1}"
RUN_RAW_CACHE_PRUNE="${RUN_RAW_CACHE_PRUNE:-1}"
COMMIT_MESSAGE_PREFIX="${COMMIT_MESSAGE_PREFIX:-Update AEMO generator credit data}"

cd "${APP_DIR}"

git fetch origin main
git checkout main
git pull --ff-only origin main

"${PYTHON}" -m src.main ${PIPELINE_ARGS}

if [[ "${RUN_TESTS}" == "1" ]]; then
  "${PYTHON}" -m pytest tests/test_outputs.py -v
fi

if [[ "${RUN_RAW_CACHE_PRUNE}" == "1" ]]; then
  "${APP_DIR}/deploy/prune-raw-cache.sh"
fi

git add docs/data/

if git diff --cached --quiet; then
  echo "No publishable docs/data changes."
  exit 0
fi

git config user.name "${GIT_AUTHOR_NAME:-aemo-vps-bot}"
git config user.email "${GIT_AUTHOR_EMAIL:-aemo-vps-bot@users.noreply.github.com}"
git commit -m "${COMMIT_MESSAGE_PREFIX} $(date -u +%Y-%m-%d)"

if [[ "${PUSH_CHANGES}" == "1" ]]; then
  git push origin main
else
  echo "PUSH_CHANGES=0; commit created but not pushed."
fi
