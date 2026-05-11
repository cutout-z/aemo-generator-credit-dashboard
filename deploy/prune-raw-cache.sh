#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/aemo-generator-credit-dashboard}"
CACHE_ROOT="${CACHE_ROOT:-${APP_DIR}/data}"
NEMOSIS_CACHE_DIR="${NEMOSIS_CACHE_DIR:-${CACHE_ROOT}/nemosis_cache}"
RAW_CACHE_RETENTION_DAYS="${RAW_CACHE_RETENTION_DAYS:-120}"

if [[ "${RAW_CACHE_RETENTION_DAYS}" -le 0 ]]; then
  echo "RAW_CACHE_RETENTION_DAYS=${RAW_CACHE_RETENTION_DAYS}; skipping raw-cache prune."
  exit 0
fi

if [[ -d "${NEMOSIS_CACHE_DIR}" ]]; then
  echo "Pruning raw NEMOSIS cache files older than ${RAW_CACHE_RETENTION_DAYS} days from ${NEMOSIS_CACHE_DIR}"
  find "${NEMOSIS_CACHE_DIR}" -type f -mtime "+${RAW_CACHE_RETENTION_DAYS}" -print -delete
fi

if [[ -d "${CACHE_ROOT}" ]]; then
  echo "Pruning legacy full INTERMITTENT_GEN_SCADA feather caches older than ${RAW_CACHE_RETENTION_DAYS} days from ${CACHE_ROOT}"
  find "${CACHE_ROOT}" -maxdepth 1 -type f -name 'intermittent_[0-9][0-9][0-9][0-9]_*.feather' -mtime "+${RAW_CACHE_RETENTION_DAYS}" -print -delete
fi
