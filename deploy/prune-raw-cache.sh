#!/usr/bin/env bash
# Raw-cache retention for the shared lane clone. Two passes:
#
# 1. CSV-duplicate pass (RAW_CSV_PRUNE=1): delete a nemosis `<stem>.CSV` once
#    `<stem>.parquet` beside it is known good. nemosis reads the parquet when
#    it exists and re-reads the CSV only when the parquet is missing or on a
#    rebuild, so on every routine run the CSV is dead weight (it was ~33 GB of
#    a ~38 GB cache in 2026-10). Guards: the parquet must be non-empty, open
#    with pyarrow and hold rows (nemosis writes it in place, so a killed run
#    can leave a truncated one and the CSV is then the only good copy); a
#    BIDPEROFFER_D parquet must also carry the columns the offer fetch needs
#    (else the thin-cache heal would re-download instead of re-decoding); the
#    CSV must be older than PRUNE_CSV_MIN_AGE_MINUTES (never a file mid-write);
#    the pass refuses outright when the cache dir is missing or holds fewer
#    than PRUNE_MIN_PARQUETS parquets (an unmounted or emptied cache must not
#    look like a cache to prune); at most PRUNE_MAX_CSV_DELETE files go per
#    run. GENCONDATA and SPDCONNECTIONPOINTCONSTRAINT files are never touched.
#
# 2. mtime pass (RAW_CACHE_RETENTION_DAYS > 0): delete raw nemosis CSVs and
#    legacy full intermittent feathers older than the retention, EXCEPT the
#    GENCONDATA / SPDCONNECTIONPOINTCONSTRAINT month files (both name forms),
#    which every constraints refresh recompiles from 2009 onwards. Parquets
#    are never aged out: with the CSVs gone they are the only copy of each
#    month (~5 GB for 2021-2026), and every cache file carries the same
#    October 2026 mtime, so an age rule would delete the whole history at once.
#
# data/gencondata.feather and data/spdcp_constraint_versions.feather sit
# outside the nemosis cache and match neither pass.
# So do data/next_day_unit_solution_<YYYYMMDD>.feather (~3 MB per market day,
# written only when an MMSDM DISPATCHLOAD month is short; src/download_scada.py).
set -euo pipefail

APP_DIR="${APP_DIR:-/workspace/repos/aemo-generator-credit-dashboard}"
PYTHON="${PYTHON:-${APP_DIR}/.venv/bin/python}"
CACHE_ROOT="${CACHE_ROOT:-${APP_DIR}/data}"
NEMOSIS_CACHE_DIR="${NEMOSIS_CACHE_DIR:-${CACHE_ROOT}/nemosis_cache}"
RAW_CACHE_RETENTION_DAYS="${RAW_CACHE_RETENTION_DAYS:-120}"
RAW_CSV_PRUNE="${RAW_CSV_PRUNE:-1}"
PRUNE_MIN_PARQUETS="${PRUNE_MIN_PARQUETS:-100}"
PRUNE_MAX_CSV_DELETE="${PRUNE_MAX_CSV_DELETE:-1000}"
PRUNE_CSV_MIN_AGE_MINUTES="${PRUNE_CSV_MIN_AGE_MINUTES:-360}"

csv_duplicate_pass() {
  # One python invocation for the whole cache (pyarrow from the lane venv).
  "${PYTHON}" - "${NEMOSIS_CACHE_DIR}" "${PRUNE_MIN_PARQUETS}" \
    "${PRUNE_MAX_CSV_DELETE}" "${PRUNE_CSV_MIN_AGE_MINUTES}" <<'PY'
import os
import sys
import time
from collections import Counter

cache_dir = sys.argv[1]
min_parquets = int(sys.argv[2])
max_delete = int(sys.argv[3])
min_age_s = int(sys.argv[4]) * 60

EXCLUDE = ("GENCONDATA", "SPDCONNECTIONPOINTCONSTRAINT")
# Mirrors src/download_bids.py BIDPEROFFER_REQUIRED_COLS (a test pins it).
BIDPEROFFER_REQUIRED_COLS = {
    "INTERVAL_DATETIME", "DUID", "BIDTYPE",
    "MAXAVAIL", "ENABLEMENTMIN", "ENABLEMENTMAX",
    *(f"BANDAVAIL{i}" for i in range(1, 11)),
}

if not os.path.isdir(cache_dir):
    print(f"WARN: CSV prune refused: {cache_dir} is missing; deleted nothing.")
    sys.exit(0)

names = os.listdir(cache_dir)
n_parquet = sum(1 for n in names if n.endswith(".parquet"))
if n_parquet < min_parquets:
    print(
        f"WARN: CSV prune refused: {cache_dir} holds {n_parquet} parquet files "
        f"(< PRUNE_MIN_PARQUETS={min_parquets}); deleted nothing."
    )
    sys.exit(0)

import pyarrow.parquet as pq


def parquet_ok(path, csv_name):
    try:
        if os.path.getsize(path) <= 0:
            return "parquet empty"
        meta = pq.ParquetFile(path)
        if meta.metadata.num_rows <= 0:
            return "parquet has no rows"
        if "BIDPEROFFER_D" in csv_name:
            missing = BIDPEROFFER_REQUIRED_COLS - set(meta.schema_arrow.names)
            if missing:
                return "parquet lacks required BIDPEROFFER_D columns"
    except FileNotFoundError:
        return "no parquet"
    except Exception as exc:  # truncated / corrupt footer
        return f"parquet unreadable ({type(exc).__name__})"
    return None


now = time.time()
deleted = 0
freed_disk = 0
freed_apparent = 0
kept = Counter()
capped = 0
for name in sorted(names):
    stem, ext = os.path.splitext(name)
    if ext.lower() != ".csv" or any(t in name for t in EXCLUDE):
        continue
    csv_path = os.path.join(cache_dir, name)
    try:
        st = os.stat(csv_path)
    except OSError:
        continue
    if now - st.st_mtime < min_age_s:
        kept["younger than min age"] += 1
        continue
    reason = parquet_ok(os.path.join(cache_dir, stem + ".parquet"), name)
    if reason:
        kept[reason] += 1
        continue
    if deleted >= max_delete:
        capped += 1
        continue
    try:
        os.unlink(csv_path)
    except OSError as exc:
        kept[f"unlink failed ({type(exc).__name__})"] += 1
        continue
    print(f"deleted {csv_path}")
    deleted += 1
    freed_disk += getattr(st, "st_blocks", 0) * 512
    freed_apparent += st.st_size

kept_txt = ", ".join(f"{v} {k}" for k, v in sorted(kept.items())) or "none"
print(
    f"CSV prune: deleted {deleted} CSV files, freed {freed_disk} bytes on disk "
    f"({freed_disk / 1e9:.2f} GB; {freed_apparent / 1e9:.2f} GB apparent); "
    f"kept: {kept_txt}"
)
if capped:
    print(
        f"WARN: CSV prune hit PRUNE_MAX_CSV_DELETE={max_delete}; "
        f"{capped} more eligible CSVs left for the next run."
    )
PY
}

if [[ "${RAW_CSV_PRUNE}" == "1" ]]; then
  echo "Pruning nemosis CSVs that have a valid parquet sibling in ${NEMOSIS_CACHE_DIR}"
  csv_duplicate_pass || echo "WARN: CSV-duplicate prune failed (exit $?); continuing."
else
  echo "RAW_CSV_PRUNE=${RAW_CSV_PRUNE}; skipping CSV-duplicate prune."
fi

if [[ "${RAW_CACHE_RETENTION_DAYS}" -le 0 ]]; then
  echo "RAW_CACHE_RETENTION_DAYS=${RAW_CACHE_RETENTION_DAYS}; skipping mtime prune."
  exit 0
fi

if [[ -d "${NEMOSIS_CACHE_DIR}" ]]; then
  echo "Pruning raw NEMOSIS CSVs older than ${RAW_CACHE_RETENTION_DAYS} days from ${NEMOSIS_CACHE_DIR} (parquets, GENCONDATA, SPDCONNECTIONPOINTCONSTRAINT kept)"
  find "${NEMOSIS_CACHE_DIR}" -type f -iname '*.csv' -mtime "+${RAW_CACHE_RETENTION_DAYS}" \
    ! -name '*GENCONDATA*' ! -name '*SPDCONNECTIONPOINTCONSTRAINT*' \
    -print -delete
fi

if [[ -d "${CACHE_ROOT}" ]]; then
  echo "Pruning legacy full INTERMITTENT_GEN_SCADA feather caches older than ${RAW_CACHE_RETENTION_DAYS} days from ${CACHE_ROOT}"
  find "${CACHE_ROOT}" -maxdepth 1 -type f -name 'intermittent_[0-9][0-9][0-9][0-9]_*.feather' -mtime "+${RAW_CACHE_RETENTION_DAYS}" -print -delete
fi
