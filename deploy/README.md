# Frequency-Driven Updates — NAS runner (production)

> **VPS retired 2026-05-29.** The NAS runner runs every lane; any VPS or systemd setup described in this repo is historical. The current estate (hosts, lanes, schedules) is in ARCHITECTURE.md in the agent-contracts repo.

The production model (see the main `README.md` "Deployment" section for the
authoritative overview):

- The **NAS runner** (QNAP `ai-wif-runner` container) runs the
  frequency-driven data pipeline and keeps only the raw cache needed for
  recent updates.
- GitHub stores code, publishable `docs/data` outputs, and the compact
  `docs/data/processed-cache` settled-history snapshot.
- GitHub Pages deploys after the NAS lane pushes updated `docs/data`.
- GitHub Actions remains useful for manual verification, but is not the
  primary heavy data runner.

## Update Lanes

QNAP scheduled tasks invoke `nas-job aemo-generator-credit-*`, which runs
this repo's `deploy/run-update.sh` (the shared entry script) with
per-lane `PIPELINE_ARGS`:

| Lane | `PIPELINE_ARGS` | Purpose |
| --- | --- | --- |
| Daily market data | `--months-back 2 --refresh-mlf --skip-constraints` | Reprocess recent SCADA, dispatch prices, dispatch load, FCAS, and pick up small MLF tracker changes. |
| Weekly reference data | `--months-back 2 --refresh-metadata --refresh-mlf --skip-constraints` | Refresh AEMO registration/metadata and MLF tracker without a full 5-year rebuild. |
| Annual MLF lane | `--skip-scada --skip-constraints --refresh-mlf` | Force a lightweight MLF refresh around annual final MLF publication without touching SCADA or constraints. |
| Monthly constraints (`env.constraints.example`) | `--skip-scada --skip-fcas-factors --skip-offer-factors --skip-geninfo --skip-network-outages --skip-aer-qa --months-back 2` | Refresh binding-constraint hours. Runs only the constraint step on top of the cached history: fetches every month after the newest cached one, re-pulls GENCONDATA / SPDCONNECTIONPOINTCONSTRAINT when their cache is older than 25 days, and **exits non-zero** when the constraints source is not ok (the `run_status.json` it wrote on the NAS disk says why; nothing is committed). The first run (2026-10-06) could not load those two tables, exited 0, and left the published constraint data ending 2026-03. |

The daily and weekly lanes skip constraints on purpose: a missing
`data/gencondata.feather` makes the constraint step pull GENCONDATA history
from 2020, which is too heavy for a daily run. Constraint data therefore
comes only from its own monthly lane, and every run's `run_status.json` carries a
`constraints` source with its as-of month. It turns **degraded** (an AEMO
ALERT through the staleness check) once constraint data trails the newest
generation month by more than two months, so a stopped constraints lane is
visible instead of silent.

The lane registry, cadence windows and report paths live in
`tools/nas-runner/configs/brain-ops.nas.toml` (the NAS runner tooling).
`deploy/run-update.sh` is the NAS-native entry script (renamed from the
retired VPS-era `run-vps-update.sh` in the 2026-09 cleanup). Each lane runs the
full test suite and commits/pushes only when `docs/data` changed, and the
script self-heals a rewritten `main`: if `git pull --ff-only` is impossible it
resets onto the fetched remote instead of exiting 128.

## Raw Cache Retention

`deploy/prune-raw-cache.sh` runs at the end of every lane run
(`RUN_RAW_CACHE_PRUNE=1`): after `git push` on a publishing day and after the
"No publishable docs/data changes" check on a no-change day, still inside the
clone's flock. It cannot fail the run: an error prints
`WARN: raw-cache prune failed (exit N)` and the run keeps its exit code, so a
prune problem never blocks a publish. It has two passes.

**1. CSV-duplicate pass** (`RAW_CSV_PRUNE`, default `1`; set `0` to switch it
off). nemosis keeps every downloaded month's raw `<stem>.CSV` beside the
`<stem>.parquet` it writes, and it reads the parquet whenever one exists: the
CSV is re-read only when the parquet is missing or on a rebuild
(`--full-refresh`, the BIDPEROFFER_D thin-cache heal). In 2026-10 those CSVs
were ~33 GB of the ~38 GB cache. The pass deletes `<stem>.CSV` only when all
of these hold:

- `<stem>.parquet` exists, is non-empty, opens with pyarrow and has
  `num_rows > 0` (nemosis writes it in place, so a killed run can leave a
  truncated parquet; the CSV is then the only good copy);
- for `BIDPEROFFER_D`, the parquet also carries every column in
  `src/download_bids.py` `BIDPEROFFER_REQUIRED_COLS` (else the thin-cache heal
  would re-download the month instead of re-decoding the CSV);
- the CSV is older than `PRUNE_CSV_MIN_AGE_MINUTES` (default 360, i.e. 6 h),
  so a file being written is never touched;
- it is not a `GENCONDATA` or `SPDCONNECTIONPOINTCONSTRAINT` file.

Guards: the pass refuses (prints a `WARN: CSV prune refused` line, deletes
nothing, exits 0) when the cache dir is missing or holds fewer than
`PRUNE_MIN_PARQUETS` parquets (default 100), because an unmounted or emptied
cache must not look like one to prune. It deletes at most
`PRUNE_MAX_CSV_DELETE` CSVs per run (default 1000, enough for the one-off
backlog) and warns when it hits the cap. It ends with a summary line: files
deleted, bytes freed on disk and apparent, and how many CSVs it kept and why.
The parquet check runs in one invocation of the lane's `${PYTHON}`.

**2. mtime pass** (`RAW_CACHE_RETENTION_DAYS`, default 120; `0` switches it
off). Deletes raw nemosis CSVs and legacy full intermittent feather caches
older than the retention, which is deliberately wider than the normal 2-month
mutable window. It never deletes a parquet: once the CSVs are gone a parquet
is the only copy of its month (about 5 GB for 2021-2026), and the October 2026
full refreshes gave every cache file the same mtime, so an age rule on
parquets would delete the whole history in one run. It never deletes `GENCONDATA` or `SPDCONNECTIONPOINTCONSTRAINT`
month files, in either name form (`PUBLIC_DVD_<TABLE>_*` before 2024-08,
`PUBLIC_ARCHIVE#<TABLE>#FILE01#*` from 2024-08): every constraints refresh
recompiles those tables from 2009, so losing them means re-downloading the
whole history.

Neither pass touches `data/gencondata.feather`,
`data/spdcp_constraint_versions.feather` or the compact
`docs/data/processed-cache` snapshot.

---

## Historical: Hetzner VPS + systemd timers (retired)

Before the NAS migration (2026-05) this project ran on a Hetzner VPS with
systemd timers and a `/opt/aemo-generator-credit-dashboard` checkout. That
setup is **historical** — do not reinstall it:

- timers: `aemo-generator-credit-daily.timer`, `aemo-generator-credit-reference.timer`,
  `aemo-generator-credit-mlf.timer`, plus the `aemo-generator-credit@.service`
  template;
- layout: `/opt/aemo-generator-credit-dashboard` (checkout + virtualenv),
  `/srv/aemo-generator-credit/data` (optional bounded raw cache),
  `/etc/aemo-generator-credit/*.env` (per-lane settings); `data/` was
  gitignored and symlinked from `/srv` when a larger volume was wanted;
- the retired `.service`/`.timer` unit files were deleted in the 2026-09 NAS
  cleanup; `env.*.example` remain as NAS-path templates. The QNAP scheduled
  tasks are the live scheduler;
- ✅ the write-enabled GitHub deploy key `hetzner-aemo-vps-2026-05-11` (created
  2026-05-11) was a VPS-era leftover and has been **revoked** (2026-09-19). The
  NAS lanes push over HTTPS with a stored token; no deploy key is in use.
