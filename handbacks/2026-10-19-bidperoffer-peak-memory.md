---
project: AEMO Generator Credit Dashboard
agent: claude
branch: perf/bidperoffer-peak-memory
merged: true
status: PR #10 merged 2026-10-19 (70f74bec1, owner's yes); factor-step peak footprint 13-16 GiB -> 4.9 GiB on real cached months, outputs identical; lane proof pending the first scheduled run after merge
outstanding:
  - "Owner: merge PR #11 (logs the run's peak RSS; without it nothing records a run's peak after it ends)"
  - "Verify the first scheduled aemo-generator-credit-daily run after #11 merges: END status=0, a normal docs/data diff, and its 'Peak RSS after the whole run' log line well under the 13 GB of 2026-10-19 (expected ~5-6 GB)"
---

# BIDPEROFFER_D factor step: peak memory

PR: https://github.com/cutout-z/aemo-generator-credit-dashboard/pull/10 (commit e7a06397e), merged as
70f74bec1 on 2026-10-19 with the owner's yes. Tests on merged main: 514 passed, 3 skipped, 12 failed,
10 errors (all non-passes in `tests/test_outputs.py`, as before).

## What changed

- `src/main.py`: the month-outer factor driver now releases the previous month's frames before the next
  read, and drops the FCAS slice once its factors are computed and `raw` once both lanes have sliced it.
- `src/download_bids.py`: `_read_cached_window` clears `pieces` before `to_pandas(self_destruct=True)`
  (it had kept every row-group table referenced, so the month was resident as Arrow and as pandas), and
  skips `take()` for row groups wholly in the window. The FCAS slice copies only FCAS columns, and
  `dropna` runs only when something drops.
- `src/offer_curves.py`: the ENERGY slice copies only the columns the offer lanes read.
- `src/fcas_factor.py`: no whole-frame copy before setting the timestamp column, and each unit's positive
  rows come from its own group (the same rows, in the same order) instead of a dict holding all of them.
- `src/__init__.py`: `os.environ.setdefault("MIMALLOC_PURGE_DELAY", "0")`, which runs before pyarrow loads.
- `tests/test_bidperoffer_peak_memory.py`: 6 guard tests.

## Evidence

- Data: BIDPEROFFER_D and BIDDAYOFFER_D 2026-06..09 parquets, copied read-only from the lane's
  nemosis cache over the bridge, sha256-matched. Months processed: 2026-07..09, the same three the
  2026-10-19 run did (`--months-back 2` gives three months today).
- Environment: Python 3.12, pandas 3.0.3, pyarrow 24.0.0, numpy 2.4.6, nemosis 3.8.1, matching the lane's
  venv `pip freeze`. Arrow's pool is mimalloc on both machines.
- Harness: `2026-10-19-bidperoffer-peak-memory/profile_factor_lanes.py <repo> <data> <out>` runs
  `_run_optional_factor_lanes` on a temp `data_dir` whose `nemosis_cache` symlinks to the copied
  parquets. It reports macOS lifetime-max `phys_footprint` (`footprint.py`) and `ru_maxrss`, and pickles
  the four factor frames; `compare_outputs.py` diffs them with `check_exact=True`.

| | origin/main d0bae973b | branch |
|---|---|---|
| peak footprint | 14.29 / 13.06 / 16.22 GiB | 4.87 / 4.85 GiB |
| peak RSS | 18.40 / 18.08 / 17.31 GiB | 12.73 / 12.68 GiB |
| elapsed | 106-110 s | 101 s |

Cumulative peak footprint per fix: 14.29 → 6.28 (release months, read fix, narrow slices) → 5.20
(mimalloc purge) → 5.01 (FCAS copy guards) → 4.87 (no `pos_by_duid`).

- Outputs: the fcas (679×19), offer (1522×12), curves (15220×6) and daily (118271×6) frames, and the lane
  statuses, are identical to origin/main's.
- Tests in the pandas 3.0.3 env: 514 passed, 3 skipped, 12 failed, 10 errors. origin/main gives 508
  passed, 3 skipped, 12 failed, 10 errors. All the non-passes are in `tests/test_outputs.py`, on lane-only
  generated data. In the pandas 2.3.3 env both branches fail the same 2 `TestMatchesCompiler` tests
  (pre-existing).
- Each guard fails with its fix reverted: `[0, 1, 1] != [0, 0, 0]`; pool max 5638848 against a 1.5×
  limit of 3097920; slices 2 failed; `None != '0'`.
- Step 3's fetches (SCADA, price, dispatch load, same months) peak at ~2.5 GiB footprint, so the factor
  step is still the run's peak. Steps 1-2 and 4+ were not profiled.

## Not verified

- Linux VmHWM. macOS RSS counts freed-but-reusable pages, so I used footprint as the main metric. On
  Linux, mimalloc's purge and glibc's munmap of large blocks should make VmHWM track footprint, but the
  first lane run is the proof. I did not run the lane: a run publishes.

## Rulings

- Ruling: set `MIMALLOC_PURGE_DELAY` in `src/__init__.py` rather than in `deploy/run-update.sh`, because
  that covers every entry point and `setdefault` still lets the environment override it; if wrong: someone
  looking for runtime knobs in `deploy/` won't find it there (no measurable runtime cost).
- Ruling: stopped at ~4.9 GiB instead of reordering the driver to free `raw` before the FCAS compute
  (est. a further ~0.5 GiB), because the reorder changes the S3-08 error-attribution flow for a small
  gain; if wrong: ~0.5 GiB of headroom left on the table.
- Ruling: `fcas_bids_from_raw` no longer returns the BANDAVAIL columns, and `energy_volumes_from_raw` no
  longer returns MAXAVAIL/ENABLEMENT*, because no consumer reads them (the factor frames are identical);
  if wrong: a future consumer of those columns must add them back to `_FCAS_SLICE_COLS` /
  `_VOLUME_SLICE_COLS`.
- Ruling: left the raw union frame's dtypes alone (int64 bands could be narrower), because its identity
  with the nemosis compiler is pinned by `test_bidperoffer_low_memory.py`; if wrong: more savings were
  available at the cost of that guarantee.

## Noticed, not changed

- Every month logs "VERSIONNO absent from cache", so FCAS rebid dedupe is skipped (and the ENERGY dedupe
  falls back to file order). The cached parquets lack the column; this predates the change.
- `compute_offer_curves_daily` spends ~20-25 s a month on per-band MultiIndex `.loc` lookups: a speed
  item, not a memory one.

## Follow-up: PR #11 (peak-RSS logging)

Nothing recorded a run's peak memory: the 13 GB was read live from `/proc`, and the runner has no GNU
`time`. https://github.com/cutout-z/aemo-generator-credit-dashboard/pull/11 logs `ru_maxrss`
(VmHWM on Linux) as `Peak RSS after <stage>: N.NN GiB` after Step 3, after the factor step and at the
end of the run. Tests: 516 passed, 3 skipped, 12 failed, 10 errors (the same 22 `test_outputs.py`
non-passes as main); the 2 new tests fail with the change reverted.
