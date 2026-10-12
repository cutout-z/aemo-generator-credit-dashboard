---
project: AEMO Generator Credit Dashboard
agent: claude
branch: fix/bidperoffer-low-memory-read
merged: false
status: PR #5 open (low-memory BIDPEROFFER_D read + one lane per clone); awaiting merge
outstanding:
  - "to-do merge cutout-z/aemo-generator-credit-dashboard#5 (pipeline code: owner's yes)"
  - "to-do merge cutout-z/agent-contracts#65 (ARCHITECTURE §8.5 note) with or after #5"
  - "to-do verify the first two daily runs after the merge: the first still runs the old run-update.sh (it pulls the new one mid-run); from the second, the scheduler log should show 'Read BIDPEROFFER_D ... (low-memory path)' and the run should stay under ~8 GB RSS"
  - "decision container memory cap for the runner container (proposal below; container change: owner's yes)"
---

# NAS memory incident 2026-10-12: two credit lanes at once

## What happened
Around 07:30–08:22 AWST the NAS ran out of memory: load 43–51, 88% iowait, about 0.45 GB of 64 GB
available, about 20 GB swapped. Two runs of this pipeline were going at once, both in D state:
- **`aemo-generator-credit-daily`** (cron 07:15). Started 23:15:00Z; still finishing at 00:34Z,
  about 80 min against its usual 13–18 min.
- **`aemo-generator-credit-reference`** (cron Mon 07:30, adds `--refresh-metadata`). Started 23:30:00Z,
  ended 00:22:45Z with status 0, and pushed `e29430e7b`.

## Q1: who started the second run
The cron did. No manual or other-session run was involved, and there was no stale lock. The scheduler
log shows a START for each at its cron time. The per-job locks (`locks/scheduler/<job>.lock`) were
both held correctly. The scheduler's mkdir lock is keyed on the **job name**, so it never stops two
*different* jobs. Four jobs (`daily`, `reference`, `constraints`, `mlf`) run `deploy/run-update.sh`
on one clone, venv and nemosis cache. Normally daily ends before 07:30. Today it ran long, and both
runs worked the same months in the same working tree.

## Q2: why one run needed more than 10 GB
`fetch_bidperoffer_union` calls `nemosis.dynamic_data_compiler`. For month M that call:
- reads the M-1, M and M+1 archives;
- `pd.read_parquet`s each cached file **whole** before date-filtering;
- handles a cached month of about 20M rows × 21 columns, all strings.

The 2026-09 archive isn't published yet, yet each run still decoded all of August a second time to
keep the 00:05–04:00 boundary hours.

Fix, commit `d70ec6e06`: a warm cache is read with pyarrow, one row group at a time. Groups outside
the window are skipped using footer statistics, only the union columns are read, and each group is
typed with `pd.to_numeric` semantics. Measured on the real 2026-07/08 cache files, with library
versions pinned to the lane's:

| fetch | compiler | new |
|---|---|---|
| 2026-08 | 14.49 GiB, 51 s | 5.97 GiB, 6 s |
| 2026-09 (unpublished) | 7.32 GiB | 0.45 GiB |

For both months these are identical under `assert_frame_equal(check_exact=True)`: the raw frame,
the FCAS slice, the ENERGY slice and `compute_fcas_factors`. The rest of the 5.97 GiB is allocator
page retention on top of a 2.8 GiB frame (arrow's live allocation stays at the frame size).

## Q3: overlap guard
Commit `ca44f48fb`: `deploy/run-update.sh` takes a `flock` on `<clone>/.git/run-update.lock` before
any git or pipeline step. A second run waits up to 2 h, then exits 75 with `SKIP`. The kernel releases
the lock if its holder dies, so it can't go stale.

## Tests
- `tests/test_bidperoffer_low_memory.py`, 5 tests. It runs the real nemosis compiler on synthetic
  parquets. With the fix reverted the 2 behaviour tests fail; with it applied, 5 pass.
- `tests/test_run_update_lock.py`, 2 tests. It needs util-linux flock, so it skips on macOS. I ran
  it in the lane container on a temp copy: main's script failed 2, this branch passed 2.
- Full suite: main 503 passed, branch 508 passed. Both have the same 22 non-passes, all in
  `tests/test_outputs.py` (generated data git ignores; the documented baseline).

## NAS changes made
None to lane code, config or containers. The one write was a temp test dir in the container's
`/tmp`, created and removed in the same call (verified absent afterwards). No processes were
killed or signalled.

## Proposal (not applied): container memory cap
The runner container has no cgroup memory limit (`memory.max` = unlimited), so one runaway job can
swap the whole NAS. `ulimit -v` is **not** a fix: pyarrow/jemalloc reserve far more virtual address
space than they use (28 GB VSZ was seen), so it would kill healthy runs. Proposal: set `mem_limit` and
`memswap_limit` on the runner container (e.g. 32g; the post-fix lane peak is about 6–8 GB). A runaway
then gets OOM-killed inside the container instead of swapping the host.

- **Before:** read the container's current compose/run definition and `memory.max`.
- **Apply:** this needs the owner's yes and a container recreate. The container also runs the
  gateway and other lanes.
- **Verify:** `cat /sys/fs/cgroup/memory.max` inside the container shows the limit.
- **Rollback:** remove the two keys and recreate.

## Notes
- Today's `docs/data` came from two overlapping writers: reference committed `e29430e7b`, and
  daily (still running at handback) commits on top of it. The next daily run rewrites everything.
- The first lane run after the merge still runs the old script, so the lock protects from the
  second run onward.
- AGENTS.md's "never touch src/deploy" rule is for design passes. This pipeline fix was asked for
  by the owner and is declared through PR #5.
