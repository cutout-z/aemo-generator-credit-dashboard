# Frequency-Driven Updates — NAS runner (production)

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
this repo's `deploy/run-vps-update.sh` (the shared entry script) with
per-lane `PIPELINE_ARGS`:

| Lane | `PIPELINE_ARGS` | Purpose |
| --- | --- | --- |
| Daily market data | `--months-back 2 --refresh-mlf --skip-constraints` | Reprocess recent SCADA, dispatch prices, dispatch load, FCAS, and pick up small MLF tracker changes. |
| Weekly reference data | `--months-back 2 --refresh-metadata --refresh-mlf --skip-constraints` | Refresh AEMO registration/metadata and MLF tracker without a full 5-year rebuild. |
| Annual MLF lane | `--skip-scada --skip-constraints --refresh-mlf` | Force a lightweight MLF refresh around annual final MLF publication without touching SCADA or constraints. |

The lane registry, cadence windows and report paths live in
`tools/nas-runner/configs/brain-ops.nas.toml` (the NAS runner tooling). The
`run-vps-update.sh` name is retained from the retired VPS era for
compatibility — it is a NAS lane now. Each lane runs the full test suite and
commits/pushes only when `docs/data` changed.

## Raw Cache Retention

Routine runs prune raw NEMOSIS files and legacy full intermittent feather
caches older than `RAW_CACHE_RETENTION_DAYS` after validation. The default is
120 days, which is deliberately wider than the normal 2-month mutable window.
The compact `docs/data/processed-cache` snapshot is never pruned by this
script.

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
- the `.service`/`.timer` unit files and `env.*.example` files remain in
  `deploy/` for reference only. The QNAP scheduled tasks are the live
  scheduler.
