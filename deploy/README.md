# VPS Frequency-Driven Updates

The intended production model is:

- Hetzner VPS runs the frequency-driven data pipeline and keeps only the raw cache needed for recent updates.
- GitHub stores code, publishable `docs/data` outputs, and the compact `docs/data/processed-cache` settled-history snapshot.
- GitHub Pages deploys after the VPS pushes updated `docs/data`.
- GitHub Actions remains useful for manual verification, but should not be the primary heavy data runner.

## Update Lanes

| Lane | Timer | Pipeline args | Purpose |
| --- | --- | --- | --- |
| Daily market data | `aemo-generator-credit-daily.timer` | `--months-back 2 --refresh-mlf` | Reprocess recent SCADA, dispatch prices, dispatch load, constraints, FCAS, and pick up small MLF tracker changes. |
| Weekly reference data | `aemo-generator-credit-reference.timer` | `--months-back 2 --refresh-metadata --refresh-mlf` | Refresh AEMO registration/metadata and MLF tracker without a full 5-year rebuild. |
| Annual MLF lane | `aemo-generator-credit-mlf.timer` | `--skip-scada --skip-constraints --refresh-mlf` | Force a lightweight MLF refresh around annual final MLF publication without touching SCADA or constraints. |

## VPS Setup Notes

The processed-cache snapshot is the durable source of settled history. Routine VPS runs should not need a full raw-history cache: they restore the compact snapshot, reprocess the recent mutable overlap window, verify older months are unchanged, and publish a new snapshot. The current VPS disk is therefore acceptable for normal automation if raw cache is kept bounded. Add a larger volume only if you want the VPS to perform full historical rebuilds or deep raw-source audits locally.

Recommended layout:

```text
/opt/aemo-generator-credit-dashboard      git checkout + virtualenv
/srv/aemo-generator-credit/data           optional bounded raw/recent cache
/etc/aemo-generator-credit/*.env          per-lane service settings
```

The repo's `data/` directory is gitignored. On the VPS it can live on the root disk for normal automation, or be symlinked to a larger volume if you later want local full-history rebuilds:

```bash
ln -sfn /srv/aemo-generator-credit/data /opt/aemo-generator-credit-dashboard/data
```

Create `/etc/aemo-generator-credit/daily.env`, `reference.env`, and `mlf.env` from the example files in this directory. The service user needs a repo-scoped deploy key that can push to `cutout-z/aemo-generator-credit-dashboard`.

## Install Timers

```bash
sudo cp deploy/aemo-generator-credit@.service /etc/systemd/system/
sudo cp deploy/aemo-generator-credit-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aemo-generator-credit-daily.timer
sudo systemctl enable --now aemo-generator-credit-reference.timer
sudo systemctl enable --now aemo-generator-credit-mlf.timer
```

Run once manually:

```bash
sudo systemctl start aemo-generator-credit@daily.service
journalctl -u aemo-generator-credit@daily.service -f
```
