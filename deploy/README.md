# VPS Frequency-Driven Updates

The intended production model is:

- Hetzner VPS owns the warm AEMO/NEMOSIS cache and runs the data pipeline.
- GitHub stores only code plus publishable `docs/data` outputs.
- GitHub Pages deploys after the VPS pushes updated `docs/data`.
- GitHub Actions remains useful for manual verification, but should not be the primary heavy data runner.

## Update Lanes

| Lane | Timer | Pipeline args | Purpose |
| --- | --- | --- | --- |
| Daily market data | `aemo-generator-credit-daily.timer` | `--months-back 2 --refresh-mlf` | Reprocess recent SCADA, dispatch prices, dispatch load, constraints, FCAS, and pick up small MLF tracker changes. |
| Weekly reference data | `aemo-generator-credit-reference.timer` | `--months-back 2 --refresh-metadata --refresh-mlf` | Refresh AEMO registration/metadata and MLF tracker without a full 5-year rebuild. |
| Annual MLF lane | `aemo-generator-credit-mlf.timer` | `--skip-scada --skip-constraints --refresh-mlf` | Force a lightweight MLF refresh around annual final MLF publication without touching SCADA or constraints. |

## VPS Setup Notes

The current VPS root disk is too small for the existing raw cache footprint. The local raw NEMOSIS cache has been seen above 100 GB, so attach persistent storage before installing this as production. A 160-200 GB mounted volume gives enough room for warm raw cache, processed cache, logs, and growth.

Recommended layout:

```text
/opt/aemo-generator-credit-dashboard      git checkout + virtualenv
/srv/aemo-generator-credit/data           persistent cache volume
/etc/aemo-generator-credit/*.env          per-lane service settings
```

The repo's `data/` directory is gitignored. On the VPS it should either live on the attached volume or be symlinked to it:

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
