# Logic pass rollout: 2026-10-05

Branch `fix/logic-pass-2026-10`, cut from `2fe41d929`, the 30 Sep data commit. It fixes the pipeline code
for audit findings H1–H5. It has not regenerated, rewritten or published any data. None of the
steps below has been run.

## What the code changes do on the next normal run

- H1: monthly joins dedupe SCADA, DISPATCHPRICE, DISPATCHLOAD and DISPATCHCONSTRAINT intervals, and
  a join that multiplies rows now raises. Months inside the mutable window are unaffected unless their
  archive repeats rows.
- H2: offer factors are filtered to the fetched month. A partial month never replaces a more complete
  cached one. A finished month left partial degrades the offer lanes in `run_status.json`.
- H4/H5: Step 2 downloads the newest MMSDM DUDETAILSUMMARY archive (~380 KB) into
  `data/loss_factor_periods.feather`. Revenue then uses the transmission factor in effect on each day.
  The new fields `revenue_loss_adjusted_aud`, `revenue_dlf_value`, `revenue_dlf_status` and
  `revenue_mlf_source` appear in the monthly feather and the unit/station JSON. `revenue_aud` keeps
  its meaning. Units the tracker had no factor for, which are "unknown" and unadjusted, now take the dated
  factor. On Aug 2026 that is 8 units, for example MULWASF1, whose revenue falls 10.9%.

## Owner steps (in order)

1. Merge the branch into current `main`. `main` has moved on since `2fe41d929`, but only with design
   commits that touch `docs/`, so the `src/` and `tests/` changes should merge cleanly. Run
   `pytest -q tests` and expect only the NAS-data tests in `tests/test_outputs.py` to fail off the lane.
2. Resolve the FY26-27 bidirectional factor question before any rewrite. For FY26-27 the MLF Tracker's
   value for batteries equals DUDETAILSUMMARY's `SECONDARY_TLF` (the load side), and its "Import" column
   equals `TRANSMISSIONLOSSFACTOR`. Example: RESS1 tracker 0.9862 vs DUDETAILSUMMARY 0.9008 / 0.9862.
   FY24-25 and FY25-26 agree. Check AEMO's FY26-27 MLF report, and fix `aemo-mlf-tracker` if the
   tracker has the columns swapped. Until this is resolved, the pipeline keeps the tracker value for
   the 50 affected DUIDs and logs them every month.
3. Run an audited history rewrite on the NAS lane: `python -m src.main --full-refresh`. This is the
   only path the settled-history guard allows. It rewrites:
   - 2022-06 for every unit (H1): NEM 20.01 → 16.93 TWh, BW01 547,389 → 466,485 MWh;
   - the 26 batteries' months up to 2026-01 (H3): about −$8.4M, RESS1 $18.51M → $16.42M;
   - FY25-26 units with mid-year revisions (H5): QPSFB1 Feb–Jun −9.9%, LIMOSF11 −$214k;
   - `revenue_loss_adjusted_aud` across all history (H4);
   - offer factors / curves, which repairs 2026-05..08 (H2).
4. Before publishing, diff the rewritten `monthly_aggregates.feather` against the current one.
   `revenue_aud` should move only for the cases listed in step 3, units that were "unknown", and
   DUIDs whose dated factor differs within the FY. Anything else needs explaining first.
5. In the same commit as the rewritten data, remove both exemptions in `tests/test_outputs.py`:
   `CF_SUSPENSION_ANOMALY_MONTH` (2022-06) and `UNADJUSTED_BATTERY_DUIDS` /
   `UNADJUSTED_BATTERY_LAST_MONTH`. Then run `tests/test_outputs.py` on the lane.
6. Constraint hours get the DISPATCHCONSTRAINT dedupe only when constraints are re-run. The lane
   currently passes `--skip-constraints` (finding M7).
7. Page decision, separate from this branch: whether to show `revenue_loss_adjusted_aud`, which is
   MLF × DLF, or keep `revenue_aud` and label it as transmission-MLF only.

## Known limitation

The DUDETAILSUMMARY archive lags about one month. If AEMO revises a factor after the newest archive, a
month can leave the mutable window before the revision is visible, and that month is then frozen at
the old factor. The fix for that is another audited rewrite. Watching the per-month
`revenue_mlf_source` field will show when it is needed.
