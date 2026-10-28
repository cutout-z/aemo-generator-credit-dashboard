---
project: AEMO Generator Credit Dashboard
agent: claude
branch: fix/dispatchload-tail-gap
pr: cutout-z/aemo-generator-credit-dashboard#19
pr: cutout-z/aemo-generator-credit-dashboard#20
pr: cutout-z/aemo-generator-credit-dashboard#21
status: #19, #20 and #21 merged; Sep 2025 reprocessed (65c4b1c); portfolio view live (5e316f8)
noticed:
  - "AEMO MMSDM Sep 2025 DISPATCHLOAD archive ends 2025/09/30 04:00 (240 intervals) -> done: PR 19 fills whole missing intervals from Next_Day_Dispatch"
  - "Sep 2025 settled aggregates still computed on 29.2 of 30 days -> decision: card c56b9b118f16fac68 answered (reprocess), closed -> done: reprocessed in 65c4b1c, diff below"
  - "Review L notes on #20 (legacy-row rounding drift up to 0.05 MWh per member; non-curtailable units carry all-null arrays like their pct) -> done: accepted, 0 legacy rows in the cache"
  - "Cross-repo fact (short settled MMSDM month; nemosis Next_Day reads Current only) -> tracked: cutout-z/agent-contracts#175, open for the owner's merge"
  - "download_intermittent.py stages its archive zip in /tmp (NAS RAM disk) -> done: unreachable, fetch_intermittent_month has no caller since S3-01 (grep src/)"
  - "A parse failure after a successful Next_Day download is not cached, so a short in-window month would re-download ~215 MB each run -> done: accepted; heal fires only on a short archive month (one in 26 cached); revisit if it recurs"
  - "Daily CF chart: drag on its second x axis doesn't expand (verify-interactions 14/15, fails the same on main) -> tracked: proposed session 'AEMO credit: fix Daily CF axis drag and 1024px app bar' (task_a592d2ae)"
  - "App bar overflows at 1024px (1265 vs 1209 on main, slightly worse after #21) -> tracked: same proposed session"
  - "AGENTS.md Facts say deep links are #/<DUID> (they are #DUID) and omit #portfolio= -> blocked: AGENTS.md edits need the owner's yes; Brain To-Do ca3d17aa9350897b4"
  - "Price distribution panel ignores the commissioning switch -> done: accepted; price_distribution is whole-history (bins, mwh) with no month axis, so the browser cannot filter it"
  - "Station econ_curtailment_pct now energy-weighted; published station files change at the next lane run -> tracked: tonight's 21:15Z daily run; test_portfolio_js's oracle then compares econ (it asserts once any station is marked energy)"
  - "Round-2 review: the econ fixture could not fail under nameplate weights (my first mutation claim was wrong) -> done: c964d1f, fails in JS (0.1267) and pipeline (0.1378)"
  - "Rolling 12-month trace crashed renderCharts for sets under 12 months (latent on main at #MULWASF1) -> done: c964d1f, guarded; live page renders 11 charts"
outstanding:
  - "to-do merge cutout-z/agent-contracts#175; blocked on: the owner's merge (land refuses agent-contracts)"
  - "to-do [mac-local] approve the AGENTS.md Facts fix (deep links #DUID; add #portfolio=); blocked on: the owner's yes for AGENTS.md edits (Brain To-Do ca3d17aa9350897b4)"
---

# DISPATCHLOAD tail gap, September 2025

## Cause

AEMO's MMSDM archive `PUBLIC_ARCHIVE#DISPATCHLOAD#FILE01#202509010000` (the only DISPATCHLOAD file
for that month) ends at `2025/09/30 04:00`, followed by `END OF REPORT` after 4,417,392 rows. The
October file starts at `2025/10/01 00:05`, so the 240 intervals from 30 Sep 04:05 to 1 Oct 00:00
are in neither. With no AVAILABILITY there, `curtailment_actual_mwh` and
`curtailment_potential_mwh` for 2025-09 cover 29.2 of 30 days. As a result, `curtailment_actual_mwh`
differs from `generation_mwh` by more than 1% for 94.8% of wind and solar units that month, against
at most 2.4% in any other month.

Of every cached DISPATCHLOAD, DISPATCH_UNIT_SCADA and DISPATCHPRICE month on the NAS lane (Aug 2024
to Sep 2026), only this one does not end at the next month's 00:00.

## Change (PR 19, merged 4ee4ab9)

`heal_dispatchload_gaps()` in `src/download_scada.py` runs after each DISPATCHLOAD fetch in the
month loop. For a month that ended at least two days ago and has some archive rows, it fills whole
missing intervals from NEMWEB Next_Day_Dispatch.

nemosis `NEXT_DAY_DISPATCHLOAD` reads only `Reports/Current`, so archived days come from the monthly
`Reports/ARCHIVE/Next_Day_Dispatch` bundle (~215 MB), staged under `data/`. The rows are cached per
market day as `data/next_day_unit_solution_<YYYYMMDD>.feather`. A failed fetch logs and keeps the
archive rows.

## Evidence

- Real data, run under pandas 2.3.3 and under the lane's pinned pandas 3.0.3 / pyarrow 24:
  - 240 of 240 intervals filled, 529 units per interval (the archive's own count), no duplicate keys;
  - 29 Sep market day (in both sources): 151,872 of 151,872 AVAILABILITY values equal.
- Tests: 11 new tests. All fail with `src/` reverted and all pass with it. Full suite: 588 passed. The
  22 non-passes are the known `tests/test_outputs.py` set, and `land`'s gate reported the same 22 on
  `origin/main`.
- Review: an adversarial read-only pass returned PASS with changes. Its low items are fixed in
  aaad3ac31, and its pandas 3 item is covered by the runs above.

## What's left

Settled months are not rewritten, so the published Sep 2025 figures are unchanged until a deliberate
`--months-back 14` run on the NAS lane. Estimated effect (lane SCADA joined to the healed intervals):
- per-unit curtailment moves by a median of −0.1 percentage points (5th to 95th percentile −1.4 to
  +0.8, extremes −2.3 and +3.3);
- fleet curtailment goes from 17.82% to 17.71%.

That run also recomputes 13 other settled months, so it should be diffed before it is published.
This is a decision for the owner (card above).

Ruling: healed only whole intervals missing for every unit, not per-unit holes, because the
observed gap is an archive truncation; if wrong, a unit-level hole stays unfilled, as before.

## Follow-up, 28 Oct 2026: curtailment energy fields (#20)

The owner approved the reprocess and PR 1 of the portfolio plan. #20 adds monthly
`curtailment_actual_mwh` and `curtailment_potential_mwh` to every unit and station JSON, resolved by
`_curtailment_energy_columns`. The summing rule is `max(0, 1 − Σactual/Σpotential)`, with the clip applied
after summing, because actual exceeds potential in 1,150 of 10,252 unit-months. Evidence: the full suite
gave 591 passed, and all 22 non-passes are the known `tests/test_outputs.py` set. The adversary's verdict
was "changes" (the clip rule was unstated); after the fix it was "pass", quoted in the merge commit.

The owner chose (a) for the commissioning default in the portfolio view: include every month, with a
switch to exclude the months before full commercial use, because the financial models already account
for commissioning. This binds PR 2, which is not started.

Ruling: the reprocess run pushes as normal (`PUSH_CHANGES=1`) and is diffed month by month afterwards,
not held unpushed, because the lane pulls with `--ff-only || reset --hard origin/main`. An unpushed
commit would be silently dropped the next time `main` moves. If wrong, a month outside Sep 2025 reaches
the public site until it is reverted.

## Sep 2025 reprocess, 28 Oct 2026

The owner ran `--months-back 14` from the QNAP admin shell, on code at bfb8f67 (#19 and #20). The log shows
the heal fired: `DISPATCHLOAD 2025-09: filled 240 of 240 missing intervals (126,960 rows) from
Next_Day_Dispatch`. The guard reported `47 protected months unchanged`; 616 tests passed on the NAS, and the
run pushed 65c4b1c.

Diff bfb8f67 → 65c4b1c:
- `monthly_aggregates.feather`: only rows for 2025-09 changed (190 units), and only in the four curtailment
  columns (actual 190, potential 189, pct 174, econ 166). No row was added or removed, and no column changed.
- The FCAS aggregates, FCAS factors, offer curves and offer factors caches were rewritten, but every row is
  identical.
- 495 unit and station JSONs: every one that publishes `curtailment_pct` now carries the two new arrays.
  The only existing values that changed are in 2025-09 (`curtailment_pct` 187, `econ_curtailment_pct` 179).
  No other top-level key changed. `curtailment_by_fy.csv` and `run_status.json` also changed.
- Live site: for HD1WF1 in Sep 2025, actual is 31,328.4 MWh, which matches generation of 31,328 MWh
  (30,538.7 before). The page at `#HD1WF1` renders all 12 charts, and the Sep 2025 point reads the new value.

## Portfolio view (#21), 28 Oct 2026

The owner said "Yes, build PR2". #21 adds a portfolio view to the page. Viewers pick units or stations from
the existing public list, and the browser combines their published unit files (`assets/js/portfolio.js`,
`Portfolio.combine`). The pipeline knows nothing about portfolios. The link carries only DUIDs
(`#portfolio=A,B`); the name stays in the viewer's own browser (localStorage `gcd:portfolios`), and Copy
link warns that the link shows membership. No presets ship. Two panels are new: Generation by Asset and
Capacity Factor by Asset. Every panel states its coverage ("N of M units"). Exports get an Assets
section and a `portfolio-<name>` filename.

One pipeline change: station `econ_curtailment_pct` is now energy-weighted, as the total already was
(`econ_curtailment_weighting: "energy"`), so a one-station portfolio matches its station page. Station
values shift slightly at the next lane run.

Evidence: the combiner rebuilds the 63 published stations that have monthly data with 0 mismatches. A
fixture test proves browser econ equals pipeline econ, and it fails under three mutations. The full suite
gave 596 passed; all 22 non-passes are the known `tests/test_outputs.py` set (land's gate confirmed the same 22 on main). verify-design passed.
verify-interactions passed 14 of 15 (`chartDailyCF.xaxis2` also fails on main). Browser checks covered
both themes at desktop and phone width, with screenshots in `design/screens/portfolio-*`.

Ruling: "months before full commercial use" uses the unit's own Generation Information row; for a
multi-stage project with no own row it uses the operating rows, and it ignores an In Service date later
than the latest published month. I chose this because taking the latest date across all rows wiped
ERB01's history with a later stage's 2027 date. If wrong, a few units' early months are mislabelled, and
only when the switch is on.
Ruling: the stats bar shows from 1600px, and the Portfolio button is icon-only from 1024 to 1599px,
because the app bar already overflowed at 1440 on main and the new button made it worse. If wrong, the
stats bar is hidden on 1440–1599px screens.

Review: the adversary ran two rounds, both "changes". Round 1 found M1 (wrong full-commercial-use row), M2 (an econ
comparison that could never fail) and L1 to L3; all were fixed in a16e8c4. Round 2 found no H. Its two Ms
(a rolling-average crash under 12 months, and a fixture that was proportional to nameplate) and one L
(one-row register blocks) were fixed in c964d1f. The round-2 verdict is quoted in the merge commit.

Live check (5e316f8 on Pages): `#portfolio=CLRKCWF1,WTAHB1,MULWASF1` renders 11 charts, including Generation by Asset
and Capacity Factor by Asset, with "3 of 3 units loaded". With the switch on, Generation goes from 25 to 4 months.
No console errors.
