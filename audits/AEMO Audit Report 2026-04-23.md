# AEMO Audit Report — 2026-04-23

## Summary
- **Projects audited**: Historical Prices, MLF Tracker, Negative Prices, Renewable Generator Dashboard, Credit Dashboard
- **Spot checks per project**: 5
- **Total checks**: ~130
- **Results**: 118 PASS, 12 P3, 2 P2, 2 P1

---

## P1 Issues

| Project | Target | Metric | Published | Re-derived | Delta | Notes |
|---------|--------|--------|-----------|------------|-------|-------|
| Credit Dashboard | WANDSF1 / EMERASF1 | FCAS revenue | Identical across both DUIDs | Should differ (different generators) | 100% duplication | WANDSF1 and EMERASF1 share identical FCAS revenue data across all 37 overlapping months and all 8 FCAS services (296/296 matches). These are different generators in different locations. Pipeline bug — FCAS was copied from one to the other. |
| Credit Dashboard | BW02 | Daily capacity_factor | Up to 1.0364 | Should be <= 1.0 | 20 days > 1.0 | 20 days in the daily timeseries exceed 100% CF (max 1.0364 on 2025-08-18). Implies dispatch of ~684 MW against 660 MW nameplate. Monthly CFs are within bounds. |

## P2 Issues

| Project | Target | Metric | Published | Re-derived | Delta | Notes |
|---------|--------|--------|-----------|------------|-------|-------|
| Credit Dashboard | TOWER | Generation data | No monthly data | Should have data if dispatched | Missing entirely | JSON contains only metadata + MLF data. Tower is a 41.2 MW waste coal mine gas plant — may be non-scheduled and not in NEMOSIS dispatch tables. |
| Credit Dashboard | BLAIRFOX_KARAKIN_WF1 | Data freshness | Latest: 2023-09 | Current data expected | ~30 months stale | WEM generator — data pipeline may have stopped, or generator decommissioned. |

## P3 Issues

| Project | Target | Metric | Published | Re-derived | Delta | Notes |
|---------|--------|--------|-----------|------------|-------|-------|
| Renewable Dashboard | CLARESF1 | NAMEPLATE_MW | 110.4 | 110.262 (NEM Reg) | 0.13% | Enrichment source vs NEM Registration List |
| Renewable Dashboard | CRWASF1 | NAMEPLATE_MW | 36.01 | 36.0 (NEM Reg) | 0.03% | Decimal precision artifact |
| Credit Dashboard | EMERASF1 (x3 months) | generation_mwh | Integer | Float (1dp) | < 0.5 MWh | JSON rounds to int; feather retains decimals |
| Credit Dashboard | WANDSF1 (x3 months) | generation_mwh | Integer | Float (1dp) | < 0.5 MWh | Same rounding pattern |
| Credit Dashboard | BW02 (x3 months) | generation_mwh | Integer | Float (1dp) | < 0.3 MWh | Same rounding pattern |
| Credit Dashboard | BLAIRFOX (x3 months) | generation_mwh | Integer | Float (1dp) | < 0.5 MWh | Same rounding pattern |

## Cross-Project Checks

| Check | Status | Details |
|-------|--------|---------|
| MLF consistency (Renewable vs MLF Tracker) | PASS | All 5 sampled DUIDs match exactly for FY25-26 and FY26-27 |
| MLF consistency (Credit vs MLF Tracker) | PASS | Capacity values match across all sampled DUIDs |
| Region completeness (Prices) | PASS | All 5 NEM regions present |
| Region completeness (Negative Prices) | PASS | All 5 NEM regions present |
| Import MLF flow-through (Renewable) | Fixed | `download_mlf.py` was picking up empty import columns — filtered out |
| Import MLF flow-through (Credit) | PASS | Regex filter correctly ignores import columns |

## Per-Project Results

### Historical Prices — 20/20 PASS
All 5 samples (VIC1/2023-02, NSW1/2022-07, NSW1/2007-10, SA1/2004-10, QLD1/2022-06) re-derived with 0.0000% relative error. Covers both 30-min (pre-Oct 2021) and 5-min interval regimes.

### MLF Tracker — 30/30 PASS
All 5 DUIDs (BLUEGSF1, TALWA1, TUMUT1-4, DUNDWF2, CROOKWF2) verified across DUDETAILSUMMARY feather + final Excel overrides. YOY_CHANGE computations exact. TUMUT3 import MLF history correctly shows NaN for recent FYs.

### Negative Prices — 45/45 PASS
All 5 samples re-derived from raw NEMOSIS dispatch price data. Counts, percentages, and arithmetic consistency all exact. Monotonicity confirmed.

### Renewable Generator Dashboard — 28/30 PASS, 2 P3
MLFs, regions, and curtailment ranges all correct. Two minor capacity rounding differences (CLARESF1 0.13%, CRWASF1 0.03%).

### Credit Dashboard — 9/15 PASS, 10 P3, 2 P2, 2 P1
JSON-to-feather consistency is good (rounding only). Two significant pipeline bugs found (FCAS duplication, daily CF > 100%). Two data coverage gaps (TOWER missing data, BLAIRFOX stale).

---

## Recommendations

1. **P1 — FCAS duplication (WANDSF1/EMERASF1)**: Investigate the FCAS data pipeline. Check whether the bug affects other generator pairs or is isolated to these two.
2. **P1 — BW02 daily CF > 1.0**: Either cap daily CF at 1.0 in the export pipeline, or investigate whether BW02's registered capacity (660 MW) is understated vs actual dispatch capability.
3. **P2 — TOWER**: Determine if non-scheduled generators should be in the pipeline at all. If not, exclude them or add a "no_dispatch_data" flag.
4. **P2 — BLAIRFOX**: Check WEM pipeline status — data stopped at 2023-09.
5. **No full pipeline re-run needed** for Prices, MLF Tracker, or Negative Prices.
