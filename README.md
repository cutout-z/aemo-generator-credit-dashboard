# AEMO Generator Credit Dashboard

A credit risk analysis tool for Australian NEM (National Electricity Market) generators. Aggregates 5 years of operational data from AEMO to compute monthly generation, revenue, capacity factor, curtailment, MLF trajectories, price capture, FCAS participation, regional price spreads, binding network constraints, and LGC eligibility for registered generators.

**Dashboard**: [cutout-z.github.io/aemo-generator-credit-dashboard](https://cutout-z.github.io/aemo-generator-credit-dashboard/)

---

## Data Sources

### 1. AEMO NEM Registration & Exemption List
- **What**: Generator metadata — DUID, station name, fuel type, capacity, technology, region, connection point
- **Source**: [AEMO Registration List (.xls)](https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls)
- **Update**: Refreshed by the weekly reference-data lane, or on full refresh

### 2. AEMO MMSDM DUDETAILSUMMARY
- **What**: Transmission Loss Factors (MLFs) per generator per financial year, plus connection point IDs
- **Source**: AEMO NEMWeb Data Archive (MMSDM monthly packages)
- **Coverage**: FY15-16 to current FY (~11 years)
- **Update**: Auto-probes for latest available month

### 3. AEMO Draft MLFs
- **What**: Indicative/draft MLFs for the upcoming financial year
- **Source**: AEMO Loss Factors publications (Excel workbook with per-region sheets)
- **Coverage**: Next FY only (published ~March each year)
- **Update**: Checked by the daily market-data lane and forced by the annual MLF lane; shown as distinct "Draft" marker on MLF chart

### 4. NEMOSIS Dynamic Data
- **What**: 5-minute interval operational data via [NEMOSIS](https://github.com/UNSW-CEEM/NEMOSIS) (AEMO's public data API wrapper)
- **Tables**:
  - `DISPATCH_UNIT_SCADA` — actual generation output (MW) per DUID
  - `DISPATCHPRICE` — regional spot price (RRP) and FCAS prices (8 markets), AUD/MWh
  - `DISPATCHLOAD` — unconstrained availability (UIGF) for curtailment calculation
  - `BIDPEROFFER_D` — daily bid/offer data per DUID, used for per-DUID FCAS participation factors (services offered, share of intervals offering, average/peak offered MW by service)
- **Coverage**: Rolling 5 years of history
- **Update**: Daily incremental on the NAS (last 2 months reprocessed to capture late-arriving data)

### Data Capture

The target production model is frequency-driven scheduled automation on the NAS. Daily market-data runs reprocess the recent overlap window, weekly reference-data runs refresh generator metadata, and an annual MLF lane forces lightweight loss-factor publication checks. The validated processed history is published as a compact `docs/data/processed-cache` snapshot so cold runners can restore settled monthly facts without rebuilding the full raw AEMO history. NEMOSIS still handles its own large raw-data cache on persistent NAS storage for backfills and recent source refreshes.

**Incremental mode** (default): restores the settled processed-cache snapshot, reprocesses only the recent mutable overlap window, verifies that older settled months are unchanged, then republishes the compact snapshot.

**Full refresh** (`--full-refresh`): exceptional audit/remediation mode only. Routine automation should not use it; historical data is treated as settled unless a deliberate audited methodology change requires rewriting it.

### Future data sources

`docs/FUTURE_DATA_SOURCES.md` tracks not-yet-built sources: BIDDAYOFFER energy offer curves, AEMO Generation Information quarterly, AER wholesale performance and rebidding reports, ASX electricity futures, network outage data, ST/MT PASA forecasts, FPP-era data, and the participant-only prudential data gap.

---

## Calculation Methodology

All metrics are computed at monthly granularity from 5-minute interval data.

| Metric | Formula | Notes |
|--------|---------|-------|
| **Generation (MWh)** | `sum(SCADAVALUE) / 12` | 5-min MW readings converted to MWh. Negatives clipped to zero. |
| **Implied 100% Merchant Revenue (AUD)** | `sum(SCADAVALUE / 12 × RRP × MLF)` | Revenue assuming 100% merchant (no PPA hedge). MLF adjusts for transmission losses. Excludes FCAS and LGC income. |
| **Capacity Factor (%)** | `Generation_MWh / (Nameplate_MW × Hours_in_Month)` | Ratio of actual to theoretical maximum output. |
| **Grid Curtailment (%)** | `1 - (Actual_SCADA / Unconstrained_AVAILABILITY)` | Solar and wind only. Total curtailment uses AEMO's UIGF forecast as the unconstrained baseline. From August 2024, split into grid vs. mechanical using `INTERMITTENT_GEN_SCADA` quality flags (see below). |
| **Estimated Economic Curtailment (%)** | `Forgone generation during RRP < $0 / Total UIGF` | Solar and wind only. Proxy for voluntary bid-off during negative price periods. |
| **Captured Price (AUD/MWh)** | `sum(SCADAVALUE × RRP) / sum(SCADAVALUE)` | Volume-weighted average price received when actually generating. |
| **Avg Regional RRP (AUD/MWh)** | `mean(RRP)` | Time-weighted average spot price for the generator's region. |
| **Price Capture Ratio** | `Captured_Price / Avg_RRP` | >1.0 = captures premium prices. <1.0 = captures below-average prices (common for solar). |
| **Price Distribution** | Generation-weighted histogram across 6 bins | Bins: `<0`, `0–50`, `50–100`, `100–200`, `200–300`, `300+` AUD/MWh. |
| **LGC Eligibility** | `fuel_type in {Solar, Wind, Hydro, Other Renewable}` | For eligible generators, 1 MWh ≈ 1 LGC created. Volume only, no revenue estimation. |

### Key concepts

- **MLF (Marginal Loss Factor)**: Adjusts generator revenue for transmission losses. Typical range 0.95–1.00. A lower MLF means more energy lost in transmission, reducing effective revenue.
- **Draft MLF**: AEMO publishes indicative MLFs for the upcoming FY around March each year. Shown as a distinct marker on the MLF trajectory chart.
- **Intervention filtering**: AEMO manual market interventions (`INTERVENTION != 0`) are excluded from price and dispatch data (~0.5% of records).
- **Financial year convention**: July 1 to June 30. MLFs are published per FY.
- **FCAS prices**: 8 regional FCAS markets (Raise/Lower × 6s/60s/5min/Regulation) shown as context and labelled **regional average** — they describe the market, not the generator. Per-generator FCAS participation factors are estimated from bid offers (BIDPEROFFER_D); actual enablement and revenue are participant-only data and not estimated.

### Curtailment methodology note

Total curtailment is calculated as `1 - (SCADA / AVAILABILITY)` from the DISPATCHLOAD table, comparing actual output to AEMO's unconstrained intermittent generation forecast (UIGF).

From **August 2024 onwards**, the pipeline uses AEMO's `INTERMITTENT_GEN_SCADA` table to split total curtailment into two components:
- **Grid curtailment**: intervals where the `SCADA_QUALITY` flag on `ELAV` (electrical availability) records is "Good" — the generator was mechanically available but constrained off by the network
- **Mechanical curtailment**: intervals where the quality flag is non-Good — indicating mechanical downtime or communications issues

The split is proportional: if 80% of intervals have "Good" quality, then 80% of total curtailment is attributed to grid constraints and 20% to mechanical causes. For months **before August 2024**, only total (unsplit) curtailment is available.

### Regional Price Spreads (BESS arbitrage) methodology

The **Regional Price Spreads** panel is a market-level view — the same series applies to every unit in the region — estimating the spread a battery could capture by charging in the cheapest intervals of the day and discharging in the most expensive. Each day has 288 five-minute intervals (24 h × 12). For each day and region, `DISPATCHPRICE` intervals are ranked by price and two capture windows are defined — the top (highest-priced) and bottom (lowest-priced) **duration × 12** five-minute intervals:

| Duration | Window size (5-min intervals) | Fraction of day |
|----------|-------------------------------|-----------------|
| 1h | 12 | 1/24 |
| 2h | 24 | 1/12 |
| 4h | 48 | 1/6 |
| 8h | 96 | 1/3 |
| Decile | fixed top/bottom 10% of intervals | ≈ 2.4h equivalent |

The daily spread is the top-window VWAP minus the bottom-window VWAP (equal weighting per 5-minute interval). Because wider windows necessarily blend progressively more mid-priced intervals, the realisable spread **declines monotonically with duration** — 1h ≥ 2h ≥ 4h ≥ 8h for the same day and region. This is a structural property of the method, not a market signal.

The **Decile** option (fixed top/bottom 10% of intervals, ≈ 2.4h) is the legacy 1–2h battery proxy, retained for continuity with the AEMO QED benchmark. The quarterly rollup in `docs/data/market_daily.json` stores per-duration quarterly average spreads plus divergence ratios against the QED NEM battery charge/discharge spread:

| Quarter | QED benchmark spread (AUD/MWh) |
|---------|-------------------------------|
| 2025Q2 | 342 |
| 2026Q1 | 121 |
| 2026Q2 | 51 |

Data: `docs/data/market_daily.json` → `regions.<code>.by_duration.{1h,2h,4h,8h}` with `vwap_high[]`, `vwap_low[]` and `spread[]` daily arrays, plus the legacy decile fields.

### FCAS participation factors (BIDPEROFFER_D)

Per-DUID FCAS participation is derived from AEMO **BIDPEROFFER_D** daily offer data:
- **Services offered** — which of the 8 FCAS markets (Raise/Lower × 6s/60s/5min/Reg) the DUID offers into
- **Share of intervals offering** — how consistently each service is offered
- **Average / peak offered MW** per service

These appear as a summary box on the generator card. The regional FCAS price chart is explicitly labelled **scope: regional average** — it describes the market the generator operates in, not the generator's own FCAS behaviour or revenue. Actual enablement and FCAS revenue remain participant-only data.

**Era note**: from **8 June 2025** the NEM is in the FPP (Frequency Performance Payments) era — AEMO's causer-pays global FCAS factors ceased — so participation semantics before and after mid-2025 are not directly comparable.

### Binding network constraints — credit translation

The Binding Network Constraints panel maps the generator's connection point to constraints via `SPDCONNECTIONPOINTCONSTRAINT` and counts binding hours (marginal value > 0) from `DISPATCHCONSTRAINT`. A credit-translation summary callout then:

- **Detects own-unit commissioning hold-point constraints** of the form `C_N_<DUID>_<MW>` and compares the cap against the unit's nameplate — a cap below nameplate signals reduced available capacity during commissioning, a direct input to credit capacity assessment
- **Classifies each constraint** from the AEMO ID taxonomy: **Own unit** (the DUID's own constraint), **Commissioning** (hold-point caps), **Non-conformance**, and **System** — where `N>` / `N>>` are system-normal trip constraints
- **Readable bar labels**, with the raw constraint ID preserved in the hover tooltip

---

## Dashboard

A single-page static site built with vanilla HTML/CSS/JS and [Plotly.js](https://plotly.com/javascript/) for charting. No build step or framework.

### Features
- **Search**: Real-time autocomplete by station name or DUID, with region and fuel type filters
- **Station aggregation**: Multi-DUID stations (e.g. Clarke Creek Wind Farm) appear as a single aggregated entry with summed generation/revenue, station-level curtailment/constraint charts, and per-DUID MLF traces
- **Generator card**: DUID, station, region, fuel type, technology, capacity, connection point
- **Time selector**: 3M / 6M / 12M / 3Y / 5Y / All (does not affect MLF chart)
- **Duration selector**: The Regional Price Spreads panel parameterizes the capture window (1h / 2h / 4h / 8h / Decile)
- **FCAS participation summary box**: Per-DUID services offered, share of intervals offering, and average/peak offered MW from BIDPEROFFER_D
- **Constraints classification**: Binding constraints classified as Own unit / Commissioning / Non-conformance / System, with a credit-translation callout comparing commissioning caps to nameplate
- **Scope labelling**: Market-level panels are explicitly labelled (regional average / applies to every unit in the region) so they are not mistaken for generator-level data
- **Methodology tooltips**: Hover over any chart title for formula, methodology, and caveats
- **URL hashing**: Bookmark any generator directly (e.g. `#CLRKCWF1`)

### Charts (12 per-generator panels + 1 market-level panel)
1. **Implied 100% Merchant Revenue** — monthly bar chart (AUD), assumes no PPA hedge
2. **Monthly Generation** — bar chart (MWh), annotated with LGC equivalence for eligible renewables
3. **Generation (Last 12 Months)** — daily bars (MWh) colour-coded by capacity-factor band, with a daily CF line overlay
4. **Capacity Factor** — line chart with 25% reference line
5. **Grid Curtailment Analysis** — area chart (solar/wind only)
6. **Estimated Economic Curtailment** — area chart showing generation forgone during negative price periods (solar/wind only)
7. **Mechanical Outage** — share of curtailment attributable to mechanical/comms issues from `INTERMITTENT_GEN_SCADA` quality flags
8. **MLF Trajectory** — annual line chart with draft FY marker (diamond symbol). Station view shows per-DUID traces
9. **Price Capture** — dual overlay of captured price vs regional average RRP
10. **Spot Price Exposure** — horizontal bar showing generation share across price bins
11. **Regional FCAS Prices** — 8 FCAS market price lines for the generator's NEM region, labelled scope: regional average
12. **Binding Network Constraints** — hours bound per constraint, classified (Own unit / Commissioning / Non-conformance / System) with a credit-translation callout; readable labels with raw IDs in tooltips
13. **Regional Price Spreads (BESS Arbitrage)** — market-level panel (applies to every unit in the region): daily top-vs-bottom capture-window spread, duration-parameterized (1h / 2h / 4h / 8h / Decile)

---

## Running Locally

### Prerequisites
- Python 3.11+
- Dependencies: `pip install -r requirements.txt`

### Commands

```bash
# NAS daily lane (production schedule)
python -m src.main --months-back 2 --refresh-mlf --skip-constraints
pytest          # full suite: 6 test files, 56 tests

# Incremental update (last 2 months)
python -m src.main

# Exceptional audited rebuild only (not for routine automation)
python -m src.main --full-refresh

# Custom lookback
python -m src.main --months-back 6

# Metadata only (regenerate JSON from cached aggregates)
python -m src.main --metadata-only

# Refresh generator metadata without a full historical rebuild
python -m src.main --months-back 2 --refresh-metadata

# Refresh MLF tracker data without a full historical rebuild
python -m src.main --skip-scada --skip-constraints --refresh-mlf

# Skip SCADA download (use cached data)
python -m src.main --skip-scada
```

### View dashboard locally
```bash
open docs/index.html
```

---

## Deployment

Hosted on **GitHub Pages** from the `docs/` directory. The NAS runs the daily data lane and pushes changed `docs/data` files to `main`; the lightweight `deploy-pages.yml` workflow then publishes the site.

### Automated schedule
- **Daily market data**: NAS daily lane runs `python -m src.main --months-back 2 --refresh-mlf --skip-constraints`, then the full test suite (`pytest` — 6 files, 56 tests), then commits and pushes `docs/data`
- **Weekly reference data**: NAS scheduled lane refreshes generator registration metadata plus MLF tracker data without forcing a full historical rebuild
- **Annual MLF lane**: NAS scheduled lane forces a lightweight MLF refresh around final MLF publication season without touching SCADA or constraints
- **Manual trigger**: GitHub Actions remains available via `workflow_dispatch` for verification/fallback, but the NAS is the primary scheduled data runner for normal bounded-cache automation

See `deploy/README.md` for runner setup details.

---

## Data Quality & Pipeline Safeguards

The daily lane enforces systematic quality gates — a failure stops the run and alerts rather than publishing suspect data:

- **Freshness guards**: the pipeline hard-fails if the latest monthly aggregate is older than 75 days or the latest daily data older than 60 days; a Mac-side alert (via autopull) flags staleness
- **Fuel-aware daily capacity-factor bounds**: daily CF is checked against hard bounds — hydro 1.25, non-hydro 1.10 — a breach fails the run
- **Monthly CF > 1.0 audit**: `src/audit_cf.py` lists units needing investigation; the current list covers TAS/NSW hydro peakers KAREEYA1–4, POAT110 and FISHER, plus gas units BW02, OSB-AG and QPS3 under review
- **BARRON correction**: BARRON-1/2 capacity corrected 21 → 33.2 MW with a retroactive history fix (see below)
- **Test suite**: a full 6-file test suite (56 tests) runs in the daily NAS lane after the data update; failures block the commit + push
- **Resolved audits**: the Apr-2026 WANDSF1/EMERASF1 finding was resolved as regional-average labelling semantics (the data describes the market, not the unit)

---

## Project Structure

```
├── src/
│   ├── main.py                 # Pipeline orchestrator
│   ├── config.py               # Constants, URLs, fuel type mappings
│   ├── download_metadata.py    # AEMO registration list parser
│   ├── download_mlf.py         # MLF history + connection points
│   ├── download_draft_mlf.py   # Draft/indicative MLF download
│   ├── download_scada.py       # NEMOSIS SCADA + dispatch load
│   ├── download_dispatch.py    # NEMOSIS dispatch prices + FCAS + bid offers
│   ├── aggregate.py            # Monthly metric calculations + FCAS aggregation
│   ├── audit_cf.py             # Capacity factor audit — flags stale registrations
│   └── generate_json.py        # JSON output + station aggregation
├── tests/                      # Test suite (6 files, 56 tests) — runs in the daily lane
├── docs/
│   ├── index.html              # Dashboard SPA
│   ├── FUTURE_DATA_SOURCES.md  # Not-yet-built data source backlog
│   └── data/
│       ├── index.json          # Generator + station search index
│       ├── market_daily.json   # Market-level daily price-spread series (by duration) + quarterly rollup
│       ├── generators/         # Per-generator and per-station JSON files
│       └── processed-cache/    # Compact settled-history cache snapshot
├── data/                       # Local cache (gitignored)
│   ├── *.feather               # Working processed data cache
│   └── nemosis_cache/          # Raw AEMO data cache
├── deploy/                     # NAS runner scripts and schedule
├── .github/workflows/
│   ├── deploy-pages.yml        # GitHub Pages deployment
│   └── monthly-update.yml      # Manual/fallback CI data runner
└── requirements.txt
```

---

## Why no WEM (Western Australia)?

This dashboard covers only the NEM (National Electricity Market — NSW, QLD, VIC, SA, TAS). The WA Wholesale Electricity Market (WEM) is excluded because AEMO stopped publishing public facility-level generation data after the WEM Reform went live on 1 October 2023.

**What happened**: Before the reform, AEMO published monthly facility SCADA CSVs at `data.wa.aemo.com.au/datafiles/facility-scada/`. The last file covers 1 October 2023 only (513 KB vs the typical ~7 MB for a full month). No replacement public dataset was created. The `operational-measurements`, `balancing-summary`, and `load-summary` directories all stopped at the same date.

**What still exists**: AEMO continues to publish system-level aggregate generation (`tt30gen`) and some STEM/bidding data, but nothing with per-facility generation. The WEM Data Dashboard on aemo.com.au is a view-only Power BI embed with no downloadable data. The AEMO API portal (`dev.aemo.com.au`) has WEM APIs for bids, dispatch instructions, and settlement — but these are participant-only (require AEMO registration and accreditation) and don't include metered SCADA.

**Open Electricity** (`api.openelectricity.org.au`) may carry post-reform WEM facility data, but requires an API key and its upstream source for post-reform data is unverified.

This dashboard previously included pre-reform WEM data (Jul 2012 – Sep 2023) but it was removed because frozen historical data without ongoing updates provides limited credit risk value.

---

## Capacity Factor > 1.0 and Registration Overrides

Some generators report monthly capacity factors exceeding 1.0, meaning their actual SCADA output exceeds the nameplate capacity recorded in AEMO's NEM Registration List. These fall into two categories:

### Stale registrations (corrected via overrides)

Generators that have been physically uprated but whose registration data was never updated. These are corrected in `config.py` via `CAPACITY_OVERRIDES`:

| DUID | Registered MW | Override MW | Reason |
|------|--------------|-------------|--------|
| HUMENSW | 29 | 58 | Both Hume NSW units dispatch under one DUID; registered at single-unit capacity |
| LOYYB1 | 500 | 580 | Loy Yang B Unit 1 uprated; AEMO constraint `#LOYYB1_E1` caps at 580 MW |
| LOYYB2 | 500 | 580 | Loy Yang B Unit 2 uprated; peak SCADA consistently ~585 MW |
| BARRON-1/2 | 21 | 33.2 | Capacity corrected; retroactive history fix applied (peak SCADA consistently above registered capacity) |

The pipeline runs an automated audit (`src/audit_cf.py`) after each aggregation that flags DUIDs with CF > 1.0 in three or more months, reporting whether they are already overridden or need investigation.

### Hydro headwater effect (not corrected — real physics)

Hydro generators can physically exceed their nameplate capacity when reservoir head (water level) is high. Higher head increases the pressure drop across the turbine, producing more power from the same flow rate. This is normal operating behaviour, not a data error.

The following hydro DUIDs regularly show CF > 1.0 and are **intentionally not overridden**:

| DUID | Registered MW | Typical Overrun | Months > 1.0 | Location |
|------|--------------|-----------------|---------------|----------|
| KAREEYA1–4 | 21 each | ~2–5% | 7–10/61 | Tully Falls, QLD |
| POAT110 | 100 | ~5–10% | 4/61 | Poatina, TAS |
| FISHER | 43 | ~5% | 3/61 | Fisher, TAS |
| LEM_WIL | 82 | ~5% | 2/61 | Lemonthyme/Wilmot, TAS |
| REPULSE | 28 | ~15% | 2/61 | Repulse, TAS |

The current CF > 1.0 audit list covers the TAS/NSW hydro peakers KAREEYA1–4, POAT110 and FISHER (headwater effects, above) plus gas units **BW02, OSB-AG and QPS3 under review**. Remaining single-month CF > 1.0 instances on fossil and other hydro generators (BW01, LYA1/3/4, YWPS2/3, etc.) are transient events — brief SCADA overshoots or unusual dispatch conditions — and do not warrant overrides.

---

## Known Limitations

- **Revenue is 100% merchant assumption**: Does not include PPA, FCAS, or LGC income — useful as a stress-test floor, not actual revenue
- **Pre-Aug 2024 curtailment is unsplit**: Before August 2024, curtailment cannot be separated into grid vs. mechanical components (INTERMITTENT_GEN_SCADA data not available)
- **Economic curtailment is estimated**: Based on RRP < $0 proxy — cannot distinguish voluntary bid-off from AEMO dispatch instructions without bid data
- **FCAS participation factors are offer-based estimates**: Derived from BIDPEROFFER_D offers (services offered, offered MW) — actual enablement, output and revenue require participant-only data. The regional FCAS price chart is regional-average by design
- **FPP-era discontinuity**: Causer-pays global FCAS factors ceased 8 June 2025; participation semantics differ before/after
- **Spreads assume perfect capture**: The Regional Price Spreads panel assumes a battery captures the full top/bottom window at the window average — real arbitrage is eroded by round-trip efficiency, state-of-charge limits and bidding. Wider durations blend mid-priced hours, so realisable spread falls monotonically with duration
- **Decile is a legacy proxy**: The ~2.4h decile option is retained for continuity with the AEMO QED benchmark, which has itself fallen sharply (2025Q2 $342 → 2026Q2 $51/MWh)
- **LGC volumes are estimated**: 1 MWh ≈ 1 LGC for eligible generators — actual creation may differ due to station use and accreditation
- **Connection point gaps**: ~20% of generators lack connection point data in DUDETAILSUMMARY (constraint mapping is approximate)
- **MLF fallback**: If exact FY data is missing for a generator, the latest available FY is used
- **Data lag**: AEMO data has a ~2 week lag; the 2-month reprocessing window accounts for this
- **Prudential data gap**: AEMO's own credit and prudential data is participant-only; this dashboard approximates credit exposure from public market data (see `docs/FUTURE_DATA_SOURCES.md`)
