# AEMO Generator Credit Dashboard

A credit risk analysis tool for Australian NEM (National Electricity Market) generators. Aggregates 5 years of operational data from AEMO to compute monthly generation, revenue, capacity factor, curtailment, MLF trajectories, price capture, FCAS context, and LGC eligibility for registered generators.

**Dashboard**: [cutout-z.github.io/aemo-generator-credit-dashboard](https://cutout-z.github.io/aemo-generator-credit-dashboard/)

---

## Data Sources

### 1. AEMO NEM Registration & Exemption List
- **What**: Generator metadata — DUID, station name, fuel type, capacity, technology, region, connection point
- **Source**: [AEMO Registration List (.xls)](https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls)
- **Update**: Re-downloaded on full refresh

### 2. AEMO MMSDM DUDETAILSUMMARY
- **What**: Transmission Loss Factors (MLFs) per generator per financial year, plus connection point IDs
- **Source**: AEMO NEMWeb Data Archive (MMSDM monthly packages)
- **Coverage**: FY15-16 to current FY (~11 years)
- **Update**: Auto-probes for latest available month

### 3. AEMO Draft MLFs
- **What**: Indicative/draft MLFs for the upcoming financial year
- **Source**: AEMO Loss Factors publications (Excel workbook with per-region sheets)
- **Coverage**: Next FY only (published ~March each year)
- **Update**: Downloaded on pipeline run; shown as distinct "Draft" marker on MLF chart

### 4. NEMOSIS Dynamic Data
- **What**: 5-minute interval operational data via [NEMOSIS](https://github.com/UNSW-CEEM/NEMOSIS) (AEMO's public data API wrapper)
- **Tables**:
  - `DISPATCH_UNIT_SCADA` — actual generation output (MW) per DUID
  - `DISPATCHPRICE` — regional spot price (RRP) and FCAS prices (8 markets), AUD/MWh
  - `DISPATCHLOAD` — unconstrained availability (UIGF) for curtailment calculation
- **Coverage**: Rolling 5 years of history
- **Update**: Monthly incremental (last 2 months reprocessed to capture late-arriving data)

### Data Capture

The pipeline runs monthly via GitHub Actions (18th of each month, 00:00 UTC). It can also be triggered manually. Data is cached locally in Apache Arrow Feather format to avoid redundant downloads. NEMOSIS handles its own Parquet/CSV caching for raw AEMO data.

**Incremental mode** (default): reprocesses the last 2 months and merges with existing aggregates, deduplicating overlapping months.

**Full refresh** (`--full-refresh`): re-downloads all metadata and reprocesses the full 5-year history.

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
- **FCAS prices**: 8 regional FCAS markets (Raise/Lower × 6s/60s/5min/Regulation) shown as context. Per-generator FCAS revenue is participant-only data and not estimated.

### Curtailment methodology note

Total curtailment is calculated as `1 - (SCADA / AVAILABILITY)` from the DISPATCHLOAD table, comparing actual output to AEMO's unconstrained intermittent generation forecast (UIGF).

From **August 2024 onwards**, the pipeline uses AEMO's `INTERMITTENT_GEN_SCADA` table to split total curtailment into two components:
- **Grid curtailment**: intervals where the `SCADA_QUALITY` flag on `ELAV` (electrical availability) records is "Good" — the generator was mechanically available but constrained off by the network
- **Mechanical curtailment**: intervals where the quality flag is non-Good — indicating mechanical downtime or communications issues

The split is proportional: if 80% of intervals have "Good" quality, then 80% of total curtailment is attributed to grid constraints and 20% to mechanical causes. For months **before August 2024**, only total (unsplit) curtailment is available.

---

## Dashboard

A single-page static site built with vanilla HTML/CSS/JS and [Plotly.js](https://plotly.com/javascript/) for charting. No build step or framework.

### Features
- **Search**: Real-time autocomplete by station name or DUID, with region and fuel type filters
- **Station aggregation**: Multi-DUID stations (e.g. Clarke Creek Wind Farm) appear as a single aggregated entry with summed generation/revenue and per-DUID MLF traces
- **Generator card**: DUID, station, region, fuel type, technology, capacity, connection point
- **Time selector**: 3M / 6M / 12M / 3Y / 5Y / All (does not affect MLF chart)
- **Methodology tooltips**: Hover over any chart title for formula, methodology, and caveats
- **URL hashing**: Bookmark any generator directly (e.g. `#CLRKCWF1`)

### Charts (10 panels)
1. **Implied 100% Merchant Revenue** — monthly bar chart (AUD), assumes no PPA hedge
2. **Monthly Generation** — bar chart (MWh), annotated with LGC equivalence for eligible renewables
3. **Capacity Factor** — line chart with 25% reference line
4. **Grid Curtailment Analysis** — area chart (solar/wind only)
5. **Estimated Economic Curtailment** — area chart showing generation forgone during negative price periods (solar/wind only)
6. **MLF Trajectory** — annual line chart with draft FY marker (diamond symbol). Station view shows per-DUID traces
7. **Price Capture** — dual overlay of captured price vs regional average RRP
8. **Spot Price Exposure** — horizontal bar showing generation share across price bins
9. **Regional FCAS Prices** — 8 FCAS market price lines for the generator's NEM region

---

## Running Locally

### Prerequisites
- Python 3.11+
- Dependencies: `pip install -r requirements.txt`

### Commands

```bash
# Incremental update (last 2 months)
python -m src.main

# Full rebuild (all 5 years)
python -m src.main --full-refresh

# Custom lookback
python -m src.main --months-back 6

# Metadata only (regenerate JSON from cached aggregates)
python -m src.main --metadata-only

# Skip SCADA download (use cached data)
python -m src.main --skip-scada
```

### View dashboard locally
```bash
open docs/index.html
```

---

## Deployment

Hosted on **GitHub Pages** from the `docs/` directory. The GitHub Actions workflow (`monthly-update.yml`) runs the pipeline, commits updated JSON files, and deploys automatically.

### Automated schedule
- **When**: 18th of each month, 00:00 UTC (~10am AEST)
- **What**: Incremental update (last 2 months), commit, deploy
- **Manual trigger**: Available via `workflow_dispatch` with optional `full_refresh` and `months_back` parameters

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
│   ├── download_dispatch.py    # NEMOSIS dispatch prices + FCAS
│   ├── aggregate.py            # Monthly metric calculations + FCAS aggregation
│   └── generate_json.py        # JSON output + station aggregation
├── docs/
│   ├── index.html              # Dashboard SPA
│   └── data/
│       ├── index.json          # Generator + station search index
│       └── generators/         # Per-generator and per-station JSON files
├── data/                       # Local cache (gitignored)
│   ├── *.feather               # Processed data cache
│   └── nemosis_cache/          # Raw AEMO data cache
├── .github/workflows/
│   └── monthly-update.yml      # Monthly CI/CD pipeline
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

## Known Limitations

- **Revenue is 100% merchant assumption**: Does not include PPA, FCAS, or LGC income — useful as a stress-test floor, not actual revenue
- **Pre-Aug 2024 curtailment is unsplit**: Before August 2024, curtailment cannot be separated into grid vs. mechanical components (INTERMITTENT_GEN_SCADA data not available)
- **Economic curtailment is estimated**: Based on RRP < $0 proxy — cannot distinguish voluntary bid-off from AEMO dispatch instructions without bid data
- **FCAS is regional context only**: Per-generator FCAS enablement and revenue requires participant-only data
- **LGC volumes are estimated**: 1 MWh ≈ 1 LGC for eligible generators — actual creation may differ due to station use and accreditation
- **Connection point gaps**: ~20% of generators lack connection point data in DUDETAILSUMMARY
- **MLF fallback**: If exact FY data is missing for a generator, the latest available FY is used
- **Data lag**: AEMO data has a ~2 week lag; the 2-month reprocessing window accounts for this
