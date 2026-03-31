# AEMO Generator Credit Dashboard

A credit risk analysis tool for Australian NEM (National Electricity Market) generators. Aggregates 5 years of operational data from AEMO to compute monthly generation, revenue, capacity factor, curtailment, MLF trajectories, and price capture profiles for all 559 registered generators.

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

### 3. NEMOSIS Dynamic Data
- **What**: 5-minute interval operational data via [NEMOSIS](https://github.com/UNSW-CEEM/NEMOSIS) (AEMO's public data API wrapper)
- **Tables**:
  - `DISPATCH_UNIT_SCADA` — actual generation output (MW) per DUID
  - `DISPATCHPRICE` — regional spot price (RRP, AUD/MWh)
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
| **Spot Revenue (AUD)** | `sum(SCADAVALUE / 12 × RRP × MLF)` | Revenue at 5-min granularity. MLF adjusts for transmission losses. **Spot market only** — excludes PPAs, FCAS, and LGCs. |
| **Capacity Factor (%)** | `Generation_MWh / (Nameplate_MW × Hours_in_Month)` | Ratio of actual to theoretical maximum output. |
| **Curtailment (%)** | `1 - (Actual_SCADA / Unconstrained_AVAILABILITY)` | Solar and wind only. Measures energy lost to grid constraints using AEMO's UIGF forecast as the unconstrained baseline. |
| **Captured Price (AUD/MWh)** | `sum(SCADAVALUE × RRP) / sum(SCADAVALUE)` | Volume-weighted average price received when actually generating. |
| **Avg Regional RRP (AUD/MWh)** | `mean(RRP)` | Time-weighted average spot price for the generator's region. |
| **Price Capture Ratio** | `Captured_Price / Avg_RRP` | >1.0 = captures premium prices (e.g. dispatchable plant). <1.0 = captures below-average prices (e.g. solar during midday glut). |
| **Price Distribution** | Generation-weighted histogram across 6 bins | Bins: `<0`, `0–50`, `50–100`, `100–200`, `200–300`, `300+` AUD/MWh. Shows what share of generation occurs in each price band. |

### Key concepts

- **MLF (Marginal Loss Factor)**: Adjusts generator revenue for transmission losses. Typical range 0.95–1.00. A lower MLF means more energy lost in transmission, reducing effective revenue.
- **Intervention filtering**: AEMO manual market interventions (`INTERVENTION != 0`) are excluded from price and dispatch data (~0.5% of records).
- **Financial year convention**: July 1 to June 30. MLFs are published per FY.

---

## Dashboard

A single-page static site built with vanilla HTML/CSS/JS and [Plotly.js](https://plotly.com/javascript/) for charting. No build step or framework.

### Features
- **Search**: Real-time autocomplete by station name or DUID, with region and fuel type filters
- **Generator card**: DUID, station, region, fuel type, technology, capacity, connection point
- **Time selector**: 3M / 6M / 12M / 3Y / All (does not affect MLF chart)
- **URL hashing**: Bookmark any generator directly (e.g. `#CLRKCWF1`)

### Charts
1. **Estimated Spot Revenue** — monthly bar chart (AUD)
2. **Capacity Factor** — line chart with 25% reference line
3. **Curtailment Analysis** — area chart (solar/wind only, hidden for other fuel types)
4. **MLF Trajectory** — annual line chart across all available financial years
5. **Price Capture** — dual overlay of captured price vs regional average RRP
6. **Spot Price Exposure** — horizontal stacked bar showing generation share across price bins

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
│   ├── download_scada.py       # NEMOSIS SCADA + dispatch load
│   ├── download_dispatch.py    # NEMOSIS dispatch prices
│   ├── aggregate.py            # Monthly metric calculations
│   └── generate_json.py        # JSON output for dashboard
├── docs/
│   ├── index.html              # Dashboard SPA
│   └── data/
│       ├── index.json          # Generator search index (563 entries)
│       └── generators/         # Per-generator JSON files
├── data/                       # Local cache (gitignored)
│   ├── *.feather               # Processed data cache
│   └── nemosis_cache/          # Raw AEMO data cache
├── .github/workflows/
│   └── monthly-update.yml      # Monthly CI/CD pipeline
└── requirements.txt
```

---

## Known Limitations

- **Spot revenue only**: Does not include PPA, FCAS, or LGC income — incomplete for full credit assessment
- **No ramp rate or regulation data**: Doesn't capture dispatchability constraints beyond curtailment
- **Connection point gaps**: ~20% of generators lack connection point data in DUDETAILSUMMARY
- **MLF fallback**: If exact FY data is missing for a generator, the latest available FY is used
- **Data lag**: AEMO data has a ~2 week lag; the 2-month reprocessing window accounts for this
