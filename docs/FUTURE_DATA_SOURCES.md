# Data Sources — Built and Future

Status of public data sources for generator credit-risk analysis. **Built**
sections document what the pipeline ingests today; **Future** sections record
researched-but-not-built sources so future work can pick them up without
re-doing the access research (all URLs below were live-verified 3 Sep 2026).

## Built

### Energy offer curves (Sep 2026)
- **What**: Per-DUID 10-band energy offer factors from `BIDDAYOFFER_D`
  (daily PRICEBAND1-10 prices, rebids deduped by VERSIONNO) plus
  `BIDPEROFFER_D` ENERGY rows (per-interval BANDAVAIL1-10 volumes).
- **Outputs**: monthly avg/p95 offered MW, band-1/band-10 price positioning,
  negative-band day share (willingness to offer below $0), rebids/day,
  top-2-band volume concentration. Published per generator as
  `doc["offers"]` with `scope: offer_based_estimate`.
- **Scope**: offers are *intent*, not dispatch outcomes. Enablement and
  settled revenue remain participant-only.
- **Gotchas**: nemosis takes `raw_data_location` only (a `cache=` kwarg
  crashes its parquet writer). Cached parquets hold only previously-requested
  columns — the volumes fetch rebuilds the month's parquet fat once; the
  FCAS lane (narrow columns) then shares the same fat cache.

### FCAS participation factors (Aug 2026)
Per-DUID offer behaviour from `BIDPEROFFER_D` FCAS rows — see
`src/fcas_factor.py` and the README. Regional FCAS *prices* are labelled
`regional_average` (market context, not generator data).

### Market spread factors (Aug 2026)
Duration-parameterized capture-window VWAPs (1h/2h/4h/8h + decile legacy
proxy benchmarked against AEMO QED) — see README methodology.

## Future — Tier 2 (situational value)

### AEMO Generation Information (quarterly xlsx) — access verified, not built
- **What**: Existing / committed / anticipated / withdrawn generation
  projects, quarterly (Jul 2026 current; series Jan/Apr/Jul/Oct).
- **Verified access**: landing page
  `https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information`;
  the xlsx link works via plain GET **only with the `?rev=<hash>&sc_lang=en`
  query (403 without, hash changes each publication — scrape the page href,
  never hard-code)**. Current example:
  `.../generation_information/2026/nem-generation-information-july-2026.xlsx?rev=3455851f2bc945b7ab61c5ceed272992&sc_lang=en`.
- **Structure** (since Oct-2025 restructure): single "Generator Information"
  sheet, header in row 4, 76 populated columns (Region is a column, not a
  sheet). Key discriminator = Commitment Status: In Service (630),
  Publicly Announced (893), Anticipated (110), Committed (42), Committed*
  (15), In Commissioning (29), Announced Withdrawal (27), Withdrawn (1).
  "Change Log" sheet records material changes per publication.
- **Critical caveat**: DUID is blank on ~62% of rows (pre-connection
  projects) — key on Gen Info Unit ID / Site Name, join DUID only where
  present.
- **Credit-risk value**: forward cannibalisation (committed BESS in a region
  compresses every incumbent's spread) and counterparty event triggers
  (Announced Withdrawal dates). Build: fetch latest xlsx quarterly, diff
  Commitment Status vs stored edition, emit new-commitment/withdrawal events.
- **Companion files** on the same page: Expected Closure Year xlsx (NER
  2.1B.3), KCI Datafile Compiled.

### Network outages (`NETWORK_OUTAGEDETAIL`) — access verified, not built
- **What**: transmission outage lifecycle records (LINE/CB/BUS/TRANS/...
  with submitted/actual start-end, status codes WDRAWN/COMPLETE/UTP/...).
- **Verified access (two routes)**:
  1. MMSDM monthly: `PUBLIC_ARCHIVE%23NETWORK_OUTAGEDETAIL%23FILE01%23{YYYYMM}010000.zip`
     under the standard MMSDM DATA path — **exists only from MMSDM_2026_07**
     (older months 404; no monthly backfill route). Each file is a ~205MB
     *full-history* CSV (890k rows, 2003→present), not a monthly slice.
  2. NEMWEB weekly: `https://www.nemweb.com.au/Reports/ARCHIVE/Network/PUBLIC_NETWORK_{YYYYMMDD}.zip`
     (Friday roll-ups, ~90MB, zips-within-zips per 30-min interval, MMS
     format; Aug 2025→present, publish lag 2–3 weeks).
- **Schema notes**: no REGION/VOLTAGE in OUTAGEDETAIL — join
  `NETWORK_EQUIPMENTDETAIL` (VOLTAGE) / `NETWORK_RATING` (REGIONID) via
  SUBSTATIONID+EQUIPMENTTYPE+EQUIPMENTID. Far-future dates (2099/2202) are
  standing/recurring windows, not errors. `NETWORK_OUTAGECONSTRAINTSET`
  (outage→constraint-set map) is monthly-archive-only.
- **Credit-risk value**: leading indicator for MLF deterioration and
  curtailment; complements the constraint lane. Build: monthly full-history
  refresh, filter to ACTIVE windows overlapping the month, aggregate
  per-region outage-days by voltage class.
- **Practical warning**: the full-history pattern means storage grows
  monotonically — parse and store the filtered slice, keep the raw zip only
  for the current cycle.

### AER market-statistics QA lane — access verified, planned as QA (not charted)
- **What**: AER quarterly market-statistics CSV suite (refreshed ~6–8 weeks
  after quarter end, e.g. re-published 2026-04-07 and 2026-08-07) plus the
  biennial WEMPR (2022/2024/2026) and annual State of the Energy Market
  workbooks. No API; the quarterly CSV suite is the only automation-suitable
  series (WEMPR/SOM are one-off workbook editions).
- **QA value (per Zalen: QA process, not dashboard charts)**: cross-check
  our derived aggregates against the regulator's published picture.
  Checkable series: quarterly volume-weighted average spot prices per
  region; negative-price interval counts; price-threshold interval counts
  (>$300/$5000/$20000); FCAS cost totals. Divergence beyond tolerance =
  data-quality alarm (same pattern as the QED divergence check).
- **Build sketch**: fetch the suite each quarter, extract per-region
  quarterlies, compare against `market_quarterly.json` values, log
  pass/warn/fail in the daily run (warn-band approach like QED).

### ASX electricity futures (base / cap / peak settlement)
- Forward hedge benchmark per region. Historical settlements + open interest
  are a **paid** ASX Energy Data Centre subscription; daily snippets are
  free but without depth. Semi-public — blocked until a data path exists.

### ST PASA / MT PASA
- Short-term (hourly, 7 days) and medium-term (weekly, 36 months)
  availability submissions; `MTPASA_DATA_EXPORT` in the MMSDM archive.
  Supply adequacy / competitor availability ahead of price events.

## Regulatory-context items (no ingestion, track manually)

- **Frequency Performance Payments (FPP)**: replaced FCAS causer-pays on
  8 June 2025. Contribution factors are 5-minute, published on NEMWEB. Any
  causer-pays logic is dead — do not resurrect it. A future FPP
  cost-allocation factor per DUID would slot next to `fcas_participation`.
- **LOR quarterly reports, RERT disclosures, market event reports**:
  event-driven flags only (AEMO NEM events pages).
- **Participant prudential settings (credit limits, margins, VPR)**: **not
  published** at participant level — public credit-risk analysis is bounded
  at what this dashboard already does.

## Revenue-grade FCAS enablement (upgrade path for fcas_participation)

`fcas_participation` measures **offers** (BIDPEROFFER_D). Settled FCAS
revenue requires dispatch *enablement*, published in the
`Next_Day_Offer_Engine` reports on NEMWEB (per-unit FCAS enablement flags in
dispatch resolution). The `/Reports/ARCHIVE/Next_Day_Offer_Engine/` listing
returned 404 at review time; current-day reports are under
`/Reports/Current/`. Keep the retention problem in mind (daily files, not
monthly) — archive what you fetch.
