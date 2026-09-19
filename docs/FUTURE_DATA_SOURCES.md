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
  negative-band day share (willingness to offer below $0),
  top-2-band volume concentration. Published per generator as
  `doc["offers"]` with `scope: offer_based_estimate`. (rebids/day is not
  published: offer frames are deduped to the latest version per day, so a
  post-dedupe count is structurally 1.0 — see `src/offer_curves.py`.)
- **Scope**: offers are *intent*, not dispatch outcomes. Enablement and
  settled revenue remain participant-only.
- **Gotchas**: nemosis takes `raw_data_location` only (a `cache=` kwarg
  crashes its parquet writer). Cached parquets hold only previously-requested
  columns — the volumes fetch rebuilds the month's parquet fat once; the
  FCAS lane (narrow columns) then shares the same fat cache.
- **Panel visibility**: the offers summary and the daily bid stack are
  UNIT-level blocks and live in their own panel (`#panelOffers` in
  `docs/index.html`), never inside the regional FCAS panel. Gating them on
  the FCAS block hid real payloads: `renderCharts()` hides `#panelFCAS` for
  units whose FCAS history is thin (<3 non-null monthly points), which left
  the offers blocks `display:block` behind a `display:none` ancestor — seen
  with the newly-registered batteries PLBESS1 (Pine Lodge BESS) and ERB02
  (Eraring BESS 2). A DUID with no `docs/data/offer_curves/{DUID}.json` has
  no panel by design, not by a CSS condition: retired units such as ADPBA1G
  (Adelaide Desalination Plant genset, MLF tracker status *Retired*, no
  current registration-list row) are not published at all after S3-06, so
  the dashboard never offers them and no offer file is written.

### AEMO network outages (Sep 2026)
- **What**: Transmission outage windows from the MMSDM monthly
  `NETWORK_OUTAGEDETAIL` archive (LINE / CB / TRANS / BUS / CAP / SVC / REAC …)
  with submitted and actual start/end and the AEMO status code (`WDRAWN`,
  `COMPLETE`, `SUBMIT`/`UTP`/`MTLTP`/`PTP` …). Published as
  `docs/data/network_outages.json`: per-region outage-days by voltage class for
  the processed months, plus the active-window and standing-window lists.
- **Metric**: *scheduled* outage-days — for each window overlapping a processed
  month, the days of that overlap clipped to the month (an open or far-future
  end runs to month end). Withdrawn windows (`WDRAWN`, `WD REQ`) are retained in
  the local snapshot for audit but excluded from the metric: a withdrawn request
  never took plant out of service. Outage-days below a voltage class are summed
  per region, so a 500 kV interconnector transformer outage and a 66 kV line
  outage are visible separately rather than blended.
- **Region/voltage**: OUTAGEDETAIL carries neither. Region resolves in
  priority order — rated-equipment exact match in `NETWORK_RATING` →
  substation-majority region from `NETWORK_RATING` → substation-level
  `REGIONID` from `NETWORK_SUBSTATIONDETAIL` (live probe: ~86% of window rows
  join, mostly via the two fallbacks). Voltage comes from
  `NETWORK_EQUIPMENTDETAIL` (latest `VALIDFROM` per key), bucketed into
  500/330/275/220/110–132/33–66/<33 kV classes. Rows that join nowhere are
  **dropped from the rollup and counted** (`summary.unjoined_region_windows`,
  `source_status.join_warning`) — never assigned a guessed region or a
  zero-filled voltage.
- **Storage (parse-and-slice)**: the monthly zip is ~23 MB and extracts to a
  **~205 MB full-history CSV** (2002 → present, ~896k rows), so the raw archive
  is deleted the moment the target months are sliced; only the compact
  window-month snapshot, the per-month slices and the standing-window file
  remain under `data/network_outages/` (machine-local, never committed). A test
  tripwire asserts no `.zip`/`.CSV` survives a run.
- **Gotchas**: AEMO's standing/recurring windows start in 2098–2202 — real
  register rows, kept and flagged, never treated as data errors. Availability
  was live-probed 2026-09-19: the MMSDM monthly route serves well before
  `MMSDM_2026_07` (checked back to 2024_06/2024_09/2025_03…2026_08), so the
  earlier "exists only from 2026_07, no monthly backfill route" note was wrong
  and `--network-outage-backfill N` now slices prior months. Scheduled (not
  observed) days mean a long-dated maintenance window counts for every month it
  spans — read it as planned exposure, which is the leading-indicator value for
  MLF drift and curtailment.

### AEMO Generation Information (quarterly xlsx)
- **What**: Existing / committed / anticipated / withdrawn generation projects
  (quarterly editions), diffed edition-to-edition for new commitments,
  de-commitments and announced withdrawals — published as `docs/data/gen_info.json`
  and attached per generator (`gen_info` block). See `src/geninfo.py`.
- **Gotchas**: the landing-page link needs its per-publication `?rev=<hash>`
  query, so the module scrapes the href and falls back to probing the
  deterministic media URL; DUID is blank on ~62% of rows, so edition identity
  keys on `Gen Info Unit ID`.

### FCAS participation factors (Aug 2026)
Per-DUID offer behaviour from `BIDPEROFFER_D` FCAS rows — see
`src/fcas_factor.py` and the README. Regional FCAS *prices* are labelled
`regional_average` (market context, not generator data).

### Market spread factors (Aug 2026)
Duration-parameterized capture-window VWAPs (1h/2h/4h/8h + decile legacy
proxy benchmarked against AEMO QED) — see README methodology.

### AER market-statistics QA cross-check (quarterly CSV suite, Sep 2026)
- **What**: The AER's quarterly market-statistics CSV suite — regional VWA spot
  prices, counts of 30-minute prices below $0 and above $5,000, and NEM total FCAS
  costs — ingested and cross-checked against our own derived
  `docs/data/market_quarterly.json` with QED-style warn-bands. **QA process only:
  no chart, no panel, no `index.html` change** (owner decision 2026-09-19).
  Outputs `docs/data/aer_qa.json` + an `aer_qa` lane record in
  `docs/data/run_status.json`; see `src/aer_qa.py`.
- **Checks**: AER's published regional VWA must land inside our derived quarterly
  band `[avg_vwap_low, avg_vwap_high]` (±15% of band width); AER's below-$0
  interval count, converted to a share of the quarter's 4,368 trading intervals,
  is ratio-banded 0.5–2.0 against our `neg_price_share` (5-minute denominator —
  wide band, gross-divergence alarm only, and counts under 50 intervals on both
  sides are `below_noise_floor`, not warns). Above-$5,000 counts and FCAS cost
  totals have no counterpart column in our artifact: they are ingested and
  published as reference values marked `not_comparable` with the reason — never
  dropped, never faked into a pass. Warns never fail the run.
- **Gotchas**: the chart pages are behind a bot-management interstitial
  (live-probed 2026-09-19: all four slugs answer HTTP 200 with a ~2.4 KB
  `bm-verify` refresh stub — the slug exists, the content does not), while the
  static `/sites/default/files/<edition>/…CSV` assets serve plain quarterly CSVs
  (HTTP 200, 1.4–2.9 KB). Hence the two-step route (scrape the page href → GET the
  static file) with the last-verified seed URLs in `config.AER_QA_SEED_URLS` as
  fallback, and the published `routes` map (`page_scrape` / `seed_url`) so it is
  always visible which path produced the numbers. Editions land ~6–8 weeks after
  quarter end: `AER_QA_PUBLISH_LAG_WEEKS = 8` separates "not published yet"
  (`awaiting_edition`, never a warn) from a stale seed, and per-series
  `content_sha256` means an unchanged edition is not re-fetched. Refresh the seed
  folder date (e.g. `2026-11`) when a new edition lands.
- **First live run** (AER 2026-08 edition covering through 2026Q2, 2026-09-19):
  27 pass / 1 warn / 2 reference-only. The warn is a real finding, not a lane
  bug: TAS1 2025Q4 negative-price share — ours 3.00% of 5-minute intervals vs the
  AER's 8.04% of 30-minute trading intervals (ratio 0.37). Worth investigating
  whether our negative-price capture under-counts TAS through that quarter.

## Future — Tier 2 (situational value)

### AEMO Generation Information (quarterly xlsx) — BUILT (see "Built" above)
Retained here only for the access research (landing-page `?rev=` href, Oct-2025
sheet restructure, blank-DUID caveat). Implemented in `src/geninfo.py`; the
heading above was left saying "not built" for a few commits after the lane
shipped.
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

### Network outages — BUILT (see "Built → AEMO network outages" above)
The MMSDM monthly route and the region/voltage joins are implemented in
`src/network_outages.py`; this section is retained only as a pointer so the old
access research is not re-done. The two corrections that mattered: the monthly
route is available well before `MMSDM_2026_07`, and `NETWORK_SUBSTATIONDETAIL`
is needed on top of `NETWORK_RATING` to join the majority of substations.

### AER market-statistics QA lane — BUILT (see "Built → AER market-statistics QA cross-check")
The quarterly CSV cross-check is implemented in `src/aer_qa.py`; this section is
retained as a pointer. Two things the build settled that the research could not:
the "Download CSV" pages are bot-walled but the static edition files serve fine
(so the lane uses a scrape-then-seed route and records which one it used), and the
suite's coverage is ~6–8 weeks after quarter end, which is what makes
`awaiting_edition` (a lag) distinguishable from a divergence (a finding).
- **What**: AER quarterly market-statistics CSV suite (refreshed ~6–8 weeks
  after quarter end, e.g. re-published 2026-04-07 and 2026-08-07) plus the
  biennial WEMPR (2022/2024/2026) and annual State of the Energy Market
  workbooks. No API; the quarterly CSV suite is the only automation-suitable
  series (WEMPR/SOM are one-off workbook editions).
- **QA value (per Zalen: QA process, not dashboard charts)**: cross-check
  our derived aggregates against the regulator's published picture.
  Divergence beyond tolerance = data-quality alarm (same pattern as the QED
  divergence check).

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
