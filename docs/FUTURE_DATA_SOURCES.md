# Future Data Sources — Documented Availability

Sources identified in the Sep-2026 public-data review that are **not built**
into the pipeline. Each entry records what it would add to generator credit
risk analysis and the access path, so future work can pick them up without
re-doing the research.

## Tier 1 candidates (highest value, not yet ingested)

### BIDDAYOFFER / BIDPEROFFER energy offer curves
- **What**: Unit-level 10-band energy offer prices + volumes (BIDDAYOFFER is
  the parent table; BIDPEROFFER_D the dispatch-frequency child).
- **Credit-risk value**: Conduct risk — rebidding frequency/direction,
  withholding detection (capacity offered in high bands while physically
  available), availability honesty. The only public source showing market
  *intent*, not outcomes. Enforcement/penalty exposure is a contingent
  liability on the participant.
- **Access**: `BIDPEROFFER_D` already proven fetchable via nemosis (same MMSDM
  monthly archive, table exists with BIDTYPE/MAXAVAIL/ENABLEMENTMIN/MAX —
  see `src/download_bids.py` for the FCAS-row pattern). Energy bids = rows
  where `BIDTYPE == 'ENERGY'` with BANDAVAIL1-10 + price bands from BIDDAYOFFER.
- **Effort**: Medium — same download pattern as FCAS factors; the analytics
  (rebid detection needs offer-version tracking) are the real work.

## Tier 2 (situational value)

### AEMO Generation Information (quarterly xlsx)
- **What**: Existing / committed / proposed / **withdrawn** capacity, with
  anticipated commissioning and retirement dates, per project.
- **Credit-risk value**: Forward cannibalisation (committed BESS in a region
  compresses every incumbent's spread) and counterparty event triggers
  (published withdrawal dates). ~Quarterly xlsx from AEMO's Generation
  Information page. A quarterly diff (new commitments, withdrawn projects)
  would be cheap and high-signal.

### AER Wholesale Performance / rebidding reports
- **What**: Independent quarterly conduct surveillance, negative-price
  records, price-setting mix, FCAS cost totals; downloadable chart data.
- **Credit-risk value**: Regulatory overlay (conduct findings = contingent
  liabilities) plus an independent benchmark to cross-check derived factors —
  divergence between our aggregates and AER's published picture is itself a
  data-quality alarm.

### ASX electricity futures (base / cap / peak settlement)
- **What**: Forward-looking hedge benchmark per region (NSW/Qld/SA/Vic;
  monthly, quarterly, $300 caps, morning/evening peak).
- **Credit-risk value**: Forward margin pressure and hedge-income context.
- **Caveat**: Historical settlement prices + open interest are a **paid**
  subscription (ASX Energy Data Centre). Daily settlement snippets are
  published free but without history depth. Semi-public.

### Network Outage Schedule (`NETWORK_OUTAGEDETAIL` via NEMWEB)
- **What**: Planned transmission outages, ~2 years ahead, half-hourly refresh.
- **Credit-risk value**: Leading indicator for MLF deterioration and
  curtailment — complements the constraint lane that is currently skipped
  (`--skip-constraints`) and flagged as needing its own audited run.

### ST PASA / MT PASA
- **What**: Short-term (hourly, next 7 days) and medium-term (weekly, 36
  months) availability submissions per scheduled unit; `MTPASA_DATA_EXPORT`
  exists in the MMSDM archive.
- **Credit-risk value**: Supply adequacy and competitor availability ahead of
  price events; LOR/RERT-proximity context.

## Regulatory-context items (no ingestion, track manually)

- **Frequency Performance Payments (FPP)**: replaced FCAS causer-pays on
  8 June 2025. Contribution factors are now 5-minute and published on NEMWEB
  (next-day public data + payment/recovery rates). Any causer-pays logic is
  dead — do not resurrect it. A future FPP cost-allocation factor per DUID
  would slot next to `fcas_participation`.
- **LOR framework quarterly reports, RERT disclosures, market event reports**:
  event-driven flags only (AEMO publishes on its NEM events pages).
- **Participant prudential settings (credit limits, margins, VPR)**: **not
  published** at participant level — AEMO publishes only CLP methodology and
  an annual aggregate effectiveness report. Participant-level credit risk from
  public data is bounded at what this dashboard already does.

## Revenue-grade FCAS enablement (upgrade path for fcas_participation)

`fcas_participation` measures **offers** (BIDPEROFFER_D). Settled FCAS
revenue requires dispatch *enablement*, published in the
`Next_Day_Offer_Engine` reports on NEMWEB (per-unit FCAS enablement flags in
dispatch resolution). The `/Reports/ARCHIVE/Next_Day_Offer_Engine/` listing
returned 404 at review time; current-day reports are under
`/Reports/Current/`. If revenue-grade FCAS factors are ever needed, start
there and keep the retention problem in mind (daily files, not monthly).
