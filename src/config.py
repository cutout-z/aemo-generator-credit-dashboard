"""Configuration for AEMO Generator Credit Dashboard."""

from datetime import datetime

# ─── Regions ────────────────────────────────────────────────────────────────

REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

REGION_NAMES = {
    "NSW1": "NSW",
    "QLD1": "QLD",
    "VIC1": "VIC",
    "SA1": "SA",
    "TAS1": "TAS",
}

STATE_TO_REGION = {v: k for k, v in REGION_NAMES.items()}

# ─── Financial Year Logic ───────────────────────────────────────────────────

FY_START = 2015  # Earliest FY for MLF history (FY15-16)


def current_fy_start() -> int:
    """Return the start year of the latest FY with published final MLFs.

    AEMO publishes final MLFs each April for the upcoming FY (e.g. April 2026
    covers FY26-27). From April onwards the upcoming FY's finals are available
    in DUDETAILSUMMARY; prior months fall back to the previous FY.
    """
    now = datetime.now()
    return now.year if now.month >= 4 else now.year - 1


def fy_label(start_year: int) -> str:
    """E.g. 2024 → 'FY24-25'."""
    return f"FY{start_year % 100:02d}-{(start_year + 1) % 100:02d}"


# ─── Fuel Type Categories ──────────────────────────────────────────────────

FUEL_TYPE_MAP = {
    "Solar": "Solar",
    "Wind": "Wind",
    "Hydro": "Hydro",
    "Battery Storage": "Battery",
    "Fossil": "Fossil",
    "Renewable/ Biomass / Waste": "Other Renewable",
    "Renewable/ Biomass / Waste and Fossil": "Other Renewable",
    "-": "Other",
}

# Fuel types where curtailment analysis is meaningful
CURTAILMENT_FUEL_TYPES = {"Solar", "Wind"}

# S3-01: curtailment is a forecast-to-output shortfall proxy (1 − SCADA/
# AVAILABILITY). Version marks the schema/methodology generation so
# downstream consumers can tell rows produced by the retired causal
# split (grid/mechanical) from proxy-only rows.
CURTAILMENT_METRIC_VERSION = "2.0-proxy"

# INTERMITTENT_GEN_SCADA availability start (year, month)
INTERMITTENT_SCADA_START = (2024, 8)

# Binding constraints: how many months of history to fetch
CONSTRAINTS_HISTORY_MONTHS = 24

# Fuel types eligible for LGC creation (1 LGC ≈ 1 MWh)
LGC_ELIGIBLE_FUEL_TYPES = {"Solar", "Wind", "Hydro", "Other Renewable"}

# ─── Data Sources ───────────────────────────────────────────────────────────

# NEM Registration and Exemption List
REGISTRATION_URL = (
    "https://www.aemo.com.au/-/media/Files/Electricity/NEM/"
    "Participant_Information/NEM-Registration-and-Exemption-List.xls"
)
REGISTRATION_SHEET = "PU and Scheduled Loads"

# MMSDM archive for DUDETAILSUMMARY (MLFs)
MMSDM_BASE_URL = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
DUDETAILSUMMARY_URL_TEMPLATE = (
    MMSDM_BASE_URL
    + "{year:04d}/MMSDM_{year:04d}_{month:02d}/"
    "MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_ARCHIVE%23DUDETAILSUMMARY%23FILE01%23{year:04d}{month:02d}010000.zip"
)

# NEMWEB base for probing available months
NEMWEB_BASE_URL = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"

# MMSDM monthly archive for the transmission outage lifecycle
# (NETWORK_OUTAGEDETAIL). The monthly zip is ~23 MB and holds a ~205 MB
# *full-history* CSV (2002 → present, ~896k rows), so the lane parses-and-slices:
# the raw zip and its extracted CSV are deleted as soon as the target months'
# windows are sliced out (see src/network_outages.py). Live-probed 2026-09-19:
# the monthly route serves 2024_06 → 2026_08, i.e. well before MMSDM_2026_07
# (the "exists only from 2026_07" note in the spec was wrong).
NETWORK_OUTAGE_URL_TEMPLATE = (
    MMSDM_BASE_URL
    + "{year:04d}/MMSDM_{year:04d}_{month:02d}/"
    "MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_ARCHIVE%23NETWORK_OUTAGEDETAIL%23FILE01%23{year:04d}{month:02d}010000.zip"
)

# Supporting monthly tables carrying the region/voltage context that
# OUTAGEDETAIL itself lacks. NETWORK_RATING gives REGIONID per
# SUBSTATIONID+EQUIPMENTTYPE+EQUIPMENTID, NETWORK_EQUIPMENTDETAIL gives VOLTAGE
# for the same key, and NETWORK_SUBSTATIONDETAIL is the substation-level
# REGIONID fallback the live probe showed is needed (the rating table only
# covers ~358 of the ~1023 substations that carry outages).
NETWORK_SUPPORT_URL_TEMPLATE = (
    MMSDM_BASE_URL
    + "{year:04d}/MMSDM_{year:04d}_{month:02d}/"
    "MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_ARCHIVE%23{table}%23FILE01%23{year:04d}{month:02d}010000.zip"
)

# How many months back the outage discovery probes for the newest published file
# (AEMO publishes the MMSDM monthly archive ~2 weeks after month-end).
NETWORK_OUTAGE_PROBE_MONTHS_BACK = 4

# Outage windows that start at/after this year are AEMO's standing/recurring
# windows (observed 2098, 2099, 2100, 2202), never data errors: they are kept
# and flagged rather than dropped.
NETWORK_OUTAGE_STANDING_YEAR = 2090

# ─── AER market-statistics QA lane (Tier-2 lane 3) ──────────────────────────
# The AER re-publishes a quarterly CSV suite ~6-8 weeks after quarter end. The
# lane cross-checks our own derived aggregates against the regulator's published
# picture and records pass/warn per series. This is a QA PROCESS ONLY — no
# dashboard panel, chart or index.html change consumes it (owner decision
# 2026-09-19).
#
# Live-probed 2026-09-19: the chart pages below all answer HTTP 200 with a ~2.4 KB
# bot-management interstitial ("bm-verify" refresh stub) from this network, while
# the static /sites/default/files/... CSVs serve fine — hence the two-step route
# (scrape the page's "Download CSV" href, then GET the static file) plus the
# last-verified seed URLs, and why a walled page is a recorded route note rather
# than a lane failure.
AER_BASE_URL = "https://www.aer.gov.au"

AER_QA_SERIES = {
    "vwap_region_quarter": {
        "page": "/industry/registers/charts/"
                "quarterly-volume-weighted-average-spot-prices-regions",
        "csv_match": ("vwa spot prices", "data"),
        "scope": "regional",
        "unit": "AUD/MWh",
        "what": "quarterly demand-weighted average spot price per region",
    },
    "neg_price_count": {
        "page": "/industry/registers/charts/"
                "quarterly-count-30-minute-prices-below-0mwh",
        "csv_match": ("below", "$0", "data"),
        "scope": "regional_count",
        "unit": "30-minute trading intervals",
        "what": "count of 30-minute settlement periods below $0/MWh per region",
    },
    "high_price_count_5000": {
        "page": "/industry/registers/charts/"
                "quarterly-count-30-minute-prices-above-5000mwh",
        "csv_match": ("above", "$5000", "data"),
        "scope": "regional_count",
        "unit": "30-minute trading intervals",
        "what": "count of 30-minute settlement periods above $5,000/MWh per region",
    },
    "fcas_total_cost": {
        "page": "/industry/registers/charts/"
                "quarterly-fcas-total-costs-global-and-local",
        "csv_match": ("fcas costs",),
        "scope": "nem_total",
        "unit": "$m",
        "what": "NEM-wide FCAS cost per quarter (recorded for reference)",
    },
}

# Last-verified static CSV URLs (AER 2026-08 edition, live-fetched 2026-09-19 —
# each returned HTTP 200 / application/octet-stream / 1.4-2.9 KB with a plain
# quarterly CSV body). Used ONLY when the CMS page is bot-walled, and always
# edition-stamped: the lane publishes the latest quarter the edition actually
# covers and reports "awaiting_edition" rather than comparing against old data.
# Refresh the folder date (e.g. 2026-11) when a new edition lands.
AER_QA_SEED_URLS = {
    "vwap_region_quarter": (
        "https://www.aer.gov.au/sites/default/files/2026-08/"
        "AER_Spot%20prices_Quarterly%20VWA%20spot%20prices%20DATA_2_20260807084204.CSV"
    ),
    "neg_price_count": (
        "https://www.aer.gov.au/sites/default/files/2026-08/"
        "AER_Spot%20prices_Quarterly%20count%20of%20spot%20prices%20below%20%240"
        "%20DATA_2_20260807084210.CSV"
    ),
    "high_price_count_5000": (
        "https://www.aer.gov.au/sites/default/files/2026-08/"
        "AER_Spot%20prices_Quarterly%20count%20of%20spot%20prices%20above%20%245000"
        "%20DATA_2_20260807084208.CSV"
    ),
    "fcas_total_cost": (
        "https://www.aer.gov.au/sites/default/files/2026-08/Total%20FCAS%20Costs.csv"
    ),
}

# Warn-bands (QED-style: a divergence is an investigate flag, never a run
# failure). comparator=None means "ingest and publish as reference, we have no
# counterpart column" — the reason is published, never a silent pass.
AER_QA_BANDS = {
    "vwap_region_quarter": {
        "comparator": "band_contains",
        # Proportional slack on our derived [avg_vwap_low, avg_vwap_high] band.
        "tolerance_ratio": 0.15,
    },
    "neg_price_count": {
        "comparator": "share_ratio",
        # Different denominators by construction (AER: 30-minute trading
        # intervals; ours: 5-minute dispatch intervals) — wide band, gross
        # divergence only.
        "ratio_min": 0.5,
        "ratio_max": 2.0,
        # A quarter with a handful of negative intervals cannot support a ratio.
        "noise_floor": 50,
    },
    "high_price_count_5000": {
        "comparator": None,
        "reason": "market_quarterly.json carries no >$5,000 interval count",
    },
    "fcas_total_cost": {
        "comparator": None,
        "reason": "our quarterly artifact carries no NEM-wide FCAS cost total",
    },
}

# Suite publication lag: quarter end + this many weeks is when the AER edition
# covering it is expected. Used to tell "not published yet" (awaiting_edition,
# never a warn) apart from a stale seed URL.
AER_QA_PUBLISH_LAG_WEEKS = 8
# Trading intervals (30-minute settlement periods) per day — the denominator when
# converting an AER interval count into a share of the quarter.
AER_QA_TRADING_INTERVALS_PER_DAY = 48

# AEMO Generation Information (quarterly project register xlsx). The landing
# page hrefs carry a per-publication ?rev=<hash> query; the module scrapes them
# and falls back to probing the deterministic media URL (see src/geninfo.py).
GENINFO_LANDING_URL = (
    "https://www.aemo.com.au/energy-systems/electricity/"
    "national-electricity-market-nem/nem-forecasting-and-planning/"
    "forecasting-and-planning-data/generation-information"
)

# ─── Paths (relative to project root) ──────────────────────────────────────

DATA_DIR = "data"
NEMOSIS_CACHE_DIR = "data/nemosis_cache"
DOCS_DATA_DIR = "docs/data"
GENERATORS_JSON_DIR = "docs/data/generators"
INDEX_JSON = "docs/data/index.json"

# Cache files
GENERATOR_CACHE = "data/generators.feather"
MLF_CACHE = "data/mlf_history.feather"
MONTHLY_AGGREGATES_CACHE = "data/monthly_aggregates.feather"

# ─── Pipeline Settings ─────────────────────────────────────────────────────

# How many years of SCADA/price history to process
HISTORY_YEARS = 5

# Default months to reprocess on incremental run (overlap for late data)
DEFAULT_MONTHS_BACK = 2

# Price distribution bins (AUD/MWh) — $10 increments from -100 to +100
PRICE_BINS = (
    [float("-inf")]
    + list(range(-100, 110, 10))  # -100, -90, ..., 90, 100
    + [float("inf")]
)
PRICE_BIN_LABELS = (
    ["< -100"]
    + [f"{lo} to {lo+10}" for lo in range(-100, 100, 10)]
    + ["> 100"]
)

# ─── Known Registration Corrections ────────────────────────────────────────
# AEMO's Registration List sometimes has stale or unit-level (not station-level)
# capacity figures. These overrides correct known errors identified via sustained
# CF > 1.0 in SCADA data.
#
# HUMENSW: registered at 29 MW (one unit) but both units dispatch under this DUID;
#          station total is 2 × 29 MW = 58 MW. HUMEV (VIC side) is a separate DUID.
CAPACITY_OVERRIDES: dict[str, float] = {
    "HUMENSW": 58.0,
    # Loy Yang B units uprated from 500 MW; AEMO constraint #LOYYB1_E1 caps at 580 MW,
    # peak SCADA ~585 MW. Registration List still shows 500 MW.
    "LOYYB1": 580.0,
    "LOYYB2": 580.0,
    # BARRON-1/-2: registered 30 MW each, but sustained mean CF > 1.0 across
    # FY25-26 with daily peaks ~1.10x registered (Apr-2026 audit; same class
    # as the KAREEYA/BARRON reference in the audit skill). Implied ~33.2 MW.
    "BARRON-1": 33.2,
    "BARRON-2": 33.2,
}

# ─── Network ────────────────────────────────────────────────────────────────

MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds
REQUEST_TIMEOUT = 60
USER_AGENT = "Mozilla/5.0 AEMO-Generator-Credit-Dashboard"
