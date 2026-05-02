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
}

# ─── Network ────────────────────────────────────────────────────────────────

MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds
REQUEST_TIMEOUT = 60
USER_AGENT = "Mozilla/5.0 AEMO-Generator-Credit-Dashboard"
