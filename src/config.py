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
    """Return the start calendar year of the current financial year."""
    now = datetime.now()
    return now.year if now.month >= 7 else now.year - 1


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

# ─── Network ────────────────────────────────────────────────────────────────

MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds
REQUEST_TIMEOUT = 60
USER_AGENT = "Mozilla/5.0 AEMO-Generator-Credit-Dashboard"
