"""Configuration constants for the WEM (Western Australia) pipeline.

WEM differences from NEM:
- Single zone (SWIS — South West Interconnected System), no regional split
- Pre-Reform (to Sep 2023): 30-minute dispatch intervals, 48 per day
- Post-Reform (Oct 2023+): 5-minute intervals — public historical archive unavailable
- Loss factor: TLF (Transmission Loss Factor), not MLF
- Balancing Price replaces Regional Reference Price
- No public curtailment (UIGF) data — curtailment metrics omitted
"""

# ─── Region ──────────────────────────────────────────────────────────────────

WEM_REGION = "WEM"

# ─── Interval structure (pre-Reform) ─────────────────────────────────────────

INTERVALS_PER_DAY = 48          # 30-minute intervals
INTERVAL_DURATION_H = 0.5       # hours per interval

# ─── Data coverage ────────────────────────────────────────────────────────────

# Both SCADA and balancing price available from this point forward
WEM_DATA_START = (2012, 7)      # (year, month) — FY2012-13 start
# Last full pre-Reform month (Reform took effect Oct 2023)
WEM_PRE_REFORM_END = (2023, 9)  # (year, month) — CSV archive cutoff

# Post-Reform starts Oct 2023 — sourced from Open Electricity API (not AEMO CSVs)
WEM_POST_REFORM_START = (2023, 10)  # (year, month)

# ─── TLF ─────────────────────────────────────────────────────────────────────

# HTTP Referer required to download TLF files from AEMO website
TLF_REFERER = (
    "https://www.aemo.com.au/energy-systems/electricity/"
    "wholesale-electricity-market-wem/data-wem/loss-factors"
)

# Annual TLF CSV/XLSX URLs keyed by "YYYY-YY" FY label.
# Note: 2013-14 is not published — pipeline falls back to nearest available FY.
TLF_URLS: dict[str, str] = {
    "2025-26": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2026/transmission-loss-factors-2025-26.csv"
    ),
    "2024-25": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2025/transmission-loss-factors-2024-25.xlsx"
    ),
    "2023-24": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2024/transmission-loss-factors-2023-24-october-1st.csv"
    ),
    "2022-23": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2022/transmission-loss-factors-2022-23.csv"
    ),
    "2021-22": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2021/transmission-loss-factors-2021-22.csv"
    ),
    "2020-21": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2020/transmission-loss-factors-2020-21.csv"
    ),
    "2019-20": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2019/transmission-loss-factors-2019_20.csv"
    ),
    "2018-19": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2018/transmission-loss-factors-2018_19.csv"
    ),
    "2017-18": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2017/transmission-loss-factors-2017_18.csv"
    ),
    "2016-17": (
        "https://www.aemo.com.au/-/media/files/electricity/wem/data/loss-factors"
        "/2016/transmission-loss-factors-2016-17.csv"
    ),
    "2015-16": (
        "https://www.aemo.com.au/-/media/archive/docs/default-source/market-data"
        "/loss-factors/transmission-loss-factors-2015-16f6a0.csv"
    ),
    "2014-15": (
        "https://www.aemo.com.au/-/media/archive/docs/default-source/market-data"
        "/loss-factors/transmission-loss-factors-2014-15f6a0.csv"
    ),
    "2012-13": (
        "https://www.aemo.com.au/-/media/archive/docs/default-source/market-data"
        "/loss-factors/transmission-loss-factors-2012-135eee.csv"
    ),
}

# ─── Data portal URLs ────────────────────────────────────────────────────────

# Pre-Reform archive (HTTP, not HTTPS)
OLD_PORTAL_BASE = "http://data.wa.aemo.com.au/datafiles"
SCADA_URL_TEMPLATE = OLD_PORTAL_BASE + "/facility-scada/facility-scada-{year:04d}-{month:02d}.csv"
BALANCING_URL_TEMPLATE = OLD_PORTAL_BASE + "/balancing-summary/balancing-summary-{year:04d}.csv"

# Facility metadata
FACILITIES_URL = (
    "https://data.wa.aemo.com.au/public/public-data/datafiles/facilities/facilities.csv"
)
POST_FACILITIES_URL = (
    "https://data.wa.aemo.com.au/public/public-data/datafiles/post-facilities/facilities.csv"
)

# FCESS uplift payments — per-facility, post-Reform only (Oct 2023+)
FCESS_UPLIFT_URL_TEMPLATE = (
    "https://data.wa.aemo.com.au/public/public-data/datafiles"
    "/fcess-uplift-payments/fcess-uplift-payments-{year:04d}.csv"
)
FCESS_YEARS = [2023, 2024, 2025, 2026]

# FCESS service column suffixes (from uplift CSV column names)
FCESS_SERVICES = ["CR", "CL", "RCS", "RR", "RL"]
FCESS_SERVICE_LABELS = {
    "CR": "Contingency Reserve",
    "CL": "Contingency Lower",
    "RCS": "Regulation Capacity Service",
    "RR": "RoCoF Response",
    "RL": "Regulation Lower",
}

# ─── Paths (relative to project root) ────────────────────────────────────────

WEM_DATA_DIR = "data/wem"
WEM_SCADA_CACHE_DIR = "data/wem/scada"
WEM_PRICE_CACHE_DIR = "data/wem/prices"
WEM_METADATA_CACHE = "data/wem/facilities.feather"
WEM_TLF_CACHE = "data/wem/tlf_history.feather"
WEM_MONTHLY_AGGREGATES_CACHE = "data/wem/monthly_aggregates.feather"
WEM_FCESS_CACHE = "data/wem/fcess_uplifts.feather"

# ─── Fuel type classification (keyword-based from facility name) ──────────────

# Checked in order; first match wins
FUEL_KEYWORDS: list[tuple[str, list[str]]] = [
    ("Solar",           ["solar", "pv", "photovoltaic"]),
    ("Wind",            ["wind"]),
    ("Hydro",           ["hydro"]),
    ("Battery",         ["battery", "bess", " storage"]),
    ("Other Renewable", ["biomass", "waste", "landfill", "biogas", "bagasse"]),
    ("Fossil",          ["gas", "coal", "diesel", "oil", "distillate"]),
]
# Fallback for unclassified
FUEL_FALLBACK = "Other"

# ─── Price distribution bins (matches NEM for consistent frontend) ────────────

PRICE_BINS = (
    [float("-inf")]
    + list(range(-100, 110, 10))
    + [float("inf")]
)
PRICE_BIN_LABELS = (
    ["< -100"]
    + [f"{lo} to {lo + 10}" for lo in range(-100, 100, 10)]
    + ["> 100"]
)

# ─── Network ─────────────────────────────────────────────────────────────────

MAX_RETRIES = 3
RETRY_BACKOFF = 5
REQUEST_TIMEOUT = 120
USER_AGENT = "Mozilla/5.0 AEMO-Generator-Credit-Dashboard"
