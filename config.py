import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Útvonalak ---
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
FIGURES_DIR = BASE_DIR / "figures"
REPORTS_DIR = BASE_DIR / "reports"

# --- URL-ek ---
URLS = {
    "syrb": "https://www.esrb.europa.eu/national_policy/shared/pdf/esrb.measures_overview_macroprudential_measures.xlsx",
    "ccyb": "https://www.esrb.europa.eu/national_policy/ccb/shared/data/esrb.ccybd_CCyB_data.xlsx",
    "capital_measures": "https://www.esrb.europa.eu/national_policy/shared/pdf/esrb.measures_overview_capital-based_measures.xlsx"
}

# --- Fájlok ---
FILES = {
    "syrb_source": DATA_DIR / "esrb.measures_overview_macroprudential_measures.xlsx",
    "measures_overview_source": DATA_DIR / "esrb.measures_overview_macroprudential_measures.xlsx",
    "ccyb_source": DATA_DIR / "esrb.ccybd_CCyB_data.xlsx",
    "capital_measures_source": DATA_DIR / "esrb.measures_overview_capital-based_measures.xlsx",
    "syrb_processed": DATA_DIR / "processed_syrb.parquet",
    "latest_syrb": DATA_DIR / "latest_syrb.parquet",
    "ccyb_processed": DATA_DIR / "processed_ccyb.parquet",
    "latest_ccyb": DATA_DIR / "latest_ccyb.parquet",
    "bbm_processed": DATA_DIR / "processed_bbm.parquet",
    "latest_bbm": DATA_DIR / "latest_bbm.parquet",
    "osii_processed": DATA_DIR / "processed_osii.parquet",
    "latest_osii": DATA_DIR / "latest_osii.parquet",
    "trend_ccyb": DATA_DIR / "trend_ccyb.parquet",
    "trend_syrb": DATA_DIR / "trend_syrb.parquet",
    "trend_bbm": DATA_DIR / "trend_bbm.parquet",
}

# --- LLM ---
LLM_CONFIG = {
    "model_name": "gemini-2.5-flash-lite",
    "max_output_tokens": 2000,
    "api_key_env": "GOOGLE_API_KEY",
}

# --- Google Search (Grounded Validation) ---
SEARCH_CONFIG = {
    "enabled": True,
    "search_enabled_env": "SEARCH_ENABLED",
    "api_key_env": "CUSTOM_SEARCH_API_KEY",
    "cse_id_env": "GOOGLE_CSE_ID",
    "allowed_domains_env": "SEARCH_ALLOWED_DOMAINS",
    "max_results": 5,
    "report_path": str(DATA_DIR / "validation_report.json"),
    "allowed_domains": [
        "ecb.europa.eu",
        "esrb.europa.eu",
        "bankofgreece.gr",
        "banque-france.fr",
        "bde.es",
        "bundesbank.de",
        "bankofitaly.it",
        "oesterreichische-nationalbank.at",
        "nbp.pl",
        "nbs.sk",
        "mnb.hu",
        "centralbank.ie",
        "bank.lv",
        "bankofestonia.ee",
        "bankoflithuania.lt",
        "bsi.si",
        "bportugal.pt",
        "bnr.ro",
        "cnb.cz",
        "dnb.nl",
        "fi.se",
        "fma.gv.at",
        "finanssivalvonta.fi",
        "fma.li",
        "norges-bank.no",
        "riksbank.se",
        "snb.ch",
    ],
}

# --- News / External Updates ---
NEWS_CONFIG = {
    "enabled": True,
    "api_key_env": "CUSTOM_SEARCH_API_KEY",
    "cse_id_env": "GOOGLE_CSE_ID",
    "months_back": 12,
    "max_results": 10,
    "query": (
        "macroprudential OR macroprudential policy OR macroprudential report OR "
        "countercyclical capital buffer OR countercyclical buffer OR systemic risk buffer OR "
        "borrower-based measures OR reciprocation OR reciprocity OR "
        "CCyB OR SyRB OR LTV OR DSTI OR LTI OR DTI OR O-SII OR OSII OR "
        "(site:ecb.europa.eu OR site:esrb.europa.eu OR site:mnb.hu OR "
        "site:bankofgreece.gr OR site:bundesbank.de OR site:bankofitaly.it)"
    ),
}

# --- BBM / DTI Expert Table ---
# Gold CSVs are the dashboard source of truth (gold + delta). Excel is optional import.
# Set BBM_EXCEL_PATH in .env to override. If unset, uses data/BBM táblázatok.xlsx.
_BBM_EXCEL_ENV = os.getenv("BBM_EXCEL_PATH", "").strip()
BBM_EXCEL_PATH = Path(_BBM_EXCEL_ENV) if _BBM_EXCEL_ENV else (DATA_DIR / "BBM táblázatok.xlsx")

FILES["dti_gold"] = DATA_DIR / "dti_expert_table.csv"
FILES["ltv_gold"] = DATA_DIR / "ltv_gold.csv"
FILES["bbm_gold_state"] = DATA_DIR / "bbm_gold_state.json"
FILES["bbm_delta_report"] = DATA_DIR / "bbm_delta_report.json"
FILES["pipeline_manifest"] = DATA_DIR / "pipeline_manifest.json"

# When true, changed ESRB descriptions are sent to the LLM for match/conflict.
# Hash comparison always runs. Set BBM_GOLD_DELTA_AI=false to only flag hashes.
BBM_GOLD_DELTA_AI = os.getenv("BBM_GOLD_DELTA_AI", "true").strip().lower() in (
    "1", "true", "yes", "on",
)

# Skip bronze/silver/LLM when hashes match. FORCE_REBUILD=true disables all skips.
FORCE_REBUILD = os.getenv("FORCE_REBUILD", "").strip().lower() in ("1", "true", "yes", "on")
NEWS_TTL_DAYS = int(os.getenv("NEWS_TTL_DAYS", "7"))

# --- Supabase Configuration (for Render Stage) ---
SUPABASE_RENDER_CONFIG = {
    "enabled": os.getenv("USE_SUPABASE_FOR_RENDER", "false").lower() == "true",
    "url": os.getenv("SUPABASE_URL", ""),
    "anon_key": os.getenv("SUPABASE_KEY", ""),  # Anon key for frontend
}