from pathlib import Path

# --- Útvonalak ---
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
FIGURES_DIR = BASE_DIR / "figures"
REPORTS_DIR = BASE_DIR / "reports"

# --- URL-ek ---
URLS = {
    "syrb": "https://www.esrb.europa.eu/national_policy/shared/pdf/esrb.measures_overview_macroprudential_measures.xlsx",
    "ccyb": "https://www.esrb.europa.eu/national_policy/ccb/shared/data/esrb.ccybd_CCyB_data.xlsx"
}

# --- Fájlok ---
FILES = {
    "syrb_source": DATA_DIR / "esrb.measures_overview_macroprudential_measures.xlsx",
    "ccyb_source": DATA_DIR / "esrb.ccybd_CCyB_data.xlsx",
    "syrb_processed": DATA_DIR / "processed_syrb.parquet",
    "latest_syrb": DATA_DIR / "latest_syrb.parquet",
    "ccyb_processed": DATA_DIR / "processed_ccyb.parquet",
    "latest_ccyb": DATA_DIR / "latest_ccyb.parquet",
    "bbm_processed": DATA_DIR / "processed_bbm.parquet",
    "latest_bbm": DATA_DIR / "latest_bbm.parquet"
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