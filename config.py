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
    "max_output_tokens": 2000
}