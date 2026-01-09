import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import country_converter as coco
import numpy as np

# --- Konfiguráció ---
DATA_DIR = Path("data")
ESRB_URL = "https://www.esrb.europa.eu/national_policy/ccb/shared/data/esrb.ccybd_CCyB_data.xlsx"
FILES = {
    "raw": DATA_DIR / "esrb.ccybd_CCyB_data.xlsx",
    "processed": DATA_DIR / "processed_data.parquet",
    "agg_trend": DATA_DIR / "agg_trend.parquet",
    "latest": DATA_DIR / "latest_country.parquet"
}

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

# --- Letöltés ---
def download_data():
    if FILES["raw"].exists():
        print("Data file already exists. Skipping download.")
        return

    print(f"Downloading from {ESRB_URL}...")
    try:
        response = requests.get(ESRB_URL, stream=True)
        response.raise_for_status()
        FILES["raw"].write_bytes(response.content)
        print("Download successful!")
    except Exception as e:
        raise SystemExit(f"Download failed: {e}")

# --- Feldolgozás ---
def summarize_reasoning(text):
    if pd.isna(text) or str(text).strip().upper() == "N/A":
        return "N/A"

    text = str(text).lower()
    mapping = {
        "Credit Growth": ["credit", "lending", "loan"],
        "Real Estate": ["house", "property", "real estate", "mortgage"],
        "Indebtedness": ["debt", "leverage", "indebted"],
        "Systemic Resilience": ["resilience", "buffer", "shock", "loss"],
        "Positive Neutral": ["neutral", "standard", "cycle", "baseline"],
        "Macro Trends": ["gdp", "growth", "economy"]
    }
    
    found = [key for key, keywords in mapping.items() if any(k in text for k in keywords)]
    return " | ".join(sorted(found)) if found else "General Macro-Financial Monitoring"

def process_data():
    print("Processing ESRB data...")
    
    # Excel fejléc keresése
    xl = pd.ExcelFile(FILES["raw"])
    # Feltételezzük, hogy az adatlap neve 'Data' vagy az első/második lap
    sheet_name = "Data" if "Data" in xl.sheet_names else xl.sheet_names[0]
    
    preview = xl.parse(sheet_name, header=None, nrows=15)
    # Megkeressük azt a sort, ahol a "Country" szó szerepel
    header_idx = preview[preview.apply(lambda row: row.astype(str).str.contains("Country", case=False).any(), axis=1)].index[0]
    
    df = xl.parse(sheet_name, skiprows=header_idx)
    
    # Oszlopnevek tisztítása
    df.columns = df.columns.str.lower().str.replace(' ', '_', regex=False)
    
    # Oszlopok normalizálása (Mapping)
    col_map = {}
    for col in df.columns:
        if 'ccyb_rate' in col: col_map[col] = 'rate'
        elif 'credit_gap' in col: col_map[col] = 'credit_gap'
        elif 'credit-to-gdp' in col: col_map[col] = 'credit_to_gdp'
    df = df.rename(columns=col_map)

    # Dátum konverzió
    if 'application_since' in df.columns:
        df['date'] = pd.to_datetime(df['application_since'], errors='coerce')
    
    # Számok konverziója
    for col in ['rate', 'credit_gap', 'credit_to_gdp']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Feature Engineering
    df = df.dropna(subset=['date', 'country']).sort_values(['country', 'date'])
    df['reasoning'] = df.get('justification', pd.Series(dtype='object')).apply(summarize_reasoning)
    df['iso2'] = coco.convert(names=df['country'].tolist(), to='iso2', not_found=None)
    
    return df.reset_index(drop=True)

def calculate_metrics(df):
    print("Calculating metrics (Vectorized)...")
    
    # 1. Latest State (Minden országból a legfrissebb dátum)
    latest_df = df.loc[df.groupby('country')['date'].idxmax()].reset_index(drop=True)

    # 2. Aggregált Trend (Pivot + FFill - SOKKAL GYORSABB mint a ciklus)
    # Sorok: Dátumok, Oszlopok: Országok, Értékek: Ráta
    pivot = df.pivot_table(index='date', columns='country', values='rate', aggfunc='last')
    
    # Forward fill: Ha egy dátumon nincs adat, az előző érvényes marad
    pivot = pivot.ffill().fillna(0)
    
    # Megszámoljuk soronként, hány országban pozitív a ráta
    agg_trend = (pivot > 0).sum(axis=1).reset_index(name='n_positive')
    
    return agg_trend, latest_df

def run_etl():
    ensure_dirs()
    download_data()
    
    df = process_data()
    agg_trend_df, latest_country_df = calculate_metrics(df)
    
    # Mentés Parquet-ba
    df.to_parquet(FILES["processed"], index=False)
    agg_trend_df.to_parquet(FILES["agg_trend"], index=False)
    latest_country_df.to_parquet(FILES["latest"], index=False)
    print("ETL process complete. Data saved.")
    
    return df, agg_trend_df, latest_country_df

if __name__ == "__main__":
    run_etl()
