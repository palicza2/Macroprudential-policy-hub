import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import country_converter as coco
import numpy as np
import re
import os

# --- Konfiguráció ---
DATA_DIR = Path("data")

# URL-ek (csak referenciának, ha a letöltés nem menne)
ESRB_CCYB_URL = "https://www.esrb.europa.eu/national_policy/ccb/shared/data/esrb.ccybd_CCyB_data.xlsx"
ESRB_OVERVIEW_URL = "https://www.esrb.europa.eu/national_policy/shared/pdf/esrb.measures_overview_macroprudential_measures.xlsx"

# Fájlok lehetséges nevei (a könyvtárban lévő fájlokat keressük)
FILES = {
    "ccyb_raw": DATA_DIR / "esrb.ccybd_CCyB_data.xlsx",
    "overview_raw": DATA_DIR / "esrb.measures_overview_macroprudential_measures.xlsx",
    "capital_raw": DATA_DIR / "esrb.measures_overview_capital-based_measures.xlsx", # Alternatív név
    
    # Kimeneti fájlok
    "ccyb_processed": DATA_DIR / "processed_ccyb.parquet",
    "syrb_processed": DATA_DIR / "processed_syrb.parquet",
    "agg_trend": DATA_DIR / "agg_trend.parquet",
    "latest_ccyb": DATA_DIR / "latest_ccyb.parquet",
    "latest_syrb": DATA_DIR / "latest_syrb.parquet"
}

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

# --- 1. Letöltés ---
def download_data():
    # Itt most nem töltünk le, mert a felhasználó feltöltötte a fájlokat
    pass

# --- 2. Segédfüggvények ---

def find_header_row(df, keyword="Country"):
    """
    Megkeresi, hányadik sorban van a fejléc (0-based index).
    """
    for i in range(min(20, len(df))): # Az első 20 sorban keresünk
        row_values = df.iloc[i].astype(str).values
        # Ha bármelyik cellában benne van a kulcsszó (case-insensitive)
        if any(keyword.lower() in val.lower() for val in row_values):
            return i
    return 0 # Fallback: első sor

def clean_columns(df):
    """Eltávolítja a szóközöket és újsorokat az oszlopnevekből."""
    df.columns = df.columns.astype(str).str.strip().str.replace('\n', ' ')
    return df

def summarize_reasoning(text):
    if pd.isna(text) or text == '': return "Standard periodic review"
    text = str(text).lower()
    if "unchanged" in text or "maintain" in text: return "Maintain current rate"
    if "increase" in text or "build-up" in text:
        if "credit" in text: return "Credit growth concerns"
        if "real estate" in text or "housing" in text: return "Real estate risks"
        return "Building resilience"
    if "decrease" in text or "release" in text: return "Support lending"
    if "neutral" in text: return "Positive neutral rate strategy"
    return str(text)[:50] + "..."

def extract_syrb_rate(description):
    if pd.isna(description): return 0.0
    text = str(description).lower()
    matches = re.findall(r'(\d+(?:[.,]\d+)?)\s*(?:%|per cent)', text)
    if matches:
        rates = [float(m.replace(',', '.')) for m in matches]
        return max(rates)
    return 0.0

def determine_sectoral_type(row):
    exposure = str(row.get('exposure_type', '')).lower()
    desc = str(row.get('description', '')).lower()
    sectoral_keywords = ['sectoral', 'real estate', 'housing', 'mortgage', 'commercial', 'rre', 'cre', 'retail']
    if 'all exposures' in exposure and not any(k in desc for k in sectoral_keywords): return 'General'
    if any(k in exposure for k in sectoral_keywords) or any(k in desc for k in sectoral_keywords): return 'Sectoral'
    return 'General'

# --- 3. Adatfeldolgozás ---

def process_ccyb():
    print("Processing CCyB data...")
    try:
        # Fájl keresése (többféle néven is lehet)
        target_file = None
        if FILES["ccyb_raw"].exists(): target_file = FILES["ccyb_raw"]
        # Ha esetleg (1)-es végű lenne a feltöltés miatt
        elif (DATA_DIR / "esrb.ccybd_CCyB_data (1).xlsx").exists(): target_file = DATA_DIR / "esrb.ccybd_CCyB_data (1).xlsx"
        
        if not target_file:
            print("  ⚠️ CCyB raw file missing!")
            return pd.DataFrame()

        # Excel betöltése (összes sheet)
        xl = pd.ExcelFile(target_file)
        # Megpróbáljuk megtalálni a megfelelő sheet-et
        # Preferáljuk a "Data", "dataset", "additionaltable" neveket, vagy az elsőt
        sheet_name = xl.sheet_names[0] # Default
        for name in xl.sheet_names:
            if name.lower() in ['data', 'dataset', 'additionaltable']:
                sheet_name = name
                break
        
        print(f"  -> Using CCyB sheet: '{sheet_name}'")
        
        # Először betöltjük fejléc nélkül a header kereséséhez
        raw_df = pd.read_excel(target_file, sheet_name=sheet_name, header=None, nrows=10)
        header_idx = find_header_row(raw_df, keyword="Country")
        
        # Most betöltjük rendesen
        df = pd.read_excel(target_file, sheet_name=sheet_name, skiprows=header_idx)
        df = clean_columns(df)

        # Oszlop átnevezés (rugalmasabb map)
        rename_map = {
            'Country': 'country',
            'CCyB rate': 'rate',
            'Application since': 'date',
            'Date of Announcement': 'announcement_date',
            'Justification': 'justification',
            'Credit-to-GDP': 'credit_to_gdp',
            'Credit Gap': 'credit_gap'
        }
        df = df.rename(columns=rename_map)
        
        # Adattípusok
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if 'announcement_date' in df.columns:
            df['announcement_date'] = pd.to_datetime(df['announcement_date'], errors='coerce')
        
        if 'rate' in df.columns and df['rate'].dtype == 'object':
             df['rate'] = df['rate'].astype(str).str.replace('%', '').astype(float)

        if 'country' in df.columns:
            df['iso2'] = coco.convert(names=df['country'].tolist(), to='iso2', not_found=None)
            
        df['reasoning'] = df.get('justification', pd.Series(dtype='object')).apply(summarize_reasoning)
        
        df = df.dropna(subset=['date']) if 'date' in df.columns else df
        if not df.empty and 'country' in df.columns and 'date' in df.columns:
             df = df.sort_values(['country', 'date'])
             
        return df.reset_index(drop=True)
        
    except Exception as e:
        print(f"Error processing CCyB: {e}")
        return pd.DataFrame()

def process_syrb():
    print("Processing SyRB data...")
    try:
        # Fájl keresése: Először a macroprudential (history), aztán a capital-based
        target_file = None
        if FILES["overview_raw"].exists(): target_file = FILES["overview_raw"]
        elif FILES["capital_raw"].exists(): target_file = FILES["capital_raw"]
        
        if not target_file:
            print("  ⚠️ SyRB raw file missing!")
            return pd.DataFrame()

        xl = pd.ExcelFile(target_file)
        # Keressük az SRB sheet-et
        sheet_name = next((s for s in xl.sheet_names if "SRB" in s or "Systemic" in s), None)
        
        if not sheet_name:
            print(f"  ⚠️ SRB sheet not found in {target_file.name}. Sheets: {xl.sheet_names}")
            return pd.DataFrame()

        print(f"  -> Using SyRB sheet: '{sheet_name}'")

        # Fejléc keresése
        raw_df = pd.read_excel(target_file, sheet_name=sheet_name, header=None, nrows=20)
        header_idx = find_header_row(raw_df, keyword="Country")
        print(f"  -> SyRB header found at row index {header_idx}")
        
        # Betöltés
        df = pd.read_excel(target_file, sheet_name=sheet_name, skiprows=header_idx)
        df = clean_columns(df)
        df = df.dropna(how='all').dropna(axis=1, how='all')

        # Átnevezés
        rename_map = {
            'Country': 'country',
            'Description of measure': 'description',
            'Measure becomes active on': 'date', # Standard név
            'Active from': 'date',               # Alternatív név
            'Present status of measure': 'status',
            'Type of exposures applied to': 'exposure_type',
            'Interaction with O-SII': 'interaction'
        }
        df = df.rename(columns=rename_map)
        
        # Ha nincs 'date' oszlop, de van 'country', akkor ez valószínűleg csak egy snapshot (nem history)
        if 'date' not in df.columns:
            print(f"  ⚠️ 'date' column missing in SyRB sheet. Available: {df.columns.tolist()}")
            return pd.DataFrame()

        # Adatkinyerés
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['iso2'] = coco.convert(names=df['country'].tolist(), to='iso2', not_found=None)
        df['rate'] = df['description'].apply(extract_syrb_rate)
        df['syrb_type'] = df.apply(determine_sectoral_type, axis=1)
        
        df = df.dropna(subset=['date'])
        df = df.sort_values(['country', 'date'])
        
        return df.reset_index(drop=True)
        
    except Exception as e:
        print(f"Error processing SyRB: {e}")
        return pd.DataFrame()

# --- 4. Metrikák és Fő Futtatás ---

def get_latest_snapshot(df):
    if df.empty: return df
    return df.loc[df.groupby('country')['date'].idxmax()].reset_index(drop=True)

def run_etl():
    ensure_dirs()
    
    # 1. Letöltés (Kihagyva, mert helyi fájlok vannak)
    
    # 2. Feldolgozás
    ccyb_df = process_ccyb()
    syrb_df = process_syrb()
    
    # 3. Metrikák
    latest_ccyb = get_latest_snapshot(ccyb_df)
    latest_syrb = get_latest_snapshot(syrb_df)

    agg_trend = pd.DataFrame()
    if not ccyb_df.empty:
        pivot = ccyb_df.pivot_table(index='date', columns='country', values='rate', aggfunc='last')
        pivot = pivot.ffill().fillna(0)
        agg_trend = (pivot > 0).sum(axis=1).reset_index(name='n_positive')

    # 4. Mentés
    print("Saving parquet files...")
    if not ccyb_df.empty: ccyb_df.to_parquet(FILES["ccyb_processed"])
    if not syrb_df.empty: syrb_df.to_parquet(FILES["syrb_processed"])
    if not latest_ccyb.empty: latest_ccyb.to_parquet(FILES["latest_ccyb"])
    if not latest_syrb.empty: latest_syrb.to_parquet(FILES["latest_syrb"])
    if not agg_trend.empty: agg_trend.to_parquet(FILES["agg_trend"])
    
    print("ETL process complete.")
    
    return {
        'ccyb_df': ccyb_df,
        'syrb_df': syrb_df,
        'agg_trend': agg_trend,
        'latest_ccyb': latest_ccyb,
        'latest_syrb': latest_syrb
    }

if __name__ == "__main__":
    run_etl()