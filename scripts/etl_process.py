import pandas as pd
import country_converter as coco
import re
import requests
import shutil
import logging
import warnings
from pathlib import Path

# Csendesebb működés
warnings.simplefilter(action='ignore', category=FutureWarning)
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("ETL")

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

URLS = {
    "syrb": "https://www.esrb.europa.eu/national_policy/shared/pdf/esrb.measures_overview_macroprudential_measures.xlsx",
    "ccyb": "https://www.esrb.europa.eu/national_policy/ccb/shared/data/esrb.ccybd_CCyB_data.xlsx"
}

FILES = {
    "syrb_source": DATA_DIR / "esrb.measures_overview_macroprudential_measures.xlsx",
    "ccyb_source": DATA_DIR / "esrb.ccybd_CCyB_data.xlsx",
    "syrb_processed": DATA_DIR / "processed_syrb.parquet",
    "latest_syrb": DATA_DIR / "latest_syrb.parquet",
    "ccyb_processed": DATA_DIR / "processed_ccyb.parquet",
    "latest_ccyb": DATA_DIR / "latest_ccyb.parquet"
}

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def download_file_safely(url, target_path):
    print(f"  ⬇️ Letöltés: {target_path.name}...")
    temp_path = target_path.with_suffix('.tmp')
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        with open(temp_path, 'wb') as f: f.write(r.content)
        if temp_path.stat().st_size < 1000: raise ValueError("Túl kicsi fájl")
        shutil.move(temp_path, target_path)
        return True
    except Exception as e:
        print(f"  ⚠️ Hiba a letöltésnél ({e}). A meglévő fájlt használjuk.")
        if temp_path.exists(): temp_path.unlink()
        return False

def clean_columns(df):
    df.columns = df.columns.astype(str).str.strip().str.replace('\n', ' ').str.replace('  ', ' ')
    return df

def find_header_row(df, keyword="Country"):
    for i in range(min(20, len(df))):
        if any(keyword.lower() in str(val).lower() for val in df.iloc[i].values): return i
    return 0

def extract_rate(text):
    if pd.isna(text): return 0.0
    text = str(text).lower().replace(',', '.')
    matches = re.findall(r'(\d+(?:\.\d+)?)', text)
    valid = [float(m) for m in matches if not (float(m).is_integer() and 1990 <= float(m) <= 2030) and float(m) <= 50]
    return max(valid) if valid else 0.0

# --- FELDOLGOZÓK ---
def process_syrb(file_path):
    if not file_path.exists(): return pd.DataFrame()
    try:
        xl = pd.ExcelFile(file_path)
        sheet = next((s for s in xl.sheet_names if "SRB" in s or "Systemic" in s), None)
        if not sheet: return pd.DataFrame()

        df = xl.parse(sheet, skiprows=find_header_row(xl.parse(sheet, header=None, nrows=20)))
        df = clean_columns(df)
        
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if 'country' in cl: col_map['country'] = c
            elif 'active' in cl or 'applicable' in cl: col_map['date'] = c
            elif 'description' in cl: col_map['description'] = c
            elif 'exposures' in cl: col_map['exposure_type'] = c
            elif 'rate' in cl and 'guide' not in cl: col_map['rate_col'] = c
        
        if 'date' not in col_map: 
            col_map['date'] = next((c for c in df.columns if 'date' in c.lower()), None)

        if not col_map: return pd.DataFrame()
        df = df.rename(columns={v: k for k, v in col_map.items()})

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        phase_col = next((c for c in df.columns if 'phase' in c.lower()), None)
        if phase_col: df['date'] = df['date'].fillna(pd.to_datetime(df[phase_col], errors='coerce'))
        df = df.dropna(subset=['date'])
        
        df['iso2'] = coco.convert(names=df['country'].tolist(), to='iso2', not_found=None)
        
        if 'rate_col' in df.columns:
            df['rate'] = pd.to_numeric(df['rate_col'], errors='coerce').fillna(df['description'].apply(extract_rate))
        else:
            df['rate'] = df['description'].apply(extract_rate)
            
        mask_zero = (df['rate'] == 0) & (df['description'].notna())
        if mask_zero.any():
            df.loc[mask_zero, 'rate'] = df.loc[mask_zero, 'description'].apply(extract_rate)

        def tag_exp(row):
            t = f"{row.get('exposure_type', '')} {row.get('description', '')}".lower()
            if 'resident' in t and 'commercial' in t: return "CRE & RRE exposures"
            if 'resident' in t or 'housing' in t: return "RRE exposures"
            if 'commercial' in t: return "CRE exposures"
            orig = str(row.get('exposure_type', 'Sectoral'))
            return orig if len(orig) < 50 else "Sectoral"

        df['exposure_type'] = df.apply(tag_exp, axis=1)
        df['syrb_type'] = df.apply(lambda r: 'Sectoral' if any(x in str(r['exposure_type']).lower() for x in ['cre', 'rre', 'sectoral']) else 'General', axis=1)
        
        return df.sort_values(['country', 'date'], ascending=[True, False]).reset_index(drop=True)
    except Exception: return pd.DataFrame()

def process_ccyb(file_path):
    if not file_path.exists(): return pd.DataFrame()
    try:
        xl = pd.ExcelFile(file_path)
        df = xl.parse(0, skiprows=find_header_row(xl.parse(0, header=None, nrows=20)))
        df = clean_columns(df)
        
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if 'country' in cl: col_map['country'] = c
            elif 'application since' in cl: col_map['date'] = c
            elif 'decision on' in cl: col_map['decision_date'] = c
            elif 'rate' in cl and 'guide' not in cl: col_map['rate'] = c
            elif 'setting' in cl: col_map['status'] = c

        if 'date' not in col_map and 'decision_date' in col_map: col_map['date'] = col_map['decision_date']
        if 'date' not in col_map or 'rate' not in col_map: return pd.DataFrame()

        df = df.rename(columns={v: k for k, v in col_map.items()})
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date', 'country'])
        df['iso2'] = coco.convert(names=df['country'].tolist(), to='iso2', not_found=None)
        df['rate'] = df['rate'].astype(str).str.replace('%', '').str.replace(',', '.')
        df['rate'] = pd.to_numeric(df['rate'], errors='coerce').fillna(0.0)
        
        gap_col = next((c for c in df.columns if 'gap' in c.lower()), None)
        if gap_col: df['credit_gap'] = pd.to_numeric(df[gap_col], errors='coerce').fillna(0.0)
        else: df['credit_gap'] = 0.0

        return df.sort_values(['country', 'date'], ascending=[True, False]).reset_index(drop=True)
    except Exception: return pd.DataFrame()

def calculate_trends(ccyb_df, syrb_df):
    agg_trend_ccyb = pd.DataFrame()
    syrb_trend = pd.DataFrame()

    # CCyB Trend
    if not ccyb_df.empty:
        try:
            pivot = ccyb_df.pivot_table(index='date', columns='country', values='rate', aggfunc='max')
            if not pivot.empty:
                pivot = pivot.resample('D').ffill().ffill()
                agg_trend_ccyb = (pivot > 0).sum(axis=1).reset_index(name='n_positive')
        except Exception: pass

    # SyRB Trend
    if not syrb_df.empty:
        try:
            gen = syrb_df[syrb_df['syrb_type'] == 'General']
            sec = syrb_df[syrb_df['syrb_type'] == 'Sectoral']
            
            t_gen = pd.Series(dtype=float)
            t_sec = pd.Series(dtype=float)

            if not gen.empty:
                t_gen = (gen.pivot_table(index='date', columns='country', values='rate', aggfunc='max')
                         .resample('D').ffill().ffill().fillna(0) > 0).sum(axis=1)
            
            if not sec.empty:
                t_sec = (sec.pivot_table(index='date', columns='country', values='rate', aggfunc='max')
                         .resample('D').ffill().ffill().fillna(0) > 0).sum(axis=1)
            
            syrb_trend = pd.DataFrame({'General SyRB': t_gen, 'Sectoral SyRB': t_sec}).fillna(0).sort_index().reset_index()
            # Rename index to date if needed
            if 'index' in syrb_trend.columns: syrb_trend = syrb_trend.rename(columns={'index': 'date'})
        except Exception: pass

    return agg_trend_ccyb, syrb_trend

# --- MAIN ---
def run_etl():
    ensure_dirs()
    print("--- 1. ADATFRISSÍTÉS ---")
    download_file_safely(URLS["syrb"], FILES["syrb_source"])
    download_file_safely(URLS["ccyb"], FILES["ccyb_source"])

    syrb_df = process_syrb(FILES["syrb_source"])
    ccyb_df = process_ccyb(FILES["ccyb_source"])
    
    print(f"  📊 Feldolgozva: SyRB ({len(syrb_df)} sor), CCyB ({len(ccyb_df)} sor)")

    def get_latest(df): return df.loc[df.groupby('country')['date'].idxmax()].reset_index(drop=True) if not df.empty else df
    latest_syrb = get_latest(syrb_df)
    latest_ccyb = get_latest(ccyb_df)
    
    # Trendek
    agg_trend_df, syrb_trend_df = calculate_trends(ccyb_df, syrb_df)

    # Mentés
    if not syrb_df.empty: syrb_df.to_parquet(FILES["syrb_processed"])
    if not ccyb_df.empty: ccyb_df.to_parquet(FILES["ccyb_processed"])
    
    return {
        'ccyb_df': ccyb_df, 
        'syrb_df': syrb_df,
        'agg_trend_df': agg_trend_df,
        'syrb_trend_df': syrb_trend_df,
        'latest_country_df': latest_ccyb,
        'latest_syrb_df': latest_syrb
    }

if __name__ == "__main__":
    run_etl()