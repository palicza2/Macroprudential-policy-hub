import pandas as pd
import re
import io
import base64
import os
import sys
import requests
import shutil
from pathlib import Path
from contextlib import contextmanager

@contextmanager
def SuppressOutput():
    """
    Operációs rendszer szintű kimenet elnémítása (stdout és stderr).
    Ez elkapja a C-szintű könyvtárak (pl. Kaleido/Chromium) zaját is.
    """
    # 1. Mentjük az eredeti csatornákat
    original_stdout_fd = sys.stdout.fileno()
    original_stderr_fd = sys.stderr.fileno()

    saved_stdout_fd = os.dup(original_stdout_fd)
    saved_stderr_fd = os.dup(original_stderr_fd)

    # 2. Megnyitjuk a "semmit" (null device)
    devnull = os.open(os.devnull, os.O_RDWR)

    try:
        # 3. Átirányítjuk a kimeneteket a null-ba
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(devnull, original_stdout_fd)
        os.dup2(devnull, original_stderr_fd)
        yield
    finally:
        # 4. Visszaállítjuk az eredeti állapotot
        os.dup2(saved_stdout_fd, original_stdout_fd)
        os.dup2(saved_stderr_fd, original_stderr_fd)
        
        # 5. Takarítás
        os.close(saved_stdout_fd)
        os.close(saved_stderr_fd)
        os.close(devnull)

def ensure_dirs(*dirs: Path):
    for d in dirs: d.mkdir(parents=True, exist_ok=True)

def download_file_safely(url, target_path):
    print(f"  Downloading: {target_path.name}...")
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
        print(f"  Download error ({e}). Using existing file.")
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
    text_str = str(text).lower().replace(',', '.')
    matches = re.findall(r'(\d+(?:\.\d+)?)', text_str)
    valid_rates = []
    for m in matches:
        val = float(m)
        if (val.is_integer() and 1990 <= val <= 2030) or val > 50: continue
        valid_rates.append(val)
    return max(valid_rates) if valid_rates else 0.0

def create_download_link(df, title="Download Data"):
    if df is None or df.empty: return ""
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        b64 = base64.b64encode(output.getvalue()).decode()
        return f'<div style="text-align:right;margin-top:5px;"><a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="data.xlsx" style="color:#27ae60;text-decoration:none;font-weight:bold;">📊 {title}</a></div>'
    except Exception: return ""