import pandas as pd
from pathlib import Path

file_path = Path('data/esrb.measures_overview_macroprudential_measures.xlsx')
xl = pd.ExcelFile(file_path)
sheet = next((s for s in xl.sheet_names if 'SRB' in s or 'Systemic' in s), None)
df_raw = xl.parse(sheet, header=None, nrows=30)
header_idx = 0
for i, row in df_raw.iterrows():
    row_str = ' '.join(row.astype(str)).lower()
    if 'reference of measure' in row_str or 'country' in row_str:
        header_idx = i
        break
df = xl.parse(sheet, skiprows=header_idx)
df.columns = [c.strip() for c in df.columns]

col = 'Has the measure been revoked or replaced?'
if col in df.columns:
    print(df[col].value_counts())
    print("\nSample with revocation note:")
    print(df[df[col].notna()][['Country', col, 'Date of revocation/ replacement', 'Note of revocation/ replacement']].head(10))
else:
    print(f"Column {col} not found. Available: {df.columns.tolist()}")
