"""Check DTI/LTI CSV for SK, DK, LV."""
import pandas as pd
from pathlib import Path

csv_path = Path("data/dti_lti_rules.csv")
if csv_path.exists():
    df = pd.read_csv(csv_path)
    print("=== CSV Results ===")
    print(f"Total rows: {len(df)}")
    print(f"Countries: {sorted(df['Country'].unique().tolist()) if not df.empty and 'Country' in df.columns else []}")
    
    sk_dk_lv = df[df['Country'].isin(['SK', 'DK', 'LV'])] if not df.empty and 'Country' in df.columns else pd.DataFrame()
    print(f"\nSK, DK, LV rows: {len(sk_dk_lv)}")
    if not sk_dk_lv.empty:
        print("\nSK, DK, LV details:")
        print(sk_dk_lv[['Country', 'Measure_Code', 'Limit_Standard', 'Legal_Form', 'Income_Basis']].to_string())
    else:
        print("SK, DK, LV not found in CSV")
        
    # Show all rows
    print("\n=== All rows ===")
    print(df[['Country', 'Measure_Code', 'Limit_Standard']].to_string())
else:
    print(f"CSV file not found: {csv_path}")
