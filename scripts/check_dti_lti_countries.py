"""Check SK, DK, LV DTI/LTI records in ESRB data."""
import pandas as pd
from pathlib import Path

# Read BBM data
bbm_path = Path("data/processed_bbm.parquet")
if bbm_path.exists():
    bbm = pd.read_parquet(bbm_path)
    print("BBM columns:", list(bbm.columns))
    print("\n=== SK, DK, LV records ===")
    
    # Filter for SK, DK, LV
    sk_dk_lv = bbm[bbm['iso2'].isin(['SK', 'DK', 'LV'])].copy()
    
    if not sk_dk_lv.empty:
        print(f"\nTotal records for SK, DK, LV: {len(sk_dk_lv)}")
        print("\nMeasure types:")
        print(sk_dk_lv['measure_type'].value_counts())
        
        print("\nActive status:")
        print(sk_dk_lv['active_status'].value_counts() if 'active_status' in sk_dk_lv.columns else "No active_status column")
        
        print("\nSample records:")
        cols_to_show = ['iso2', 'country', 'measure_type', 'active_status', 'description']
        available_cols = [c for c in cols_to_show if c in sk_dk_lv.columns]
        print(sk_dk_lv[available_cols].head(10))
        
        # Check for DTI/LTI mentions in description
        print("\n=== DTI/LTI mentions in descriptions ===")
        for iso2 in ['SK', 'DK', 'LV']:
            country_records = sk_dk_lv[sk_dk_lv['iso2'] == iso2]
            if not country_records.empty:
                print(f"\n{iso2} ({country_records.iloc[0]['country'] if 'country' in country_records.columns else ''}):")
                for idx, row in country_records.iterrows():
                    desc = str(row.get('description', ''))[:200] if 'description' in row else ''
                    measure = row.get('measure_type', '')
                    status = row.get('active_status', '') if 'active_status' in row else ''
                    print(f"  - {measure} ({status}): {desc}...")
    else:
        print("No records found for SK, DK, LV")
else:
    print(f"BBM file not found: {bbm_path}")
