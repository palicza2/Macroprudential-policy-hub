"""Test DTI/LTI extraction for SK, DK, LV."""
import pandas as pd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import from bbm package
from bbm import build_dti_lti_items

# Read BBM data
bbm_path = Path("data/processed_bbm.parquet")
if bbm_path.exists():
    bbm = pd.read_parquet(bbm_path)
    print("=== Testing build_dti_lti_items ===")
    
    items = build_dti_lti_items(bbm)
    print(f"\nTotal items found: {len(items)}")
    
    # Filter for SK, DK, LV
    sk_dk_lv_items = [it for it in items if it.get("iso2") in ["SK", "DK", "LV"]]
    print(f"\nSK, DK, LV items: {len(sk_dk_lv_items)}")
    
    for item in sk_dk_lv_items:
        print(f"\n{item.get('iso2')} ({item.get('country')}):")
        print(f"  Measure: {item.get('measure_short')}")
        print(f"  Description (first 150 chars): {item.get('description', '')[:150]}...")
else:
    print(f"BBM file not found: {bbm_path}")
