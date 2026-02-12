"""Test BBM DTI/LTI extraction for SK, DK, LV."""
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import from bbm package
from bbm import build_dti_lti_items

# Read BBM data
bbm = pd.read_parquet('data/processed_bbm.parquet')
print("=== Testing build_dti_lti_items ===")

items = build_dti_lti_items(bbm)
print(f'\nTotal items: {len(items)}')

# Filter for SK, DK, LV
sk_dk_lv = [it for it in items if it.get('iso2') in ['SK', 'DK', 'LV']]
print(f'\nSK, DK, LV items: {len(sk_dk_lv)}')

for it in sk_dk_lv:
    print(f"  {it.get('iso2')} ({it.get('country')}): {it.get('measure_short')} - {it.get('description', '')[:80]}...")
