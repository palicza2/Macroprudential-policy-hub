"""Debug DTI/LTI extraction process."""
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from llm_analysis import LLMAnalyzer
from config import LLM_CONFIG

# Import from bbm package
from bbm import build_dti_lti_items

from bbm.dti_lti_builder import build_dti_lti_comparison_df_structured

# Read BBM data
bbm = pd.read_parquet('data/processed_bbm.parquet')
print("=== Step 1: build_dti_lti_items ===")
items = build_dti_lti_items(bbm)
print(f"Total items: {len(items)}")

# Filter for SK, DK, LV
sk_dk_lv_items = [it for it in items if it.get('iso2') in ['SK', 'DK', 'LV']]
print(f"SK, DK, LV items: {len(sk_dk_lv_items)}")
for it in sk_dk_lv_items:
    print(f"  {it.get('iso2')} ({it.get('country')}): {it.get('measure_short')}")

# Step 2: AI confirmation
print("\n=== Step 2: AI confirmation ===")
analyzer = LLMAnalyzer(LLM_CONFIG)
confirmations = analyzer.confirm_dti_lti_presence(items)
print(f"Confirmations: {len(confirmations)}")

# Check confirmations for SK, DK, LV
for idx, (it, c) in enumerate(zip(items, confirmations)):
    if it.get('iso2') in ['SK', 'DK', 'LV']:
        print(f"\n{it.get('iso2')} ({it.get('country')}):")
        print(f"  Confirmation: {c}")
        print(f"  Type: {type(c)}")
        if isinstance(c, dict):
            print(f"  confirmed: {c.get('confirmed', 'N/A')}")
            print(f"  confidence: {c.get('confidence', 'N/A')}")
            print(f"  type: {c.get('type', 'N/A')}")

# Step 3: Build comparison
print("\n=== Step 3: Build comparison ===")
df = build_dti_lti_comparison_df_structured(bbm, analyzer, validate_with_ai=False)
print(f"Result DataFrame rows: {len(df)}")
print(f"Countries: {sorted(df['Country'].unique().tolist()) if not df.empty and 'Country' in df.columns else []}")

sk_dk_lv_df = df[df['Country'].isin(['SK', 'DK', 'LV'])] if not df.empty and 'Country' in df.columns else pd.DataFrame()
print(f"\nSK, DK, LV in result: {len(sk_dk_lv_df)}")
if not sk_dk_lv_df.empty:
    print(sk_dk_lv_df[['Country', 'Measure_Code', 'Limit_Standard']].to_string())
