"""Test full DTI/LTI pipeline with detailed logging."""
import logging
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

from llm_analysis import LLMAnalyzer
from config import LLM_CONFIG
from bbm.dti_lti_builder import build_dti_lti_comparison_df_structured

# Read BBM data
bbm = pd.read_parquet('data/processed_bbm.parquet')
print("=== Full Pipeline Test ===")

analyzer = LLMAnalyzer(LLM_CONFIG)
df = build_dti_lti_comparison_df_structured(
    bbm, 
    analyzer, 
    validate_with_ai=False, 
    final_validation_with_search=False,
    search_config=None
)

print(f"\n=== Final Result ===")
print(f"Total rows: {len(df)}")
print(f"Countries: {sorted(df['Country'].unique().tolist()) if not df.empty and 'Country' in df.columns else []}")

sk_dk_lv = df[df['Country'].isin(['SK', 'DK', 'LV'])] if not df.empty and 'Country' in df.columns else pd.DataFrame()
print(f"\nSK, DK, LV rows: {len(sk_dk_lv)}")
if not sk_dk_lv.empty:
    print("\nSK, DK, LV details:")
    print(sk_dk_lv[['Country', 'Measure_Code', 'Limit_Standard', 'Legal_Form']].to_string())
else:
    print("SK, DK, LV NOT FOUND in final DataFrame")
