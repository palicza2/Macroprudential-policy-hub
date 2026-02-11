"""Test AI validation for DK to see if it fills missing limit_standard."""
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from llm_analysis import LLMAnalyzer
from config import LLM_CONFIG
from bbm.dti_lti_builder import build_dti_lti_comparison_df_structured

# Read BBM data
bbm = pd.read_parquet('data/processed_bbm.parquet')
print("=== Testing AI Validation for DK ===")

analyzer = LLMAnalyzer(LLM_CONFIG)
df = build_dti_lti_comparison_df_structured(
    bbm, 
    analyzer, 
    validate_with_ai=True, 
    final_validation_with_search=False,
    search_config=None
)

dk_row = df[df['Country'] == 'DK']
print(f"\nDK row:")
print(dk_row[['Country', 'Measure_Code', 'Limit_Standard', 'Legal_Form', 'Income_Basis']].to_string() if not dk_row.empty else "DK not found")
