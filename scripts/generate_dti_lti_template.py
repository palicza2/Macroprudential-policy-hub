"""
Generate DTI/LTI CSV template file.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from bbm.dti_lti_model import create_dti_lti_schema

def main():
    """Generate empty CSV template."""
    output_path = Path("data/dti_lti_rules.csv")
    df = create_dti_lti_schema()
    
    # Add example rows (commented out in actual CSV, but shown here for reference)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    
    print(f"Generated DTI/LTI template at {output_path}")
    print(f"Columns: {', '.join(df.columns)}")

if __name__ == "__main__":
    main()
