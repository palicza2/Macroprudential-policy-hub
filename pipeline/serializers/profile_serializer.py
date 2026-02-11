"""
Profile serialization utilities.
"""

import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


def serialize_profile(profile):
    """Convert profile data to JSON-serializable format."""
    
    def convert_value(v):
        if v is None:
            return None
        if isinstance(v, (pd.Timestamp, datetime)):
            try:
                return v.isoformat() if pd.notna(v) else None
            except:
                return str(v) if v else None
        elif isinstance(v, pd.Series):
            return v.tolist()
        elif isinstance(v, dict):
            return {k: convert_value(val) for k, val in v.items()}
        elif isinstance(v, list):
            return [convert_value(item) for item in v]
        elif isinstance(v, pd.DataFrame):
            if v.empty:
                return []
            return v.to_dict('records')
        elif pd.isna(v):
            return None
        elif isinstance(v, (int, float)):
            return float(v) if pd.notna(v) else None
        else:
            return v
    
    try:
        return {k: convert_value(v) for k, v in profile.items()}
    except Exception as e:
        logger.warning(f"Error serializing profile: {e}")
        return {}
