"""
HTML utilities.
HTML generation functions.
"""

import io
import base64
import pandas as pd


def create_download_link(df, title="Download Data"):
    """
    Create a download link for a DataFrame as Excel file.
    
    Args:
        df: DataFrame to create download link for
        title: Link title text
        
    Returns:
        HTML string with download link, or empty string if df is empty
    """
    if df is None or df.empty:
        return ""
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        b64 = base64.b64encode(output.getvalue()).decode()
        return (
            f'<div style="text-align:right;margin-top:5px;">'
            f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" '
            f'download="data.xlsx" style="color:#27ae60;text-decoration:none;font-weight:bold;">'
            f'📊 {title}</a></div>'
        )
    except Exception:
        return ""
