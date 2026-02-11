"""
LTV Table HTML Renderer.
Renders structured LTV DataFrame into HTML table.
"""

import pandas as pd


def render_ltv_table_html(df: pd.DataFrame) -> str:
    """
    Render LTV DataFrame as HTML table with proper formatting.
    
    Args:
        df: DataFrame with LTV rules
        
    Returns:
        HTML string
    """
    if df is None or df.empty:
        return "<p class='no-data'>No verified LTV details available yet.</p>"
    
    df_copy = df.copy()
    
    # Format columns for display
    if "Limit_Standard" in df_copy.columns:
        def format_limit_standard(x):
            if pd.isna(x) or x == "" or x is None:
                return ""
            # Handle list (stored as string like "80.0%, 90.0%")
            if isinstance(x, str) and "," in x:
                return x  # Already formatted as string
            # Handle list (if still a list)
            if isinstance(x, list):
                return ", ".join([f"{v:.1f}%" for v in x])
            # Handle single float
            if isinstance(x, (int, float)):
                return f"{x:.1f}%"
            return str(x)
        
        df_copy["Limit_Standard"] = df_copy["Limit_Standard"].apply(format_limit_standard)
    
    if "Limit_FTB" in df_copy.columns:
        df_copy["Limit_FTB"] = df_copy["Limit_FTB"].apply(
            lambda x: f"{x:.1f}%" if pd.notna(x) and isinstance(x, (int, float)) else "—"
        )
    
    if "Limit_BTL" in df_copy.columns:
        df_copy["Limit_BTL"] = df_copy["Limit_BTL"].apply(
            lambda x: f"{x:.1f}%" if pd.notna(x) and isinstance(x, (int, float)) else "—"
        )
    
    # Format Exception_Quota column (show "—" for empty values)
    if "Exception_Quota" in df_copy.columns:
        df_copy["Exception_Quota"] = df_copy["Exception_Quota"].apply(
            lambda x: str(x) if pd.notna(x) and x and str(x).strip() and str(x).strip() != "None"
            else "—"
        )
    
    # Format Notes column (show "—" for empty values)
    if "Notes" in df_copy.columns:
        df_copy["Notes"] = df_copy["Notes"].apply(
            lambda x: str(x) if pd.notna(x) and x and str(x).strip() and str(x).strip() != "None"
            else "—"
        )
    
    # Rename columns for display
    column_rename = {
        "Country": "Country",
        "Implementation_Status": "Status",
        "Legal_Form": "Legal Form",
        "Limit_Standard": "Standard Limit",
        "Limit_FTB": "FTB Limit",
        "Limit_BTL": "BTL Limit",
        "Exception_Quota": "Exception Quota",
        "Notes": "Notes",
    }
    
    df_copy = df_copy.rename(columns=column_rename)
    
    # Select and order columns
    display_columns = [
        "Country", "Status", "Legal Form",
        "Standard Limit", "FTB Limit", "BTL Limit",
        "Exception Quota", "Notes"
    ]
    
    # Only include columns that exist
    display_columns = [col for col in display_columns if col in df_copy.columns]
    df_display = df_copy[display_columns]
    
    # Generate HTML
    html = df_display.to_html(index=False, classes="display-table ltv-table", escape=False)
    
    return html
