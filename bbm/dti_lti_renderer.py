"""
DTI/LTI Table Renderer.
Renders structured DTI/LTI table with proper formatting for new columns.
"""

import pandas as pd
from pathlib import Path


def render_dti_lti_table_html(df: pd.DataFrame) -> str:
    """
    Render DTI/LTI DataFrame as HTML table with proper formatting.
    
    Args:
        df: DataFrame with DTI/LTI rules
        
    Returns:
        HTML string
    """
    if df is None or df.empty:
        return "<p class='no-data'>No verified DTI/LTI details available yet.</p>"
    
    df_copy = df.copy()
    
    # Format columns for display
    if "Limit_Standard" in df_copy.columns:
        df_copy["Limit_Standard"] = df_copy["Limit_Standard"].apply(
            lambda x: f"{x:.1f}x" if pd.notna(x) and isinstance(x, (int, float)) else ""
        )
    
    if "Limit_FTB" in df_copy.columns:
        df_copy["Limit_FTB"] = df_copy["Limit_FTB"].apply(
            lambda x: f"{x:.1f}x" if pd.notna(x) and isinstance(x, (int, float)) else "—"
        )
    
    if "Limit_BTL" in df_copy.columns:
        df_copy["Limit_BTL"] = df_copy["Limit_BTL"].apply(
            lambda x: f"{x:.1f}x" if pd.notna(x) and isinstance(x, (int, float)) else "—"
        )
    
    if "Limit_Green" in df_copy.columns:
        df_copy["Limit_Green"] = df_copy["Limit_Green"].apply(
            lambda x: f"{x:.1f}x" if pd.notna(x) and isinstance(x, (int, float)) else "—"
        )
    
    # Format Regulation_URL as clickable links
    if "Regulation_URL" in df_copy.columns:
        df_copy["Regulation_URL"] = df_copy["Regulation_URL"].apply(
            lambda x: f'<a href="{x}" target="_blank" rel="noopener noreferrer">Link</a>' 
            if pd.notna(x) and x and str(x).strip() and str(x).strip() != "None" 
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
        "Measure_Code": "Type",
        "Implementation_Status": "Status",
        "Legal_Form": "Legal Form",
        "Limit_Standard": "Standard Limit",
        "Limit_FTB": "FTB Limit",
        "Limit_BTL": "BTL Limit",
        "Limit_Green": "Green Limit",
        "Income_Basis": "Income Basis",
        "Allowance_Share": "Allowance",
        "Regulation_URL": "Regulation Link",
        "Notes": "Notes",
    }
    
    df_copy = df_copy.rename(columns=column_rename)
    
    # Select and order columns
    display_columns = [
        "Country", "Type", "Status", "Legal Form",
        "Standard Limit", "FTB Limit", "BTL Limit", "Green Limit",
        "Income Basis", "Allowance", "Regulation Link", "Notes"
    ]
    
    # Only include columns that exist
    display_columns = [col for col in display_columns if col in df_copy.columns]
    df_display = df_copy[display_columns]
    
    # Generate HTML
    html = df_display.to_html(index=False, classes="display-table dti-lti-table", escape=False)
    
    return html


def save_dti_lti_csv(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save DTI/LTI DataFrame to CSV file.
    
    Args:
        df: DataFrame with DTI/LTI rules
        output_path: Path to save CSV file
    """
    if df is None or df.empty:
        return
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
