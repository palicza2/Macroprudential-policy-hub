"""
DTI Expert Table Renderer.
Renders the expert-verified DTI table (Excel schema) in English.
"""
import pandas as pd


# Display column order for expert DTI schema
EXPERT_DISPLAY_COLUMNS = [
    "Country",
    "Type",
    "Debt Counted (Numerator)",
    "Legal Form",
    "Standard Limit",
    "Preferential Limit (FTB/Green/Age)",
    "Income Basis",
    "Portfolio Limit",
    "Nature of Breach (Hard Cap)",
    "Exemptions",
    "Regulation Link",
]


def render_dti_expert_table_html(df: pd.DataFrame) -> str:
    """
    Render DTI expert DataFrame as HTML table.
    
    Args:
        df: DataFrame from load_dti_expert_table (English schema)
        
    Returns:
        HTML string
    """
    if df is None or df.empty:
        return "<p class='no-data'>No DTI/LTI data available.</p>"

    df_copy = df.copy()

    # Select and order columns that exist
    display_cols = [c for c in EXPERT_DISPLAY_COLUMNS if c in df_copy.columns]
    if not display_cols:
        display_cols = list(df_copy.columns)

    df_display = df_copy[display_cols].fillna("—")

    # Replace empty strings with em dash
    for col in df_display.columns:
        df_display[col] = df_display[col].astype(str).replace("", "—").replace("nan", "—")

    html = df_display.to_html(
        index=False,
        classes="display-table dti-lti-table dti-expert-table",
        escape=False,
    )

    return html
