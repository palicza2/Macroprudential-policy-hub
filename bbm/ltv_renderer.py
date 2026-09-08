"""
LTV table HTML renderer.
"""

import pandas as pd

from .ltv_model import migrate_legacy_ltv_columns


def _dash_empty(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    text = str(value).strip()
    if not text or text in {"None", "nan"}:
        return "—"
    return text


def render_ltv_table_html(df: pd.DataFrame) -> str:
    """Render LTV DataFrame as HTML with FTB/OOO, SSB/BTL, and other-limit columns."""
    if df is None or df.empty:
        return "<p class='no-data'>No verified LTV details available yet.</p>"

    df_copy = migrate_legacy_ltv_columns(df)

    for col in ("Limit_FTB_OOO", "Limit_SSB_BTL", "Other_Limits", "Exception_Quota", "Notes"):
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(_dash_empty)

    df_copy = df_copy.rename(columns={
        "Country": "Country",
        "Implementation_Status": "Status",
        "Legal_Form": "Legal Form",
        "Limit_FTB_OOO": "FTB / OOO",
        "Limit_SSB_BTL": "SSB / BTL",
        "Other_Limits": "Other limits",
        "Exception_Quota": "Exception Quota",
        "Notes": "Notes",
    })

    display_columns = [
        "Country", "Status", "Legal Form",
        "FTB / OOO", "SSB / BTL", "Other limits",
        "Exception Quota", "Notes",
    ]
    display_columns = [c for c in display_columns if c in df_copy.columns]
    df_display = df_copy[display_columns]

    html = df_display.to_html(index=False, classes="display-table ltv-table", escape=False)
    # Helpful header titles for the short column names
    html = html.replace(
        "<th>FTB / OOO</th>",
        '<th title="First-time buyer (FTB) or owner-occupied (OOO)">FTB / OOO</th>',
    )
    html = html.replace(
        "<th>SSB / BTL</th>",
        '<th title="Second/subsequent buyer (SSB) or buy-to-let (BTL)">SSB / BTL</th>',
    )
    html = html.replace(
        "<th>Other limits</th>",
        '<th title="Other differentiations (green, secondary home, FX, …)">Other limits</th>',
    )
    return html
