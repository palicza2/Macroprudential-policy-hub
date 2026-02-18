"""
Reciprocation data: parse ESRB "Matrix of reciprocation" sheet.
Provides Table 1 (measures recommended for reciprocation) and Table 2 (reciprocation status by country).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils import clean_columns, find_header_row

logger = logging.getLogger(__name__)


def _find_sheet(xl: pd.ExcelFile, keywords: List[str]) -> Optional[str]:
    for name in xl.sheet_names:
        if any(k in name for k in keywords):
            return name
    return None


def _cell_to_status(val: Any) -> str:
    """Convert matrix cell (ref like AT.NECI.623 or BE.RECI.666) to display status."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or str(val).strip() == "":
        return "—"
    s = str(val).strip().upper()
    if "RECI" in s and "NECI" not in s:
        return "Reciprocated"
    if "NECI" in s:
        return "Not reciprocated"
    return "—"


def process_reciprocation_matrix(measures_overview_path: Path) -> Dict[str, Any]:
    """
    Parse "Matrix of reciprocation" sheet from ESRB measures overview Excel.
    
    Returns:
        dict with:
          - measures_df: DataFrame for Table 1 (ref, country, authority, year, type_of_measure, basis_union_law, esrb_recommendation, status)
          - matrix_df: DataFrame for Table 2 (same rows, columns = reciprocating countries, values = Reciprocated / Not reciprocated / —)
          - country_columns: list of country names in matrix order
    """
    result = {
        "measures_df": pd.DataFrame(),
        "matrix_df": pd.DataFrame(),
        "country_columns": [],
    }
    if not measures_overview_path.exists():
        logger.warning("Measures overview file not found: %s", measures_overview_path)
        return result

    try:
        xl = pd.ExcelFile(measures_overview_path)
        sheet = _find_sheet(xl, ["Matrix of reciprocation", "matrix of reciprocation"])
        if not sheet:
            logger.warning("Matrix of reciprocation sheet not found")
            return result

        df_raw = xl.parse(sheet, header=None)
        # Header is row 4 (0-based index 4)
        header_row = 4
        if df_raw.shape[0] <= header_row:
            return result

        df = xl.parse(sheet, header=header_row)
        clean_columns(df)

        # Map column names (case-insensitive, partial match)
        col_ref = next((c for c in df.columns if "reference" in str(c).lower() and "measure" in str(c).lower()), None)
        col_country = next((c for c in df.columns if str(c).strip().lower() == "country"), None)
        col_authority = next((c for c in df.columns if "authority" in str(c).lower()), None)
        col_year = next((c for c in df.columns if "year" in str(c).lower() and "initiative" in str(c).lower()), None)
        col_type = next((c for c in df.columns if "type of measure" in str(c).lower()), None)
        col_basis = next((c for c in df.columns if "basis" in str(c).lower() and "union" in str(c).lower()), None)
        col_status = next((c for c in df.columns if "present status" in str(c).lower()), None)
        col_requested = next((c for c in df.columns if "reciprocity" in str(c).lower() and "requested" in str(c).lower()), None)
        col_esrb = next((c for c in df.columns if "esrb" in str(c).lower() and "recommendation" in str(c).lower()), None)

        if not col_ref or not col_country:
            logger.warning("Could not find Reference or Country column in Matrix of reciprocation")
            return result

        # Filter: currently applicable (status contains "currently applicable")
        status_col = df.get(col_status)
        if status_col is not None:
            mask = status_col.astype(str).str.lower().str.contains("currently applicable", na=False)
            df = df.loc[mask].copy()
        else:
            df = df.copy()

        if df.empty:
            result["measures_df"] = pd.DataFrame()
            result["matrix_df"] = pd.DataFrame()
            return result

        # Table 1 columns
        table1_cols = [col_ref, col_country, col_authority, col_year, col_type, col_basis, col_esrb, col_status]
        table1_cols = [c for c in table1_cols if c is not None and c in df.columns]
        measures_df = df[[c for c in table1_cols if c in df.columns]].copy()
        measures_df = measures_df.rename(columns={
            col_ref: "reference",
            col_country: "activating_country",
            col_authority: "authority",
            col_year: "year",
            col_type: "type_of_measure",
            col_basis: "basis_union_law",
            col_esrb: "esrb_recommendation",
            col_status: "status",
        })
        result["measures_df"] = measures_df

        # Country columns: in the Matrix sheet, first 9 columns are measure info, then Austria, Belgium, ...
        known_non_country = {col_ref, col_country, col_authority, col_year, col_type, col_basis, col_status, col_requested, col_esrb}
        known_non_country = {c for c in known_non_country if c is not None}
        country_columns = [str(c).strip() for c in df.columns if c not in known_non_country and str(c).strip() and str(c).strip().lower() not in ("nan", "")]

        result["country_columns"] = country_columns

        # Build Table 2: same rows, columns = country_columns, values = Reciprocated / Not reciprocated / —
        matrix_rows = []
        for idx, row in df.iterrows():
            row_vals = []
            for cc in country_columns:
                if cc not in df.columns:
                    row_vals.append("—")
                    continue
                cell = row.get(cc)
                row_vals.append(_cell_to_status(cell))
            matrix_rows.append(row_vals)

        matrix_df = pd.DataFrame(matrix_rows, columns=country_columns, index=df.index)
        # Prepend measure identifier for display
        refs = df[col_ref].astype(str).values if col_ref in df.columns else [""] * len(df)
        matrix_df.insert(0, "measure_ref", refs)
        result["matrix_df"] = matrix_df

        return result
    except Exception as e:
        logger.exception("Error processing reciprocation matrix: %s", e)
        return result


def render_reciprocation_table1(measures_df: pd.DataFrame) -> str:
    """Render Table 1 HTML: measures currently recommended for reciprocation."""
    if measures_df is None or measures_df.empty:
        return "<p class='no-data'>No measures currently recommended for reciprocation.</p>"
    # Use first columns that exist
    cols = [c for c in ["reference", "activating_country", "type_of_measure", "basis_union_law", "esrb_recommendation", "status"] if c in measures_df.columns]
    if not cols:
        return "<p class='no-data'>No data.</p>"
    sub = measures_df[cols].copy()
    sub = sub.fillna("")
    return sub.to_html(index=False, classes="display-table reciprocation-table reciprocation-table1", escape=False)


def render_reciprocation_table2(measures_df: Optional[pd.DataFrame], matrix_df: pd.DataFrame, country_columns: List[str]) -> str:
    """Render Table 2 HTML: reciprocation status by country (rows = measures, columns = countries)."""
    if matrix_df is None or matrix_df.empty:
        return "<p class='no-data'>No reciprocation matrix data.</p>"
    # Build HTML: header = Measure ref + activating country (from measures_df) + country_columns; cells = status with class
    html_rows = []
    # Header
    header_cells = ["<th>Measure</th>", "<th>Activating country</th>"] + [f"<th>{c}</th>" for c in country_columns]
    html_rows.append("<tr>" + "".join(header_cells) + "</tr>")

    activating = (measures_df["activating_country"].tolist() if measures_df is not None and "activating_country" in measures_df.columns else [""] * len(matrix_df))
    refs = matrix_df["measure_ref"].tolist() if "measure_ref" in matrix_df.columns else [""] * len(matrix_df)
    for i in range(len(matrix_df)):
        ref = refs[i] if i < len(refs) else ""
        act = activating[i] if i < len(activating) else ""
        cells = [f"<td class='reciprocation-ref'>{ref}</td>", f"<td>{act}</td>"]
        for cc in country_columns:
            if cc not in matrix_df.columns:
                cells.append("<td>—</td>")
                continue
            val = matrix_df[cc].iloc[i] if i < len(matrix_df) else "—"
            cls = "reciprocation-reci" if val == "Reciprocated" else ("reciprocation-neci" if val == "Not reciprocated" else "")
            cells.append(f"<td class='{cls}'>{val}</td>")
        html_rows.append("<tr>" + "".join(cells) + "</tr>")

    thead = "<thead>" + html_rows[0] + "</thead>"
    tbody = "<tbody>" + "".join(html_rows[1:]) + "</tbody>"
    return f"<table border='1' class='dataframe display-table reciprocation-table reciprocation-table2'>\n{thead}\n{tbody}\n</table>"
