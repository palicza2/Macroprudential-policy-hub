from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from exports import ensure_report_dirs, write_download, write_partial, write_plot_html


def _iso2_to_flag_img(iso2: str) -> str:
    """Return an <img> tag for the country flag (works where Unicode flag emoji does not)."""
    if not iso2 or len(iso2) != 2 or not iso2.isalpha():
        return ""
    code = iso2.upper()
    # flagcdn.com uses ISO 3166-1 alpha-2: UK display code -> gb for URL
    url_code = "gb" if code == "UK" else code.lower()
    url = f"https://flagcdn.com/w40/{url_code}.png"
    return f'<img src="{url}" alt="" width="24" height="18" class="country-flag" style="vertical-align:middle;margin-right:4px">'


def _normalize_capital_country_code(value: Any) -> str:
    """Return two-letter country code for capital stack table. GB -> UK, Norway -> NO."""
    if value is None or pd.isna(value):
        return ""
    s = str(value).strip()
    if len(s) == 2 and s.isalpha():
        if s.upper() == "GB":
            return "UK"
        return s.upper()
    # Resolve names to ISO2
    if "norway" in s.lower() or "norwegian" in s.lower():
        return "NO"
    try:
        import country_converter as coco
        out = coco.convert(names=[s], to="ISO2", not_found=None)
        if out and (isinstance(out, list) and out[0] or isinstance(out, str) and out):
            code = out[0] if isinstance(out, list) else out
            if code and len(str(code)) == 2:
                return "UK" if str(code).upper() == "GB" else str(code).upper()
    except Exception:
        pass
    return s[:2].upper() if len(s) >= 2 else s.upper()


def render_capital_overall_table(df: pd.DataFrame) -> str:
    """
    Render capital stack table: two-letter country codes (UK not GB, NO for Norway),
    flags beside codes, buffer columns as decimals rounded to 2 digits with comma (e.g. 2,50).
    """
    if df is None or df.empty:
        return "<p class='no-data'>No Data</p>"
    if "ISO2" not in df.columns:
        return df.to_html(index=False, classes="display-table", escape=False)

    numeric_cols = [c for c in ["CCoB", "CCyB", "GSII/O-SII", "SyRB", "sSyRB", "TOTAL"] if c in df.columns]
    rows = []
    for _, row in df.iterrows():
        raw_iso2 = row.get("ISO2", "")
        code = _normalize_capital_country_code(raw_iso2)
        if len(code) != 2:
            continue
        flag_img = _iso2_to_flag_img(code)
        cell_country = (flag_img + " " + code) if flag_img else code
        cells = [f"<td>{cell_country}</td>"]
        for col in numeric_cols:
            val = row.get(col, 0)
            try:
                num = float(val)
                formatted = f"{num:.2f}".replace(".", ",")
            except (TypeError, ValueError):
                formatted = str(val)
            cells.append(f"<td class='rate-cell'>{formatted}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")

    if not rows:
        return "<p class='no-data'>No Data</p>"
    header_cells = ["<th>Country</th>"] + [f"<th>{c}</th>" for c in numeric_cols]
    thead = "<thead><tr>" + "".join(header_cells) + "</tr></thead>"
    tbody = "<tbody>" + "".join(rows) + "</tbody>"
    return f"<table border='1' class='dataframe display-table capital-overall-table'>\n{thead}\n{tbody}\n</table>"


def df_to_html_table(df: pd.DataFrame, table_type: str = None) -> str:
    """
    Convert DataFrame to HTML table.
    
    Args:
        df: DataFrame to convert
        table_type: Optional table type hint (e.g., "dti_lti") for specialized rendering
    """
    if df is None or df.empty:
        return "<p class='no-data'>No Data</p>"
    
    # Use specialized renderer for DTI/LTI tables
    if table_type == "dti_lti" or ("Measure_Code" in df.columns and "Limit_Standard" in df.columns):
        try:
            from bbm.dti_lti_renderer import render_dti_lti_table_html
            return render_dti_lti_table_html(df)
        except ImportError:
            # Fallback to standard rendering
            pass
    
    # Use specialized renderer for LTV tables
    if table_type == "ltv" or ("Limit_Standard" in df.columns and "Exception_Quota" in df.columns and "Measure_Code" not in df.columns):
        try:
            from bbm.ltv_renderer import render_ltv_table_html
            return render_ltv_table_html(df)
        except ImportError:
            # Fallback to standard rendering
            pass
    
    df_copy = df.copy()
    for col in ["DETAILS", "REASONS", "JUSTIFICATION", "FTB DETAILS", "OTHER EXCEPTIONS", "SUMMARY"]:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(lambda x: (str(x)[:200] + "...") if len(str(x)) > 200 else x)
    return df_copy.to_html(index=False, classes="display-table", escape=False)


def render_report(
    *,
    base_dir: Path,
    reports_dir: Path,
    template_dir: Path,
    template_name: str,
    generation_date: str,
    analyses: Dict[str, Any],
    plots_inline: Dict[str, str],
    plot_figs: Dict[str, Any],
    download_data: Dict[str, pd.DataFrame],
    tables_html: Dict[str, str],
    news_feed_html: str,
    bbm_ref_date: str,
    ltv_ref_date: str,
    countries_data: Dict[str, Any] = None,
    knowledge_graph_json: str = '{"nodes": [], "edges": []}',
    osii_countries: list = None,
    osii_table_html: str = "",
    all_sii_table_html: str = "",
    osii_by_country: Dict = None,
    supabase_url: str = "",
    supabase_key: str = "",
) -> str:
    dirs = ensure_report_dirs(reports_dir)
    partials_dir = dirs["partials"]
    plots_dir = dirs["plots"]
    downloads_dir = dirs["downloads"]

    table_files = {k: write_partial(base_dir, partials_dir, k, v) for k, v in tables_html.items()}
    plot_files = {k: write_plot_html(base_dir, plots_dir, k, v) for k, v in plot_figs.items()}
    download_links = {k: write_download(base_dir, downloads_dir, k, v) for k, v in download_data.items()}

    env = Environment(loader=FileSystemLoader(str(template_dir)))
    
    # Serialize countries_data to JSON for embedding in HTML
    countries_data_json = "{}"
    if countries_data and len(countries_data) > 0:
        import json
        try:
            countries_data_json = json.dumps(countries_data, default=str, ensure_ascii=False)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to serialize countries_data: {e}")
            countries_data_json = "{}"
    
    # Serialize osii_by_country to JSON for JavaScript
    osii_by_country_json = "{}"
    if osii_by_country:
        import json
        try:
            # Convert DataFrames to dict format
            osii_dict = {}
            for country, df in osii_by_country.items():
                osii_dict[country] = df.to_dict('records')
            osii_by_country_json = json.dumps(osii_dict, default=str, ensure_ascii=False)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to serialize osii_by_country: {e}")
            osii_by_country_json = "{}"
    
    html = env.get_template(template_name).render(
        generation_date=generation_date,
        analyses=analyses,
        plots_inline=plots_inline,
        plot_files=plot_files,
        download_links=download_links,
        table_files=table_files,
        news_feed_html=news_feed_html,
        bbm_ref_date=bbm_ref_date,
        ltv_ref_date=ltv_ref_date,
        countries_data_json=countries_data_json,
        knowledge_graph_json=knowledge_graph_json,
        osii_countries=osii_countries or [],
        osii_table_html=osii_table_html,
        all_sii_table_html=all_sii_table_html,
        osii_by_country_json=osii_by_country_json,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
    )
    return html

