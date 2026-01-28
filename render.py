from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from exports import ensure_report_dirs, write_download, write_partial, write_plot_html


def df_to_html_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "<p class='no-data'>No Data</p>"
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
) -> str:
    dirs = ensure_report_dirs(reports_dir)
    partials_dir = dirs["partials"]
    plots_dir = dirs["plots"]
    downloads_dir = dirs["downloads"]

    table_files = {k: write_partial(base_dir, partials_dir, k, v) for k, v in tables_html.items()}
    plot_files = {k: write_plot_html(base_dir, plots_dir, k, v) for k, v in plot_figs.items()}
    download_links = {k: write_download(base_dir, downloads_dir, k, v) for k, v in download_data.items()}

    env = Environment(loader=FileSystemLoader(str(template_dir)))
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
    )
    return html

