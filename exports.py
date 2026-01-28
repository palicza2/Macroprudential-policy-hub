from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


def ensure_report_dirs(reports_dir: Path) -> Dict[str, Path]:
    partials_dir = reports_dir / "partials"
    plots_dir = reports_dir / "plots"
    downloads_dir = reports_dir / "downloads"
    partials_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)
    downloads_dir.mkdir(parents=True, exist_ok=True)
    return {"partials": partials_dir, "plots": plots_dir, "downloads": downloads_dir}


def rel_path(base_dir: Path, path: Path) -> str:
    try:
        return path.relative_to(base_dir).as_posix()
    except Exception:
        return path.as_posix()


def wrap_partial(body_html: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="../../assets/embed.css">
</head>
<body>
    <div class="embed-wrapper">
        {body_html or "<p class='no-data'>No Data</p>"}
    </div>
</body>
</html>"""


def write_partial(base_dir: Path, partials_dir: Path, name: str, html: str) -> str:
    path = partials_dir / f"{name}.html"
    path.write_text(wrap_partial(html or ""), encoding="utf-8")
    return rel_path(base_dir, path)


def write_plot_html(base_dir: Path, plots_dir: Path, name: str, fig: Any) -> str:
    if fig is None:
        return ""
    plot_html = fig.to_html(full_html=True, include_plotlyjs="cdn", config={"responsive": True})
    path = plots_dir / f"{name}.html"
    path.write_text(plot_html, encoding="utf-8")
    return rel_path(base_dir, path)


def write_download(base_dir: Path, downloads_dir: Path, name: str, df: Optional[pd.DataFrame]) -> str:
    if df is None or df.empty:
        return ""
    path = downloads_dir / f"{name}.xlsx"
    df.to_excel(path, index=False)
    return rel_path(base_dir, path)

