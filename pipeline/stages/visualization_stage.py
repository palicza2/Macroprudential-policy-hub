"""
Visualization Stage.
Handles chart and plot generation.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

from visualizer import Visualizer
from pipeline.manifest import load_json, save_json, PLOT_KEYS

logger = logging.getLogger(__name__)


class VisualizationStage:
    """Processes visualization generation."""

    def __init__(self, figures_dir: Path, cache_path: Path = None):
        self.figures_dir = figures_dir
        self.cache_path = cache_path

    def process(
        self,
        data: Dict[str, Any],
        skip: bool = False,
    ) -> Tuple[Dict[str, str], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        if skip:
            logger.info("2. Grafikonok... (skipped; reusing plots)")
            cached = load_json(self.cache_path, default={}) or {} if self.cache_path else {}
            plots_inline = cached.get("plots_inline") or {}
            download_data: Dict[str, Any] = {}
            paths = {}
            if self.figures_dir.exists():
                for png in self.figures_dir.glob("*.png"):
                    paths[png.stem] = png
            plot_figs = {key: None for key in PLOT_KEYS}
            return plots_inline, plot_figs, download_data, paths

        logger.info("2. Grafikonok...")
        viz = Visualizer(self.figures_dir)
        today_str = datetime.now().strftime("%Y-%m-%d")
        plots_inline, plot_figs, download_data, paths = viz.generate_all_plots(data, today_str)
        if self.cache_path:
            save_json(self.cache_path, {"plots_inline": plots_inline})
        return plots_inline, plot_figs, download_data, paths
