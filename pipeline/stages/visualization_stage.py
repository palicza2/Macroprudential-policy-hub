"""
Visualization Stage.
Handles chart and plot generation.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple

from visualizer import Visualizer

logger = logging.getLogger(__name__)


class VisualizationStage:
    """Processes visualization generation."""
    
    def __init__(self, figures_dir: Path):
        """
        Initialize visualization stage.
        
        Args:
            figures_dir: Directory for figure outputs
        """
        self.figures_dir = figures_dir
    
    def process(self, data: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """
        Generate all plots.
        
        Args:
            data: Processed data dictionary
            
        Returns:
            Tuple of (plots_inline, plot_figs, download_data, paths)
        """
        logger.info("2. Grafikonok...")
        viz = Visualizer(self.figures_dir)
        today_str = datetime.now().strftime("%Y-%m-%d")
        plots_inline, plot_figs, download_data, paths = viz.generate_all_plots(data, today_str)
        
        return plots_inline, plot_figs, download_data, paths
