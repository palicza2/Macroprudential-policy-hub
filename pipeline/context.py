"""
Pipeline Context.
Shared state container for pipeline stages. Simplifies orchestration by centralizing
data flow instead of passing 15+ individual parameters between stages.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class PipelineContext:
    """
    Shared context for the macroprudential pipeline.
    Stages read from and the orchestrator writes to these fields.
    """

    # --- Data (from DataStage) ---
    data: Dict[str, Any] = field(default_factory=dict)

    # --- Visualization (from VisualizationStage) ---
    plots_inline: Dict[str, str] = field(default_factory=dict)
    plot_figs: Dict[str, Any] = field(default_factory=dict)
    download_data: Dict[str, Any] = field(default_factory=dict)
    paths: Dict[str, Any] = field(default_factory=dict)

    # --- BBM (from BBMStage) ---
    bbm_data: Dict[str, Any] = field(default_factory=dict)

    # --- AI (from AIStage) ---
    analyses: Dict[str, Any] = field(default_factory=dict)
    ccyb_decisions: Any = None
    active_syrb: Any = None
    syrb_decisions: Any = None

    # --- Profile (from ProfileStage) ---
    countries_data: Dict[str, Any] = field(default_factory=dict)

    # --- Misc ---
    knowledge_graph_json: str = '{"nodes": [], "edges": []}'
