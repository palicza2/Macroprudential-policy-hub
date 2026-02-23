"""
Pipeline package for stage-based processing.
"""

from .context import PipelineContext
from .orchestrator import PipelineOrchestrator

__all__ = ['PipelineContext', 'PipelineOrchestrator']
