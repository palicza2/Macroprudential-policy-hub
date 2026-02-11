"""
AI Analysis Stage.
Handles LLM analysis and grounded validation.
"""

import logging
import json
from typing import Dict, Any

from llm_analysis import LLMAnalyzer
from grounding_validator import GroundingValidator
from ccyb import prepare_ccyb_decisions
from syrb import prepare_syrb_tables

logger = logging.getLogger(__name__)


class AIStage:
    """Processes AI analysis and validation."""
    
    def __init__(self, llm_config: Dict[str, Any], search_config: Dict[str, Any], run_grounding: bool = False):
        """
        Initialize AI stage.
        
        Args:
            llm_config: LLM configuration
            search_config: Search configuration for grounding
            run_grounding: Whether to run grounded validation
        """
        self.llm_config = llm_config
        self.search_config = search_config
        self.run_grounding = run_grounding
        self.analyzer = LLMAnalyzer(llm_config)
    
    def process(
        self,
        ccyb_full,
        syrb_full,
        analysis_inputs: Dict[str, Any],
        paths: Dict[str, Any],
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Run AI analysis.
        
        Args:
            ccyb_full: Full CCyB dataframe
            syrb_full: Full SyRB dataframe
            analysis_inputs: Inputs for analysis
            paths: Plot paths
            data: Additional data dictionary
            
        Returns:
            Dictionary with 'analyses' and 'decisions' (ccyb_decisions, active_syrb, syrb_decisions)
        """
        logger.info("3. AI Elemzés...")
        
        ccyb_decisions = prepare_ccyb_decisions(ccyb_full, self.analyzer)
        active_syrb, syrb_decisions = prepare_syrb_tables(syrb_full, self.analyzer)
        
        # Update analysis_inputs with decisions
        analysis_inputs['ccyb_decisions_df'] = ccyb_decisions
        analysis_inputs['active_syrb_df'] = active_syrb
        analysis_inputs['syrb_decisions_df'] = syrb_decisions
        
        analyses = self.analyzer.run_analysis(analysis_inputs, paths, {})
        
        # Grounded validation against data, charts, and external sources
        if self.run_grounding:
            logger.info("3b. Grounded Validation...")
            validator = GroundingValidator(self.llm_config, self.search_config, self.analyzer._clean_text)
            analyses = validator.run(analyses, analysis_inputs, data)
        
        # Knowledge Graph AI Analysis - TEMPORARILY DISABLED
        analyses['knowledge_graph_analysis'] = "Knowledge graph analysis is temporarily disabled for performance optimization."
        logger.info("3e. Knowledge Graph AI Analysis... (DISABLED - skipped for performance)")
        
        return {
            'analyses': analyses,
            'ccyb_decisions': ccyb_decisions,
            'active_syrb': active_syrb,
            'syrb_decisions': syrb_decisions,
        }
