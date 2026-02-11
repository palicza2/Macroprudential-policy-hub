"""
Country Profile Stage.
Handles country profile generation and AI analysis.
"""

import logging
from typing import Dict, Any

from country_profiles import CountryProfileGenerator
from pipeline.serializers import serialize_profile, format_profile_for_llm

logger = logging.getLogger(__name__)


class ProfileStage:
    """Processes country profile generation."""
    
    def __init__(self, analyzer):
        """
        Initialize profile stage.
        
        Args:
            analyzer: LLMAnalyzer instance for AI analysis
        """
        self.analyzer = analyzer
    
    def process(self, data: Dict[str, Any], analyses: Dict[str, str]) -> Dict[str, Any]:
        """
        Generate country profiles and AI analysis.
        
        Args:
            data: Processed data dictionary
            analyses: Existing analyses dictionary
            
        Returns:
            Dictionary with countries_data and updated analyses
        """
        logger.info("3c. Country Profiles...")
        profile_gen = CountryProfileGenerator({
            'ccyb_df': data.get('ccyb_df'),
            'syrb_df': data.get('syrb_df'),
            'bbm_df': data.get('bbm_df'),
            'osii_df': data.get('osii_df'),
            'capital_overall_df': data.get('capital_overall_df'),
        })
        
        logger.info(f"   -> Found {len(profile_gen.countries)} countries")
        countries_data = {}
        
        for country in profile_gen.countries:
            try:
                profile = profile_gen.get_country_profile(country)
                # Convert dates to strings for JSON serialization
                profile_serializable = serialize_profile(profile)
                countries_data[country] = profile_serializable
            except Exception as e:
                logger.warning(f"Failed to generate profile for {country}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
        
        logger.info(f"   -> Generated {len(countries_data)} country profiles")
        
        # Generate AI analysis for each country profile
        for country, profile_data in countries_data.items():
            try:
                analysis_key = f"country_profile_{country.lower().replace(' ', '_')}"
                if analysis_key not in analyses:
                    # Generate AI analysis for country profile
                    profile_text = format_profile_for_llm(profile_data)
                    country_analysis = self.analyzer.summarize_text(
                        profile_text,
                        f"Provide a comprehensive 3-4 paragraph analysis of {country}'s macroprudential policy profile, focusing on current policy stance, recent trends, policy objectives, and comparison with regional context."
                    )
                    analyses[analysis_key] = country_analysis
                    profile_data['ai_analysis'] = country_analysis
            except Exception as e:
                logger.warning(f"Failed to generate AI analysis for {country}: {e}")
                profile_data['ai_analysis'] = ''
        
        return {
            'countries_data': countries_data,
            'analyses': analyses,
        }
