"""
Country Profile Generator
Generates country-specific macroprudential policy profiles.
"""
import pandas as pd
from typing import Dict, List, Optional, Any
import logging

from .region_mapper import get_iso2, get_region
from .data_aggregators import (
    get_current_status,
    get_historical_evolution,
    get_recent_changes,
    get_active_measures,
    get_comparison,
    get_institutional_setup,
)
from knowledge_graph import build_knowledge_graph_data

logger = logging.getLogger(__name__)


class CountryProfileGenerator:
    """
    Országprofil generálása adatokból.
    """
    
    def __init__(self, data: Dict[str, pd.DataFrame]):
        """
        Args:
            data: Dictionary a következő kulcsokkal:
                - ccyb_df: CCyB adatok
                - syrb_df: SyRB adatok
                - bbm_df: BBM adatok
                - osii_df: O-SII adatok
                - capital_overall_df: Capital overall adatok
        """
        self.data = data
        self.countries = self._get_available_countries()
    
    def _get_available_countries(self) -> List[str]:
        """Elérhető országok listája."""
        countries = set()
        
        for df_name, df in self.data.items():
            if df is not None and not df.empty:
                if 'country' in df.columns:
                    country_values = df['country'].dropna().unique()
                    countries.update([str(c) for c in country_values if str(c).strip()])
                elif 'COUNTRY' in df.columns:
                    country_values = df['COUNTRY'].dropna().unique()
                    countries.update([str(c) for c in country_values if str(c).strip()])
        
        return sorted(list(countries))
    
    def get_country_profile(self, country: str) -> Dict[str, Any]:
        """
        Országprofil generálása.
        
        Args:
            country: Ország neve (pl. "Hungary")
        
        Returns:
            Dictionary a profil adataival
        """
        iso2 = get_iso2(country)
        profile = {
            'country': country,
            'iso2': iso2,
            'institutional_setup': get_institutional_setup(country, self.data, iso2=iso2),
            'current_status': get_current_status(country, self.data),
            'historical_evolution': get_historical_evolution(country, self.data),
            'recent_changes': get_recent_changes(country, self.data),
            'active_measures': get_active_measures(country, self.data),
            'comparison': get_comparison(country, self.data),
        }
        
        return profile
    
