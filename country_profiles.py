"""
Country Profile Generator
Generates country-specific macroprudential policy profiles.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

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
        profile = {
            'country': country,
            'iso2': self._get_iso2(country),
            'current_status': self._get_current_status(country),
            'historical_evolution': self._get_historical_evolution(country),
            'recent_changes': self._get_recent_changes(country),
            'active_measures': self._get_active_measures(country),
            'comparison': self._get_comparison(country),
        }
        
        return profile
    
    def _get_current_status(self, country: str) -> Dict[str, Any]:
        """Aktuális állapot snapshot."""
        status = {
            'ccyb': None,
            'syrb': None,
            'osii': None,
            'bbm': [],
            'total_capital': None,
        }
        
        # CCyB
        ccyb_df = self.data.get('ccyb_df')
        if ccyb_df is not None and not ccyb_df.empty:
            country_ccyb = ccyb_df[ccyb_df['country'] == country]
            if not country_ccyb.empty:
                latest = country_ccyb.sort_values('date').iloc[-1]
                status['ccyb'] = {
                    'rate': float(latest.get('rate', 0)) if pd.notna(latest.get('rate')) else 0.0,
                    'date': latest.get('date'),
                    'status': 'Active' if latest.get('rate', 0) > 0 else 'Inactive',
                }
        
        # SyRB
        syrb_df = self.data.get('syrb_df')
        if syrb_df is not None and not syrb_df.empty:
            country_syrb = syrb_df[syrb_df['country'] == country]
            if not country_syrb.empty:
                # Legfrissebb aktív SyRB
                active = country_syrb[
                    (country_syrb.get('active_status', '') == 'Active') |
                    (country_syrb.get('status', '').astype(str).str.contains('Active', case=False, na=False))
                ]
                if not active.empty:
                    latest = active.sort_values('date').iloc[-1]
                    status['syrb'] = {
                        'rate': float(latest.get('rate_numeric', 0)) if pd.notna(latest.get('rate_numeric')) else 0.0,
                        'date': latest.get('date'),
                        'type': latest.get('measure_type', 'General'),
                        'status': 'Active',
                    }
        
        # O-SII
        osii_df = self.data.get('osii_df')
        if osii_df is not None and not osii_df.empty:
            country_osii = osii_df[osii_df['country'] == country]
            if not country_osii.empty:
                latest = country_osii.sort_values('date').iloc[-1] if 'date' in country_osii.columns else country_osii.iloc[-1]
                status['osii'] = {
                    'rate': float(latest.get('rate_numeric', 0)) if pd.notna(latest.get('rate_numeric')) else 0.0,
                    'status': 'Active' if latest.get('rate_numeric', 0) > 0 else 'Inactive',
                }
        
        # BBM
        bbm_df = self.data.get('bbm_df')
        if bbm_df is not None and not bbm_df.empty:
            country_bbm = bbm_df[
                (bbm_df['country'] == country) &
                (bbm_df.get('active_status', '') == 'Active')
            ]
            if not country_bbm.empty:
                status['bbm'] = country_bbm['measure_type'].unique().tolist()
        
        # Total Capital (Capital Overall)
        capital_df = self.data.get('capital_overall_df')
        if capital_df is not None and not capital_df.empty:
            country_col = 'COUNTRY' if 'COUNTRY' in capital_df.columns else 'country'
            if country_col in capital_df.columns:
                country_capital = capital_df[capital_df[country_col] == country]
                if not country_capital.empty:
                    row = country_capital.iloc[0]
                    status['total_capital'] = {
                        'total': float(row.get('Total', 0)) if pd.notna(row.get('Total')) else 0.0,
                        'ccob': float(row.get('CCoB', 2.5)) if pd.notna(row.get('CCoB')) else 2.5,
                        'ccyb': float(row.get('CCyB', 0)) if pd.notna(row.get('CCyB')) else 0.0,
                        'osii': float(row.get('GSII/O-SII', 0)) if pd.notna(row.get('GSII/O-SII')) else 0.0,
                        'syrb': float(row.get('SyRB', 0)) if pd.notna(row.get('SyRB')) else 0.0,
                        'ssyrb': float(row.get('sSyRB', 0)) if pd.notna(row.get('sSyRB')) else 0.0,
                    }
        
        return status
    
    def _get_historical_evolution(self, country: str) -> Dict[str, pd.DataFrame]:
        """Időbeli változások."""
        evolution = {}
        
        # CCyB trend
        ccyb_df = self.data.get('ccyb_df')
        if ccyb_df is not None and not ccyb_df.empty:
            country_ccyb = ccyb_df[ccyb_df['country'] == country].sort_values('date')
            if not country_ccyb.empty:
                cols = ['date', 'rate']
                if 'credit_gap' in country_ccyb.columns:
                    cols.append('credit_gap')
                evolution['ccyb'] = country_ccyb[cols].copy()
        
        # SyRB trend
        syrb_df = self.data.get('syrb_df')
        if syrb_df is not None and not syrb_df.empty:
            country_syrb = syrb_df[syrb_df['country'] == country].sort_values('date')
            if not country_syrb.empty:
                cols = ['date']
                if 'rate_numeric' in country_syrb.columns:
                    cols.append('rate_numeric')
                if 'measure_type' in country_syrb.columns:
                    cols.append('measure_type')
                evolution['syrb'] = country_syrb[cols].copy()
        
        return evolution
    
    def _get_recent_changes(self, country: str, months: int = 12) -> List[Dict[str, Any]]:
        """Legutóbbi változások."""
        changes = []
        cutoff_date = datetime.now() - timedelta(days=months * 30)
        
        # CCyB változások
        ccyb_df = self.data.get('ccyb_df')
        if ccyb_df is not None and not ccyb_df.empty:
            country_ccyb = ccyb_df[
                (ccyb_df['country'] == country) &
                (pd.to_datetime(ccyb_df['date'], errors='coerce') >= cutoff_date)
            ].sort_values('date')
            
            if len(country_ccyb) > 1:
                for i in range(1, len(country_ccyb)):
                    prev = country_ccyb.iloc[i-1]
                    curr = country_ccyb.iloc[i]
                    
                    prev_rate = float(prev.get('rate', 0)) if pd.notna(prev.get('rate')) else 0.0
                    curr_rate = float(curr.get('rate', 0)) if pd.notna(curr.get('rate')) else 0.0
                    
                    if prev_rate != curr_rate:
                        changes.append({
                            'date': curr.get('date'),
                            'type': 'CCyB',
                            'change': f"{prev_rate:.2f}% → {curr_rate:.2f}%",
                            'direction': 'increase' if curr_rate > prev_rate else 'decrease',
                        })
        
        # SyRB változások
        syrb_df = self.data.get('syrb_df')
        if syrb_df is not None and not syrb_df.empty:
            country_syrb = syrb_df[
                (syrb_df['country'] == country) &
                (pd.to_datetime(syrb_df['date'], errors='coerce') >= cutoff_date)
            ].sort_values('date')
            
            # Új aktíválások
            active = country_syrb[
                (country_syrb.get('active_status', '') == 'Active') |
                (country_syrb.get('status', '').astype(str).str.contains('Active', case=False, na=False))
            ]
            
            for _, row in active.iterrows():
                rate = float(row.get('rate_numeric', 0)) if pd.notna(row.get('rate_numeric')) else 0.0
                changes.append({
                    'date': row.get('date'),
                    'type': 'SyRB',
                    'change': f"Activated: {rate:.2f}%",
                    'direction': 'activation',
                })
        
        # BBM változások
        bbm_df = self.data.get('bbm_df')
        if bbm_df is not None and not bbm_df.empty:
            country_bbm = bbm_df[
                (bbm_df['country'] == country) &
                (pd.to_datetime(bbm_df['date'], errors='coerce') >= cutoff_date)
            ].sort_values('date')
            
            for _, row in country_bbm.iterrows():
                changes.append({
                    'date': row.get('date'),
                    'type': 'BBM',
                    'change': f"{row.get('measure_type', 'BBM')} - {row.get('status', '')}",
                    'direction': 'change',
                })
        
        # Dátum szerint rendezés (legfrissebb először)
        changes.sort(key=lambda x: x['date'] if pd.notna(x['date']) else datetime.min, reverse=True)
        
        return changes[:10]  # Legutóbbi 10 változás
    
    def _get_active_measures(self, country: str) -> Dict[str, Any]:
        """Aktív intézkedések részletei."""
        measures = {
            'ccyb': None,
            'syrb': [],
            'bbm': [],
            'osii': None,
        }
        
        # CCyB részletek
        ccyb_df = self.data.get('ccyb_df')
        if ccyb_df is not None and not ccyb_df.empty:
            country_ccyb = ccyb_df[ccyb_df['country'] == country].sort_values('date')
            if not country_ccyb.empty:
                latest = country_ccyb.iloc[-1]
                measures['ccyb'] = {
                    'rate': float(latest.get('rate', 0)) if pd.notna(latest.get('rate')) else 0.0,
                    'date': latest.get('date'),
                    'justification': latest.get('justification', ''),
                    'credit_gap': float(latest.get('credit_gap', 0)) if pd.notna(latest.get('credit_gap')) else None,
                }
        
        # SyRB részletek
        syrb_df = self.data.get('syrb_df')
        if syrb_df is not None and not syrb_df.empty:
            country_syrb = syrb_df[
                (syrb_df['country'] == country) &
                ((syrb_df.get('active_status', '') == 'Active') |
                 (syrb_df.get('status', '').astype(str).str.contains('Active', case=False, na=False)))
            ]
            
            for _, row in country_syrb.iterrows():
                measures['syrb'].append({
                    'rate': float(row.get('rate_numeric', 0)) if pd.notna(row.get('rate_numeric')) else 0.0,
                    'type': row.get('measure_type', 'General'),
                    'exposure': row.get('exposure_type', ''),
                    'date': row.get('date'),
                    'description': row.get('description', ''),
                })
        
        # BBM részletek
        bbm_df = self.data.get('bbm_df')
        if bbm_df is not None and not bbm_df.empty:
            country_bbm = bbm_df[
                (bbm_df['country'] == country) &
                (bbm_df.get('active_status', '') == 'Active')
            ]
            
            for _, row in country_bbm.iterrows():
                measures['bbm'].append({
                    'type': row.get('measure_type', ''),
                    'status': row.get('status', ''),
                    'date': row.get('date'),
                    'description': row.get('description', ''),
                })
        
        return measures
    
    def _get_comparison(self, country: str) -> Dict[str, Any]:
        """Összehasonlítás más országokkal."""
        comparison = {
            'regional_average': None,
            'similar_countries': [],
        }
        
        # Similar countries (hasonló tőkepuffer szinttel)
        capital_df = self.data.get('capital_overall_df')
        if capital_df is not None and not capital_df.empty:
            country_col = 'COUNTRY' if 'COUNTRY' in capital_df.columns else 'country'
            if country_col in capital_df.columns:
                country_total = capital_df[capital_df[country_col] == country]
                if not country_total.empty:
                    country_total_val = float(country_total.iloc[0].get('Total', 0)) if pd.notna(country_total.iloc[0].get('Total')) else 0.0
                    
                    # Hasonló országok (±0.5% tűrés)
                    similar = capital_df[
                        (capital_df['Total'].apply(lambda x: abs(float(x) - country_total_val) if pd.notna(x) else 999) <= 0.5) &
                        (capital_df[country_col] != country)
                    ].head(5)
                    
                    if not similar.empty:
                        comparison['similar_countries'] = similar[[country_col, 'Total']].to_dict('records')
        
        return comparison
    
    def _get_iso2(self, country: str) -> Optional[str]:
        """ISO2 kód lekérése."""
        try:
            import country_converter as coco
            iso2 = coco.convert(names=country, to='iso2', not_found=None)
            return iso2
        except Exception:
            return None
    
    def build_knowledge_graph_data(
        self,
        countries: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Knowledge graph adatok generálása.
        
        Args:
            countries: Opcionális országlista. Ha None, akkor minden ország.
        
        Returns:
            {
                'nodes': [
                    {'id': 'HU', 'label': 'Hungary', 'group': 'country', ...},
                    {'id': 'CCyB_HU', 'label': 'CCyB: 2.5%', 'group': 'ccyb', ...},
                ],
                'edges': [
                    {'from': 'HU', 'to': 'CCyB_HU', 'label': 'HAS', ...},
                ]
            }
        """
        nodes = []
        edges = []
        node_ids = set()  # Deduplikációhoz
        
        # Országok
        target_countries = countries or self.countries
        for country in target_countries:
            try:
                profile = self.get_country_profile(country)
                if not profile:
                    continue
                    
                iso2 = profile.get('iso2') or self._get_iso2(country) or country[:2].upper()
                
                # Ország node
                current_status = profile.get('current_status') or {}
                total_capital_obj = current_status.get('total_capital') or {}
                total_capital = total_capital_obj.get('total', 0) if total_capital_obj else 0
                if not total_capital:
                    total_capital = 0
                nodes.append({
                    'id': iso2,
                    'label': country,
                    'group': 'country',
                    'title': f"{country} - Total Capital: {total_capital:.2f}%",
                    'value': float(total_capital) if total_capital else 10,
                    'region': self._get_region(country),
                })
                node_ids.add(iso2)
                
                # CCyB kapcsolat
                ccyb = current_status.get('ccyb')
                if ccyb and ccyb.get('rate', 0) > 0:
                    node_id = f"CCyB_{iso2}"
                    if node_id not in node_ids:
                        nodes.append({
                            'id': node_id,
                            'label': f"CCyB: {ccyb['rate']:.2f}%",
                            'group': 'ccyb',
                            'title': f"CCyB rate: {ccyb['rate']:.2f}% (Effective: {ccyb.get('date', 'N/A')})",
                            'value': float(ccyb['rate']) if ccyb.get('rate') else 5,
                        })
                        node_ids.add(node_id)
                    
                    edges.append({
                        'from': iso2,
                        'to': node_id,
                        'label': 'HAS',
                        'title': 'Has active CCyB',
                        'color': {'color': '#64748b'},
                        'width': 2 + (float(ccyb['rate']) / 2),  # Vastagság a ráta szerint
                    })
                
                # SyRB kapcsolat
                syrb = current_status.get('syrb')
                if syrb and syrb.get('rate', 0) > 0:
                    node_id = f"SyRB_{iso2}"
                    if node_id not in node_ids:
                        nodes.append({
                            'id': node_id,
                            'label': f"SyRB: {syrb['rate']:.2f}%",
                            'group': 'syrb',
                            'title': f"SyRB rate: {syrb['rate']:.2f}% - {syrb.get('type', 'General')}",
                            'value': float(syrb['rate']) if syrb.get('rate') else 5,
                        })
                        node_ids.add(node_id)
                    
                    edges.append({
                        'from': iso2,
                        'to': node_id,
                        'label': 'HAS',
                        'title': 'Has active SyRB',
                        'color': {'color': '#64748b'},
                        'width': 2 + (float(syrb['rate']) / 2),
                    })
                
                # O-SII kapcsolat
                osii = current_status.get('osii')
                if osii and osii.get('rate', 0) > 0:
                    node_id = f"O-SII_{iso2}"
                    if node_id not in node_ids:
                        nodes.append({
                            'id': node_id,
                            'label': f"O-SII: {osii['rate']:.2f}%",
                            'group': 'osii',
                            'title': f"O-SII rate: {osii['rate']:.2f}%",
                            'value': float(osii['rate']) if osii.get('rate') else 5,
                        })
                        node_ids.add(node_id)
                    
                    edges.append({
                        'from': iso2,
                        'to': node_id,
                        'label': 'HAS',
                        'title': 'Has active O-SII buffer',
                        'color': {'color': '#64748b'},
                        'width': 2 + (float(osii['rate']) / 2),
                    })
                
                # BBM kapcsolat
                bbm = current_status.get('bbm', [])
                if bbm:
                    for measure_type in bbm:
                        node_id = f"BBM_{iso2}_{measure_type}"
                        if node_id not in node_ids:
                            nodes.append({
                                'id': node_id,
                                'label': f"BBM: {measure_type}",
                                'group': 'bbm',
                                'title': f"Borrower-based measure: {measure_type}",
                                'value': 5,
                            })
                            node_ids.add(node_id)
                        
                        edges.append({
                            'from': iso2,
                            'to': node_id,
                            'label': 'HAS',
                            'title': f'Has active {measure_type}',
                            'color': {'color': '#64748b'},
                            'width': 2,
                        })
                
                # Hasonló országok kapcsolatai
                comparison = profile.get('comparison', {})
                similar = comparison.get('similar_countries', [])
                
                for similar_country in similar:
                    similar_name = similar_country.get('COUNTRY') or similar_country.get('country', '')
                    if similar_name:
                        similar_iso2 = self._get_iso2(similar_name) or similar_name[:2].upper()
                        if similar_iso2 and similar_iso2 != iso2 and similar_iso2 in [n['id'] for n in nodes if n.get('group') == 'country']:
                            # Ellenőrizzük, hogy nincs-e már ilyen edge (kétirányú)
                            edge_exists = any(
                                (e.get('from') == iso2 and e.get('to') == similar_iso2) or
                                (e.get('from') == similar_iso2 and e.get('to') == iso2)
                                for e in edges if e.get('label') == 'SIMILAR'
                            )
                            
                            if not edge_exists:
                                edges.append({
                                    'from': iso2,
                                    'to': similar_iso2,
                                    'label': 'SIMILAR',
                                    'title': f'Similar capital buffer level: {similar_country.get("Total", 0):.2f}%',
                                    'color': {'color': '#3b82f6'},
                                    'dashes': True,
                                    'width': 2,
                                })
            except Exception as e:
                logger.warning(f"Failed to build graph data for {country}: {e}")
                continue
        
        # Hasonló intézkedések összekapcsolása - CSÖKKENTETT: csak hasonló értékeket kapcsolunk össze
        # CCyB intézkedések összekapcsolása (csak ha a ráta különbség < 0.5%)
        ccyb_nodes = [n for n in nodes if n.get('group') == 'ccyb']
        for i, node1 in enumerate(ccyb_nodes):
            rate1 = node1.get('value', 0)
            for node2 in ccyb_nodes[i+1:]:
                rate2 = node2.get('value', 0)
                # Csak akkor kapcsoljuk össze, ha a ráta különbség < 0.5%
                if abs(rate1 - rate2) < 0.5:
                    edge_exists = any(
                        (e.get('from') == node1['id'] and e.get('to') == node2['id']) or
                        (e.get('from') == node2['id'] and e.get('to') == node1['id'])
                        for e in edges if e.get('label') == 'SIMILAR_MEASURE'
                    )
                    if not edge_exists:
                        edges.append({
                            'from': node1['id'],
                            'to': node2['id'],
                            'label': 'SIMILAR_MEASURE',
                            'title': f'Similar CCyB measures ({rate1:.2f}% vs {rate2:.2f}%)',
                            'color': {'color': '#10b981'},
                            'dashes': [5, 5],
                            'width': 1,
                        })
        
        # SyRB intézkedések összekapcsolása (csak ha a ráta különbség < 1.0%)
        syrb_nodes = [n for n in nodes if n.get('group') == 'syrb']
        for i, node1 in enumerate(syrb_nodes):
            rate1 = node1.get('value', 0)
            for node2 in syrb_nodes[i+1:]:
                rate2 = node2.get('value', 0)
                if abs(rate1 - rate2) < 1.0:
                    edge_exists = any(
                        (e.get('from') == node1['id'] and e.get('to') == node2['id']) or
                        (e.get('from') == node2['id'] and e.get('to') == node1['id'])
                        for e in edges if e.get('label') == 'SIMILAR_MEASURE'
                    )
                    if not edge_exists:
                        edges.append({
                            'from': node1['id'],
                            'to': node2['id'],
                            'label': 'SIMILAR_MEASURE',
                            'title': f'Similar SyRB measures ({rate1:.2f}% vs {rate2:.2f}%)',
                            'color': {'color': '#10b981'},
                            'dashes': [5, 5],
                            'width': 1,
                        })
        
        # O-SII intézkedések összekapcsolása (csak ha a ráta különbség < 0.5%)
        osii_nodes = [n for n in nodes if n.get('group') == 'osii']
        for i, node1 in enumerate(osii_nodes):
            rate1 = node1.get('value', 0)
            for node2 in osii_nodes[i+1:]:
                rate2 = node2.get('value', 0)
                if abs(rate1 - rate2) < 0.5:
                    edge_exists = any(
                        (e.get('from') == node1['id'] and e.get('to') == node2['id']) or
                        (e.get('from') == node2['id'] and e.get('to') == node1['id'])
                        for e in edges if e.get('label') == 'SIMILAR_MEASURE'
                    )
                    if not edge_exists:
                        edges.append({
                            'from': node1['id'],
                            'to': node2['id'],
                            'label': 'SIMILAR_MEASURE',
                            'title': f'Similar O-SII measures ({rate1:.2f}% vs {rate2:.2f}%)',
                            'color': {'color': '#10b981'},
                            'dashes': [5, 5],
                            'width': 1,
                        })
        
        # BBM intézkedések összekapcsolása (azonos típusúak)
        bbm_nodes = [n for n in nodes if n.get('group') == 'bbm']
        # Csoportosítás measure type szerint
        bbm_by_type = {}
        for node in bbm_nodes:
            # Extract measure type from node ID (BBM_ISO2_MeasureType)
            parts = node['id'].split('_')
            if len(parts) >= 3:
                measure_type = '_'.join(parts[2:])  # Handle multi-word measure types
                if measure_type not in bbm_by_type:
                    bbm_by_type[measure_type] = []
                bbm_by_type[measure_type].append(node)
        
        # Összekapcsoljuk az azonos típusú BBM-eket
        for measure_type, type_nodes in bbm_by_type.items():
            for i, node1 in enumerate(type_nodes):
                for node2 in type_nodes[i+1:]:
                    edge_exists = any(
                        (e.get('from') == node1['id'] and e.get('to') == node2['id']) or
                        (e.get('from') == node2['id'] and e.get('to') == node1['id'])
                        for e in edges if e.get('label') == 'SIMILAR_MEASURE'
                    )
                    if not edge_exists:
                        edges.append({
                            'from': node1['id'],
                            'to': node2['id'],
                            'label': 'SIMILAR_MEASURE',
                            'title': f'Similar {measure_type} measures',
                            'color': {'color': '#10b981'},
                            'dashes': [5, 5],
                            'width': 1,
                        })
        
        # Régió alapú kapcsolatok ELTÁVOLÍTVA - túlzsúfolt volt
        # A SIMILAR kapcsolatok már elég információt adnak
        
        # Intézkedések közötti kapcsolatok (ha egy országnak van több intézkedése)
        # Csoportosítás ország szerint
        measures_by_country = {}
        for node in nodes:
            if node.get('group') != 'country':
                # Extract country ISO2 from node ID (e.g., CCyB_HU -> HU)
                node_id = node['id']
                if '_' in node_id:
                    parts = node_id.split('_')
                    if len(parts) >= 2:
                        # Handle both CCyB_HU and BBM_HU_MeasureType formats
                        country_iso = parts[1] if len(parts) == 2 else parts[1]
                        if country_iso not in measures_by_country:
                            measures_by_country[country_iso] = []
                        measures_by_country[country_iso].append(node)
        
        # Összekapcsoljuk az egy országhoz tartozó intézkedéseket
        for country_iso, country_measures in measures_by_country.items():
            if len(country_measures) > 1:
                for i, measure1 in enumerate(country_measures):
                    for measure2 in country_measures[i+1:]:
                        edge_exists = any(
                            (e.get('from') == measure1['id'] and e.get('to') == measure2['id']) or
                            (e.get('from') == measure2['id'] and e.get('to') == measure1['id'])
                            for e in edges if e.get('label') == 'COEXISTS'
                        )
                        if not edge_exists:
                            edges.append({
                                'from': measure1['id'],
                                'to': measure2['id'],
                                'label': 'COEXISTS',
                                'title': f'Coexists in same country',
                                'color': {'color': '#f59e0b'},
                                'dashes': [2, 2],
                                'width': 1.5,
                            })
        
        return {
            'nodes': nodes,
            'edges': edges,
        }
    
    def _get_region(self, country: str) -> str:
        """Régió meghatározása."""
        cee = ['Hungary', 'Poland', 'Czech Republic', 'Slovakia', 'Slovenia', 'Croatia', 'Romania', 'Bulgaria', 'Estonia', 'Latvia', 'Lithuania']
        nordics = ['Sweden', 'Norway', 'Denmark', 'Finland', 'Iceland']
        western = ['Germany', 'France', 'Netherlands', 'Belgium', 'Austria', 'Luxembourg', 'Ireland']
        southern = ['Spain', 'Italy', 'Portugal', 'Greece', 'Cyprus', 'Malta']
        
        if country in cee:
            return 'CEE'
        elif country in nordics:
            return 'Nordics'
        elif country in western:
            return 'Western'
        elif country in southern:
            return 'Southern'
        else:
            return 'Other'
