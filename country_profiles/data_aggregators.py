"""
Data aggregation functions for country profiles.
"""
import pandas as pd
from typing import Dict, List, Any
from datetime import datetime, timedelta


def get_current_status(country: str, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """Aktuális állapot snapshot."""
    status = {
        'ccyb': None,
        'syrb': None,
        'osii': None,
        'bbm': [],
        'total_capital': None,
    }
    
    # CCyB
    ccyb_df = data.get('ccyb_df')
    if ccyb_df is not None and not ccyb_df.empty:
        country_ccyb = ccyb_df[ccyb_df['country'] == country]
        if not country_ccyb.empty:
            latest = country_ccyb.sort_values('date').iloc[-1]
            status['ccyb'] = {
                'rate': float(latest.get('rate', 0)) if pd.notna(latest.get('rate')) else 0.0,
                'date': latest.get('date'),
                'status': 'Active' if latest.get('rate', 0) > 0 else 'Inactive',
            }
    
    # SyRB - General és Sectoral (sSyRB) együtt
    syrb_df = data.get('syrb_df')
    if syrb_df is not None and not syrb_df.empty:
        country_syrb = syrb_df[syrb_df['country'] == country]
        if not country_syrb.empty:
            # Aktív SyRB-k szűrése
            active = country_syrb[
                (country_syrb.get('active_status', '') == 'Active') |
                (country_syrb.get('status', '').astype(str).str.contains('Active', case=False, na=False))
            ]
            if not active.empty:
                # Szűrjük az ésszerű értékeket (0-10% között)
                active = active.copy()
                active['rate_numeric'] = pd.to_numeric(active.get('rate_numeric', 0), errors='coerce').fillna(0.0)
                active = active[(active['rate_numeric'] > 0) & (active['rate_numeric'] <= 10.0)]
                
                if not active.empty:
                    # General SyRB (legfrissebb)
                    general = active[
                        (active.get('syrb_type', '').astype(str).str.contains('General', case=False, na=False)) |
                        (active.get('exposure_type', '').astype(str).str.contains('General', case=False, na=False))
                    ]
                    general_rate = 0.0
                    general_date = None
                    if not general.empty:
                        latest_general = general.sort_values('date').iloc[-1]
                        general_rate = float(latest_general.get('rate_numeric', 0))
                        if general_rate > 10.0:
                            general_rate = 0.0
                        general_date = latest_general.get('date')
                    
                    # Sectoral SyRB-k (sSyRB) - lista
                    sectoral = active[
                        (active.get('syrb_type', '').astype(str).str.contains('Sectoral', case=False, na=False)) |
                        (~active.get('exposure_type', '').astype(str).str.contains('General', case=False, na=False) &
                         active.get('exposure_type', '').astype(str).str.len() > 0)
                    ]
                    ssyrb_list = []
                    if not sectoral.empty:
                        # Csoportosítás exposure_type szerint, legfrissebb dátum szerint
                        for exposure_type, group in sectoral.sort_values('date').groupby('exposure_type'):
                            latest_sectoral = group.iloc[-1]
                            rate = float(latest_sectoral.get('rate_numeric', 0))
                            if 0 < rate <= 10.0:
                                ssyrb_list.append({
                                    'exposure': exposure_type or 'Sectoral',
                                    'rate': rate,
                                    'date': latest_sectoral.get('date'),
                                })
                    
                    status['syrb'] = {
                        'rate': general_rate,
                        'date': general_date,
                        'type': 'General',
                        'status': 'Active' if general_rate > 0 else 'Inactive',
                        'ssyrb': ssyrb_list,  # Sectoral SyRB lista
                    }
    
    # O-SII - min-max intervallum számítása
    osii_df = data.get('osii_df')
    if osii_df is not None and not osii_df.empty:
        country_osii = osii_df[osii_df['country'] == country]
        if not country_osii.empty:
            # Szűrjük az aktív O-SII-ket
            active_osii = country_osii[
                (country_osii.get('active_status', '') == 'Active') |
                (country_osii.get('status', '').astype(str).str.contains('Active', case=False, na=False))
            ]
            if active_osii.empty:
                active_osii = country_osii  # Ha nincs explicit status, akkor mindet nézzük
            
            # Számítsuk a min-max intervallumot
            active_osii = active_osii.copy()
            active_osii['rate_numeric'] = pd.to_numeric(active_osii.get('rate_numeric', 0), errors='coerce').fillna(0.0)
            active_osii = active_osii[active_osii['rate_numeric'] > 0]
            
            if not active_osii.empty:
                min_rate = float(active_osii['rate_numeric'].min())
                max_rate = float(active_osii['rate_numeric'].max())
                
                status['osii'] = {
                    'rate_min': min_rate,
                    'rate_max': max_rate,
                    'rate': max_rate,  # Backward compatibility - a max értéket használjuk
                    'count': len(active_osii),
                    'status': 'Active',
                }
            else:
                status['osii'] = {
                    'rate_min': 0.0,
                    'rate_max': 0.0,
                    'rate': 0.0,
                    'count': 0,
                    'status': 'Inactive',
                }
    
    # BBM
    bbm_df = data.get('bbm_df')
    if bbm_df is not None and not bbm_df.empty:
        country_bbm = bbm_df[
            (bbm_df['country'] == country) &
            (bbm_df.get('active_status', '') == 'Active')
        ]
        if not country_bbm.empty:
            status['bbm'] = country_bbm['measure_type'].unique().tolist()
    
    # Total Capital (Capital Overall)
    capital_df = data.get('capital_overall_df')
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


def get_historical_evolution(country: str, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Időbeli változások."""
    evolution = {}
    
    # CCyB trend
    ccyb_df = data.get('ccyb_df')
    if ccyb_df is not None and not ccyb_df.empty:
        country_ccyb = ccyb_df[ccyb_df['country'] == country].sort_values('date')
        if not country_ccyb.empty:
            cols = ['date', 'rate']
            if 'credit_gap' in country_ccyb.columns:
                cols.append('credit_gap')
            evolution['ccyb'] = country_ccyb[cols].copy()
    
    # SyRB trend
    syrb_df = data.get('syrb_df')
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


def get_recent_changes(country: str, data: Dict[str, pd.DataFrame], months: int = 12) -> List[Dict[str, Any]]:
    """Legutóbbi változások."""
    changes = []
    cutoff_date = datetime.now() - timedelta(days=months * 30)
    
    # CCyB változások
    ccyb_df = data.get('ccyb_df')
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
    syrb_df = data.get('syrb_df')
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
    bbm_df = data.get('bbm_df')
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


def get_active_measures(country: str, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """Aktív, hatályos intézkedések részletei - csak aktív eszközöket ad vissza."""
    measures = {
        'ccyb': None,
        'syrb': [],
        'bbm': [],
        'osii': None,
    }
    
    # CCyB részletek - csak ha aktív (rate > 0)
    ccyb_df = data.get('ccyb_df')
    if ccyb_df is not None and not ccyb_df.empty:
        country_ccyb = ccyb_df[ccyb_df['country'] == country].sort_values('date')
        if not country_ccyb.empty:
            latest = country_ccyb.iloc[-1]
            rate = float(latest.get('rate', 0)) if pd.notna(latest.get('rate')) else 0.0
            # Csak akkor adjuk vissza, ha aktív (rate > 0)
            if rate > 0:
                measures['ccyb'] = {
                    'rate': rate,
                    'date': latest.get('date'),
                    'justification': latest.get('justification', ''),
                    'credit_gap': float(latest.get('credit_gap', 0)) if pd.notna(latest.get('credit_gap')) else None,
                }
    
    # SyRB részletek - General és Sectoral (sSyRB) együtt
    syrb_df = data.get('syrb_df')
    if syrb_df is not None and not syrb_df.empty:
        country_syrb = syrb_df[
            (syrb_df['country'] == country) &
            ((syrb_df.get('active_status', '') == 'Active') |
             (syrb_df.get('status', '').astype(str).str.contains('Active', case=False, na=False)))
        ]
        
        # General SyRB
        general_syrb = country_syrb[
            (country_syrb.get('syrb_type', '').astype(str).str.contains('General', case=False, na=False)) |
            (country_syrb.get('exposure_type', '').astype(str).str.contains('General', case=False, na=False))
        ]
        for _, row in general_syrb.iterrows():
            rate = float(row.get('rate_numeric', 0)) if pd.notna(row.get('rate_numeric')) else 0.0
            if 0 < rate <= 10.0:  # Ésszerű érték
                measures['syrb'].append({
                    'rate': rate,
                    'type': 'General',
                    'exposure': 'General',
                    'date': row.get('date'),
                    'description': row.get('description', ''),
                })
        
        # Sectoral SyRB (sSyRB)
        sectoral_syrb = country_syrb[
            (country_syrb.get('syrb_type', '').astype(str).str.contains('Sectoral', case=False, na=False)) |
            (~country_syrb.get('exposure_type', '').astype(str).str.contains('General', case=False, na=False) &
             country_syrb.get('exposure_type', '').astype(str).str.len() > 0)
        ]
        for _, row in sectoral_syrb.iterrows():
            rate = float(row.get('rate_numeric', 0)) if pd.notna(row.get('rate_numeric')) else 0.0
            if 0 < rate <= 10.0:  # Ésszerű érték
                exposure = row.get('exposure_type', 'Sectoral')
                measures['syrb'].append({
                    'rate': rate,
                    'type': 'Sectoral',
                    'exposure': exposure,
                    'date': row.get('date'),
                    'description': row.get('description', ''),
                })
    
    # BBM részletek - csak aktív eszközök
    bbm_df = data.get('bbm_df')
    if bbm_df is not None and not bbm_df.empty:
        country_bbm = bbm_df[
            (bbm_df['country'] == country) &
            (bbm_df.get('active_status', '') == 'Active')
        ]
        
        for _, row in country_bbm.iterrows():
            # Ellenőrizzük a status mezőt is - csak aktív eszközöket adjunk vissza
            status = str(row.get('status', '')).strip()
            # Ha a status tartalmazza a "not active", "inactive", "revoked", "deactivated" szavakat, akkor kihagyjuk
            if status and any(inactive_term in status.lower() for inactive_term in ['not active', 'inactive', 'revoked', 'deactivated', 'expired']):
                continue
            
            measures['bbm'].append({
                'type': row.get('measure_type', ''),
                'status': 'Active',  # Mivel már szűrtük, mindig Active
                'date': row.get('date'),
                'description': row.get('description', ''),
            })
    
    # O-SII részletek - min-max intervallum és bankok listája
    osii_df = data.get('osii_df')
    if osii_df is not None and not osii_df.empty:
        country_osii = osii_df[osii_df['country'] == country]
        if not country_osii.empty:
            # Aktív O-SII-k szűrése
            active_osii = country_osii[
                (country_osii.get('active_status', '') == 'Active') |
                (country_osii.get('status', '').astype(str).str.contains('Active', case=False, na=False))
            ]
            if active_osii.empty:
                active_osii = country_osii  # Ha nincs explicit status, akkor mindet nézzük
            
            # Min-max intervallum számítása
            active_osii = active_osii.copy()
            active_osii['rate_numeric'] = pd.to_numeric(active_osii.get('rate_numeric', 0), errors='coerce').fillna(0.0)
            active_osii = active_osii[active_osii['rate_numeric'] > 0]
            
            if not active_osii.empty:
                min_rate = float(active_osii['rate_numeric'].min())
                max_rate = float(active_osii['rate_numeric'].max())
                
                # Bankok listája
                banks = []
                for _, row in active_osii.iterrows():
                    bank_name = row.get('bank_name', '')
                    if bank_name:
                        banks.append({
                            'name': bank_name,
                            'rate': float(row.get('rate_numeric', 0)),
                            'buffer_type': row.get('buffer_type', 'O-SII'),
                            'lei_code': row.get('lei_code', ''),
                        })
                
                measures['osii'] = {
                    'rate_min': min_rate,
                    'rate_max': max_rate,
                    'rate': max_rate,  # Backward compatibility
                    'status': 'Active',
                    'count': len(active_osii),
                    'banks': banks,  # Bankok listája
                }
    
    return measures


def get_comparison(country: str, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """Összehasonlítás más országokkal."""
    comparison = {
        'regional_average': None,
        'similar_countries': [],
    }
    
    # Similar countries (hasonló tőkepuffer szinttel)
    capital_df = data.get('capital_overall_df')
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
