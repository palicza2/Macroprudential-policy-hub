"""
Data aggregation functions for country profiles.
"""
import json
import logging
from pathlib import Path

import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from utils.dataframe import ccyb_change_only_points, get_latest_quarter_end
from country_profiles.region_mapper import get_iso2
from bbm.matrix_builder import get_active_bbm_for_country, RENAME_MAP

logger = logging.getLogger(__name__)


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
                # Normalize to percentage scale (e.g. 0.01-0.02 -> 1-2%)
                if max_rate > 0 and max_rate < 1:
                    min_rate = min_rate * 100
                    max_rate = max_rate * 100
                if abs(max_rate - min_rate) < 0.01:
                    rate_display = f"{int(max_rate)}%" if max_rate == int(max_rate) else f"{max_rate:.2f}%"
                elif min_rate < 0.01:
                    rate_display = f"0-{int(round(max_rate))}%" if max_rate == int(max_rate) else f"0-{max_rate:.1f}%"
                else:
                    # Preserve 0.5 etc.: use .1f when fractional, else int (e.g. 0.5-2%, 1-2%)
                    min_str = f"{min_rate:.1f}" if min_rate != int(min_rate) else str(int(min_rate))
                    max_str = f"{max_rate:.1f}" if max_rate != int(max_rate) else str(int(max_rate))
                    rate_display = f"{min_str}-{max_str}%"
                status['osii'] = {
                    'rate_min': min_rate,
                    'rate_max': max_rate,
                    'rate': max_rate,
                    'rate_display': rate_display,
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
    
    # BBM - same logic as BBM overview table (is_bbm_row_active, country or iso2 match)
    bbm_df = data.get('bbm_df')
    if bbm_df is not None and not bbm_df.empty:
        iso2 = get_iso2(country)
        country_bbm = get_active_bbm_for_country(bbm_df, country, iso2=iso2)
        if not country_bbm.empty:
            # Use measure_short (LTV, DSTI, etc.) for consistency with overview; fallback to measure_type
            if "measure_short" not in country_bbm.columns:
                country_bbm = country_bbm.copy()
                country_bbm["measure_short"] = country_bbm["measure_type"].map(lambda x: RENAME_MAP.get(x, x))
            status['bbm'] = country_bbm["measure_short"].unique().tolist()
    
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
    
    # CCyB trend — only decision/change points; between changes buffer is unchanged (step)
    ccyb_df = data.get('ccyb_df')
    if ccyb_df is not None and not ccyb_df.empty:
        change_only = ccyb_change_only_points(ccyb_df)
        country_col = 'country' if 'country' in change_only.columns else 'iso2' if not change_only.empty else 'country'
        if not change_only.empty:
            country_ccyb = change_only[change_only[country_col] == country].sort_values('date')
        else:
            country_ccyb = ccyb_df[ccyb_df['country'] == country].sort_values('date') if 'country' in ccyb_df.columns else pd.DataFrame()
        if not country_ccyb.empty:
            cols = ['date', 'rate']
            if 'credit_gap' in country_ccyb.columns:
                cols.append('credit_gap')
            evolution['ccyb'] = country_ccyb[[c for c in cols if c in country_ccyb.columns]].copy()
            # Extrapolate last rate to latest quarter end (no rate change assumed)
            latest_q_end = get_latest_quarter_end()
            last_date = evolution['ccyb']['date'].iloc[-1]
            if pd.Timestamp(last_date) < latest_q_end:
                last_rate = float(evolution['ccyb']['rate'].iloc[-1])
                extra = {'date': latest_q_end, 'rate': last_rate}
                if 'credit_gap' in evolution['ccyb'].columns:
                    extra['credit_gap'] = evolution['ccyb']['credit_gap'].iloc[-1]
                evolution['ccyb'] = pd.concat([
                    evolution['ccyb'],
                    pd.DataFrame([extra])
                ], ignore_index=True)
    
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
    
    # BBM részletek - same logic as BBM overview (get_active_bbm_for_country)
    bbm_df = data.get('bbm_df')
    if bbm_df is not None and not bbm_df.empty:
        iso2 = get_iso2(country)
        country_bbm = get_active_bbm_for_country(bbm_df, country, iso2=iso2)
        
        for _, row in country_bbm.iterrows():
            measure_type = row.get('measure_type', '')
            measure_short = RENAME_MAP.get(measure_type, measure_type)
            measures['bbm'].append({
                'type': measure_short or measure_type,
                'status': 'Active',
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
                # Normalize to percentage scale (e.g. 0.005-0.02 -> 0.5-2%)
                if max_rate > 0 and max_rate < 1:
                    min_rate = min_rate * 100
                    max_rate = max_rate * 100

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


def _load_institutional_setup_json() -> Dict[str, Dict[str, Any]]:
    """Load institutional setup from data/institutional_setup.json (keyed by iso2)."""
    candidates = [
        Path(__file__).resolve().parent.parent / "data" / "institutional_setup.json",
        Path("data") / "institutional_setup.json",
    ]
    for p in candidates:
        if p.exists():
            try:
                with open(p, encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load institutional_setup.json from {p}: {e}")
    return {}


def get_institutional_setup(
    country: str,
    data: Dict[str, Any],
    iso2: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Get institutional setup of macroprudential policy for a country.
    Priority: data['institutional_setup_df'] > data['institutional_setup_by_country'] > data/institutional_setup.json.
    """
    iso2 = iso2 or get_iso2(country)

    # 1. DataFrame in pipeline data (e.g. from Supabase)
    df = data.get("institutional_setup_df")
    if df is not None and not df.empty and iso2:
        col = "country_iso2" if "country_iso2" in df.columns else "iso2"
        if col in df.columns:
            row = df[df[col] == iso2]
            if not row.empty:
                r = row.iloc[0].to_dict()
                return {k: (None if pd.isna(v) else v) for k, v in r.items()}

    # 2. Dict by country/iso2 in pipeline data
    by_country = data.get("institutional_setup_by_country") or {}
    if country in by_country:
        return dict(by_country[country])
    if iso2 and iso2 in by_country:
        return dict(by_country[iso2])

    # 3. Load from data/institutional_setup.json
    if iso2:
        setup_map = _load_institutional_setup_json()
        if iso2 in setup_map:
            return dict(setup_map[iso2])

    return None
