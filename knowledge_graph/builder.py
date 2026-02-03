"""
Knowledge graph data builder for country profiles.
"""
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


def build_knowledge_graph_data(
    get_country_profile_func,
    get_iso2_func,
    get_region_func,
    countries: List[str],
    target_countries: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Knowledge graph adatok generálása.
    
    Args:
        get_country_profile_func: Function to get country profile
        get_iso2_func: Function to get ISO2 code
        get_region_func: Function to get region
        countries: List of all available countries
        target_countries: Opcionális országlista. Ha None, akkor minden ország.
    
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
    target_list = target_countries or countries
    for country in target_list:
        try:
            profile = get_country_profile_func(country)
            if not profile:
                continue
                
            iso2 = profile.get('iso2') or get_iso2_func(country) or country[:2].upper()
            
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
                'region': get_region_func(country),
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
                    similar_iso2 = get_iso2_func(similar_name) or similar_name[:2].upper()
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
