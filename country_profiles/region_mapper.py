"""
Region and ISO code mapping utilities.
"""
from typing import Optional


def get_iso2(country: str) -> Optional[str]:
    """ISO2 kód lekérése."""
    try:
        import country_converter as coco
        iso2 = coco.convert(names=country, to='iso2', not_found=None)
        return iso2
    except Exception:
        return None


def get_region(country: str) -> str:
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
