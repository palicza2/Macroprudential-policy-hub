"""
OSII/GSII data processing and HTML table generation.
Extracts bank counts and rate ranges per country from OSII data.
"""
import pandas as pd
import logging
import re
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def parse_osii_description(description: str) -> Dict[str, any]:
    """
    Parse OSII description to extract bank count and rate range.
    Example: "10 banks: 0.45%-1.75%" -> {"bank_count": 10, "min_rate": 0.45, "max_rate": 1.75}
    """
    if pd.isna(description) or not description:
        return {"bank_count": 0, "min_rate": 0.0, "max_rate": 0.0}
    
    description = str(description).strip()
    
    # Extract bank count
    bank_count_match = re.search(r'(\d+)\s*banks?', description, re.IGNORECASE)
    bank_count = int(bank_count_match.group(1)) if bank_count_match else 0
    
    # Extract rate range (e.g., "0.45%-1.75%" or "0.5%-2%")
    rate_matches = re.findall(r'(\d+(?:\.\d+)?)\s*%', description)
    rates = [float(r) for r in rate_matches if 0 <= float(r) <= 20.0]
    
    if rates:
        min_rate = min(rates)
        max_rate = max(rates)
    else:
        min_rate = 0.0
        max_rate = 0.0
    
    return {
        "bank_count": bank_count,
        "min_rate": min_rate,
        "max_rate": max_rate,
        "rate_range": f"{min_rate:.2f}%-{max_rate:.2f}%" if min_rate != max_rate else f"{max_rate:.2f}%"
    }


def prepare_osii_by_country(osii_df: Optional[pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Prepare OSII data grouped by country with individual bank information.
    Returns a dictionary mapping country names to DataFrames with bank details.
    """
    if osii_df is None or osii_df.empty:
        return {}
    
    result = {}
    
    # Group by country
    for country in osii_df['country'].dropna().unique():
        country_df = osii_df[osii_df['country'] == country].copy()
        
        # If we have individual bank data (bank_name column exists)
        if 'bank_name' in country_df.columns:
            # Use individual bank data
            result[country] = country_df[[
                'country', 'iso2', 'bank_name', 'lei_code', 'rate_numeric', 
                'rate_text', 'buffer_type', 'gsii_rate', 'osii_rate', 'status', 'date'
            ]].copy()
        else:
            # Fallback: aggregate data (old format)
            for _, row in country_df.iterrows():
                description = row.get('description', '')
                rate_numeric = row.get('rate_numeric', 0.0)
                status = row.get('status', 'Active')
                date = row.get('date', pd.Timestamp.now())
                
                # Parse description
                parsed = parse_osii_description(description)
                
                # Create DataFrame row for this country
                country_data = pd.DataFrame([{
                    'country': country,
                    'iso2': row.get('iso2', ''),
                    'bank_count': parsed['bank_count'],
                    'min_rate': parsed['min_rate'],
                    'max_rate': parsed['max_rate'],
                    'rate_range': parsed['rate_range'],
                    'max_rate_numeric': rate_numeric,
                    'status': status,
                    'date': date,
                    'description': description
                }])
                
                if country not in result:
                    result[country] = country_data
                else:
                    result[country] = pd.concat([result[country], country_data], ignore_index=True)
    
    return result


def build_osii_table_html(osii_by_country: Dict[str, pd.DataFrame], selected_country: str = "Austria") -> str:
    """
    Build HTML table for OSII/GSII data for a selected country.
    Shows individual banks if available, otherwise shows aggregate data.
    """
    if not osii_by_country or selected_country not in osii_by_country:
        return "<div class='empty-state'>No OSII/GSII data available for selected country.</div>"
    
    df = osii_by_country[selected_country]
    
    # Check if we have individual bank data
    if 'bank_name' in df.columns:
        # Show all banks, not just active ones
        # Sort: active banks first, then by bank name
        if 'status' in df.columns:
            df_sorted = df.copy()
            df_sorted['_sort_key'] = df_sorted['status'].apply(lambda x: 0 if x == 'Active' else 1)
            df_sorted = df_sorted.sort_values(['_sort_key', 'bank_name']).drop(columns=['_sort_key'])
        else:
            df_sorted = df.sort_values('bank_name')
        
        # Individual bank table
        html = """
        <div class="osii-table-wrapper">
        <table class="osii-table">
            <thead>
                <tr>
                    <th>Bank Name</th>
                    <th>LEI Code</th>
                    <th>Type</th>
                    <th>G-SII</th>
                    <th>O-SII</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for _, row in df_sorted.iterrows():
            gsii_display = f"{row['gsii_rate']:.2f}%" if pd.notna(row.get('gsii_rate')) and row.get('gsii_rate', 0) > 0 else "-"
            osii_display = f"{row['osii_rate']:.2f}%" if pd.notna(row.get('osii_rate')) and row.get('osii_rate', 0) > 0 else "-"
            
            # Truncate long bank names
            bank_name = str(row['bank_name'])
            if len(bank_name) > 60:
                bank_name = bank_name[:57] + "..."
            
            # Format LEI code (show only first 4 and last 4 chars for compactness)
            lei_code = str(row.get('lei_code', '') or '-')
            if len(lei_code) > 12 and lei_code != '-':
                lei_code = lei_code[:4] + "..." + lei_code[-4:]
            
            # Get status and style accordingly
            status = row.get('status', 'Active') if 'status' in row else 'Active'
            status_class = 'status-active' if status == 'Active' else 'status-inactive'
            
            html += f"""
                <tr class="{status_class}">
                    <td class="bank-name"><strong>{bank_name}</strong></td>
                    <td class="lei-code">{lei_code}</td>
                    <td class="buffer-type">{row.get('buffer_type', 'N/A')}</td>
                    <td class="rate-cell">{gsii_display}</td>
                    <td class="rate-cell">{osii_display}</td>
                    <td class="status-cell"><span class="status-badge status-{status.lower().replace(' ', '-')}">{status}</span></td>
                </tr>
            """
        
        html += """
            </tbody>
        </table>
        </div>
        """
    else:
        # Aggregate table (fallback for old format)
        html = """
        <table class="data-table">
            <thead>
                <tr>
                    <th>Country</th>
                    <th>Number of Banks</th>
                    <th>Rate Range</th>
                    <th>Maximum Rate</th>
                    <th>Status</th>
                    <th>Description</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for _, row in df.iterrows():
            html += f"""
                <tr>
                    <td><strong>{row['country']}</strong> ({row['iso2']})</td>
                    <td>{int(row.get('bank_count', 0))}</td>
                    <td>{row.get('rate_range', 'N/A')}</td>
                    <td><strong>{row.get('max_rate_numeric', 0):.2f}%</strong></td>
                    <td>{row.get('status', 'Active')}</td>
                    <td>{row.get('description', '')}</td>
                </tr>
            """
        
        html += """
            </tbody>
        </table>
        """
    
    return html


def get_osii_countries(osii_df: Optional[pd.DataFrame]) -> List[str]:
    """Get list of countries with OSII/GSII data."""
    if osii_df is None or osii_df.empty:
        return []
    
    countries = osii_df['country'].dropna().unique().tolist()
    return sorted([str(c) for c in countries if str(c).strip()])
