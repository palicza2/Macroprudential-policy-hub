"""
LLM formatter utilities for profile data.
"""


def format_profile_for_llm(profile_data):
    """Format profile data as text for LLM analysis."""
    status = profile_data.get('current_status', {})
    changes = profile_data.get('recent_changes', [])
    
    text = f"Country: {profile_data.get('country', 'Unknown')}\n\n"
    text += "Current Status:\n"
    if status.get('ccyb'):
        text += f"- CCyB: {status['ccyb'].get('rate', 0)}%\n"
    if status.get('syrb'):
        text += f"- SyRB: {status['syrb'].get('rate', 0)}%\n"
    if status.get('osii'):
        text += f"- O-SII: {status['osii'].get('rate', 0)}%\n"
    if status.get('total_capital'):
        text += f"- Total Capital: {status['total_capital'].get('total', 0)}%\n"
    
    text += "\nRecent Changes (Last 12 Months):\n"
    for change in changes[:5]:
        date_str = change.get('date', 'N/A')
        if isinstance(date_str, str) and len(date_str) > 10:
            date_str = date_str[:10]
        text += f"- {date_str}: {change.get('type', '')} {change.get('change', '')}\n"
    
    return text
