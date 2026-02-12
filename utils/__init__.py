"""
Utility modules for the macroprudential hub.
"""

# Import all utilities for backward compatibility
from .output import SuppressOutput
from .paths import ensure_dirs
from .download import download_file_safely
from .dataframe import clean_columns, find_header_row, extract_rate
from .html import create_download_link

__all__ = [
    'SuppressOutput',
    'ensure_dirs',
    'download_file_safely',
    'clean_columns',
    'find_header_row',
    'extract_rate',
    'create_download_link',
]
