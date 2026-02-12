"""
Utils - Legacy compatibility module.

⚠️ DEPRECATED: This module is kept for backward compatibility only.
All functions have been moved to the utils/ package:
- SuppressOutput -> utils.output
- ensure_dirs -> utils.paths
- download_file_safely -> utils.download
- clean_columns, find_header_row, extract_rate -> utils.dataframe
- create_download_link -> utils.html

Please update your imports to use the utils package directly.
"""

import warnings

# Import from utils package
from utils import (
    SuppressOutput,
    ensure_dirs,
    download_file_safely,
    clean_columns,
    find_header_row,
    extract_rate,
    create_download_link,
)

# Export all for backward compatibility
__all__ = [
    'SuppressOutput',
    'ensure_dirs',
    'download_file_safely',
    'clean_columns',
    'find_header_row',
    'extract_rate',
    'create_download_link',
]
