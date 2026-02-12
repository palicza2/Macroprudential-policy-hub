"""
Path utilities.
Directory creation and path management.
"""

from pathlib import Path


def ensure_dirs(*dirs: Path):
    """Ensure directories exist, creating them if necessary."""
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
