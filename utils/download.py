"""
Download utilities.
Safe file downloading with error handling.
"""

import shutil
import requests
from pathlib import Path


def download_file_safely(url, target_path):
    """
    Download a file safely with error handling.
    
    Args:
        url: URL to download from
        target_path: Path where to save the file
        
    Returns:
        True if download successful, False otherwise
    """
    print(f"  Downloading: {target_path.name}...")
    temp_path = target_path.with_suffix('.tmp')
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        with open(temp_path, 'wb') as f:
            f.write(r.content)
        if temp_path.stat().st_size < 1000:
            raise ValueError("Túl kicsi fájl")
        shutil.move(temp_path, target_path)
        return True
    except Exception as e:
        print(f"  Download error ({e}). Using existing file.")
        if temp_path.exists():
            temp_path.unlink()
        return False
