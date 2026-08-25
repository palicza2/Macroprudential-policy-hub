"""
Download utilities.
Safe file downloading with hash-before-replace so identical bytes are not overwritten.
"""
from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Optional

import requests


def _sha256_file(path: Path) -> Optional[str]:
    if not path.exists() or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_if_changed(temp_path: Path, target_path: Path) -> bool:
    """
    Move temp_path onto target_path only when content differs.

    Returns True if the target was created or replaced, False if it was left as-is.
    """
    target_path = Path(target_path)
    temp_path = Path(temp_path)
    new_hash = _sha256_file(temp_path)
    old_hash = _sha256_file(target_path)
    if new_hash and old_hash and new_hash == old_hash:
        temp_path.unlink(missing_ok=True)
        return False
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(temp_path), str(target_path))
    return True


def download_file_safely(url, target_path) -> bool:
    """
    Download a file safely. Identical content is not written (hash-before-replace).

    Returns:
        True if a usable file exists at target_path after the call.
    """
    target_path = Path(target_path)
    print(f"  Downloading: {target_path.name}...")
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        with open(temp_path, "wb") as f:
            f.write(r.content)
        if temp_path.stat().st_size < 1000:
            raise ValueError("Túl kicsi fájl")
        changed = replace_if_changed(temp_path, target_path)
        if not changed:
            print(f"  Unchanged (hash match): {target_path.name}")
        return True
    except Exception as e:
        print(f"  Download error ({e}). Using existing file.")
        if temp_path.exists():
            temp_path.unlink()
        return target_path.exists()
