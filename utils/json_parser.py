"""
Centralized JSON Parser

Provides safe JSON parsing with fallback regex extraction.
Eliminates code duplication across the codebase.
"""

import json
import re
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def safe_json_loads(text: str, fallback: Optional[Any] = None) -> Optional[Any]:
    """
    Safely parse JSON from text, with fallback regex extraction.
    
    This function tries multiple strategies to extract JSON from text:
    1. Direct JSON parsing
    2. Regex extraction of JSON objects/arrays from text
    3. Returns fallback value if all strategies fail
    
    Args:
        text: Text that may contain JSON
        fallback: Value to return if parsing fails (default: None)
        
    Returns:
        Parsed JSON object/array, or fallback value if parsing fails
        
    Example:
        >>> safe_json_loads('{"key": "value"}')
        {'key': 'value'}
        >>> safe_json_loads('Some text with {"key": "value"} in it')
        {'key': 'value'}
        >>> safe_json_loads('Invalid text', fallback=[])
        []
    """
    if not text:
        return fallback
    
    # Strategy 1: Try direct JSON parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # Strategy 2: Try to extract JSON from text using regex
    # Match JSON objects {...} or arrays [...]
    match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", text)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            logger.debug(f"Failed to parse JSON from extracted match: {match.group(1)[:100]}")
    
    # Strategy 3: Return fallback
    logger.warning(f"Could not parse JSON from text: {text[:200]}...")
    return fallback


def safe_json_loads_list(text: str, default: list = None) -> list:
    """
    Safely parse JSON array from text, with default empty list.
    
    Args:
        text: Text that may contain a JSON array
        default: Default value if parsing fails (default: empty list)
        
    Returns:
        Parsed list, or default value if parsing fails
    """
    if default is None:
        default = []
    
    result = safe_json_loads(text, fallback=default)
    if isinstance(result, list):
        return result
    return default


def safe_json_loads_dict(text: str, default: dict = None) -> dict:
    """
    Safely parse JSON object from text, with default empty dict.
    
    Args:
        text: Text that may contain a JSON object
        default: Default value if parsing fails (default: empty dict)
        
    Returns:
        Parsed dict, or default value if parsing fails
    """
    if default is None:
        default = {}
    
    result = safe_json_loads(text, fallback=default)
    if isinstance(result, dict):
        return result
    return default
