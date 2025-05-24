"""
grouper.py

This module provides functions for grouping files by various hash strategies.
Each function accepts a list of file dictionaries and returns a dictionary mapping
hash keys to lists of matching files.

Example file dict:
    {
        "path": "/path/to/file",
        "size": 123456,
        "full_hash": None or b'\x01\x02\x03..."
    }
"""

from typing import List, Dict, Any, Callable
from collections import defaultdict
import hasher


def group_files(
        file_infos: List[Dict[str, Any]],
        key_func: Callable[[Dict[str, Any]], Any]
) -> Dict[Any, List[Dict[str, Any]]]:
    """
    Groups file info dicts by a computed key.

    Args:
        file_infos: List of file dicts with at least "path" and "size"
        key_func: Function that takes a file dict and returns a hashable key

    Returns:
        Dict[key, List[file_dict]]
    """
    groups = defaultdict(list)
    for file_info in file_infos:
        try:
            key = key_func(file_info)
            if key is not None:
                groups[key].append(file_info)
        except Exception as e:
            print(f"⚠️ Error processing {file_info['path']}: {e}")
    return dict(groups)


def group_by_size(file_infos: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Groups files by size using precomputed 'size' field.

    Args:
        file_infos: List of file dicts with "size" and "path"

    Returns:
        Dict[size, List[file_dict]]
    """
    groups = defaultdict(list)
    for file_info in file_infos:
        size = file_info["size"]
        groups[size].append(file_info)
    return dict(groups)

def group_by_partial_hash(file_infos: List[Dict[str, Any]]) -> Dict[bytes, List[Dict[str, Any]]]:
    """
    Groups files by hash of the first N bytes.

    Args:
        file_infos: List of file dicts

    Returns:
        Dict[hash, List[file_dict]]
    """
    def get_key(file_info):
        return hasher.compute_partial_hash(file_info)

    return group_files(file_infos, get_key)


def group_by_end_hash(file_infos: List[Dict[str, Any]]) -> Dict[bytes, List[Dict[str, Any]]]:
    """
    Groups files by hash of the last N bytes.

    Args:
        file_infos: List of file dicts

    Returns:
        Dict[hash, List[file_dict]]
    """
    def get_key(file_info):
        return hasher.compute_end_hash(file_info)

    return group_files(file_infos, get_key)


def group_by_middle_hash(file_infos: List[Dict[str, Any]]) -> Dict[bytes, List[Dict[str, Any]]]:
    """
    Groups files by hash of the central N bytes.

    Args:
        file_infos: List of file dicts

    Returns:
        Dict[hash, List[file_dict]]
    """
    def get_key(file_info):
        return hasher.compute_middle_hash(file_info)

    return group_files(file_infos, get_key)


def group_by_first_quarter_hash(file_infos: List[Dict[str, Any]]) -> Dict[bytes, List[Dict[str, Any]]]:
    """
    Groups files by hash of the first 25% of the file.

    Args:
        file_infos: List of file dicts

    Returns:
        Dict[hash, List[file_dict]]
    """
    def get_key(file_info):
        return hasher.compute_first_quarter_hash(file_info)

    return group_files(file_infos, get_key)


def group_by_third_quarter_hash(file_infos: List[Dict[str, Any]]) -> Dict[bytes, List[Dict[str, Any]]]:
    """
    Groups files by hash of the third quarter of the file.

    Args:
        file_infos: List of file dicts

    Returns:
        Dict[hash, List[file_dict]]
    """
    def get_key(file_info):
        return hasher.compute_third_quarter_hash(file_info)

    return group_files(file_infos, get_key)


def group_by_full_hash(file_infos: List[Dict[str, Any]]) -> Dict[bytes, List[Dict[str, Any]]]:
    """
    Groups files by full content hash.

    Args:
        file_infos: List of file dicts

    Returns:
        Dict[hash, List[file_dict]]
    """
    def get_key(file_info):
        return hasher.compute_full_hash(file_info)

    return group_files(file_infos, get_key)