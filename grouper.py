# grouper.py

from typing import List, Dict, Any, Callable
from collections import defaultdict
import os
import hasher


def group_files(file_paths: List[str], key_func: Callable[[str], Any]) -> Dict[Any, List[str]]:
    """
    Groups file paths by a computed key.

    Parameters:
        file_paths (List[str]): List of file paths
        key_func (Callable): Function that returns a hashable key for each path

    Returns:
        Dict[key, List[str]]: Dictionary mapping key → list of files with this key
    """
    groups = defaultdict(list)
    for path in file_paths:
        try:
            key = key_func(path)
            if key is not None:
                groups[key].append(path)
        except Exception as e:
            print(f"⚠️ Error processing {path}: {e}")
    return dict(groups)


def group_by_size(files: List[Dict[str, Any]]) -> Dict[int, List[str]]:
    """
    Groups files by size using precomputed 'size' field.
    No need to re-read size from disk — it's already available.
    
    Parameters:
        files (List[Dict]): List of file info dicts with 'size' and 'path' keys
    
    Returns:
        Dict[int, List[str]]: Dictionary mapping file size → list of matching files
    """
    groups = defaultdict(list)
    for file_info in files:
        size = file_info["size"]
        groups[size].append(file_info["path"])
    return dict(groups)


def group_by_partial_hash(paths: List[str] ) -> Dict[bytes, List[str]]:
    """
    Groups files by hash of the first N bytes.

    Parameters:
        paths (List[str]): List of file paths

    Returns:
        Dict[bytes, List[str]]: Dictionary mapping partial hash → list of files
    """
    def get_key(path):
        return hasher.compute_partial_hash(path)
    return group_files(paths, get_key)


def group_by_end_hash(paths: List[str]) -> Dict[bytes, List[str]]:
    """
    Groups files by hash of the last N bytes.

    Parameters:
        paths (List[str]): List of file paths

    Returns:
        Dict[bytes, List[str]]: Dictionary mapping end hash → list of files
    """
    def get_key(path):
        return hasher.compute_end_hash(path)
    return group_files(paths, get_key)


def group_by_middle_hash(paths: List[str]) -> Dict[bytes, List[str]]:
    """
    Groups files by hash of the central N bytes.

    Parameters:
        paths (List[str]): List of file paths

    Returns:
        Dict[bytes, List[str]]: Dictionary mapping middle hash → list of files
    """
    def get_key(path):
        return hasher.compute_middle_hash(path)
    return group_files(paths, get_key)


def group_by_first_quarter_hash(paths: List[str]) -> Dict[bytes, List[str]]:
    """
    Groups files by hash of the first 25% of the file.

    Parameters:
        paths (List[str]): List of file paths

    Returns:
        Dict[bytes, List[str]]: Dictionary mapping hash → list of files
    """
    def get_key(path):
        return hasher.compute_first_quarter_hash(path)
    return group_files(paths, get_key)


def group_by_third_quarter_hash(paths: List[str]) -> Dict[bytes, List[str]]:
    """
    Groups files by hash of the last 25% of the file.

    Parameters:
        paths (List[str]): List of file paths

    Returns:
        Dict[bytes, List[str]]: Dictionary mapping hash → list of files
    """
    def get_key(path):
        return hasher.compute_third_quarter_hash(path)
    return group_files(paths, get_key)


def group_by_full_hash(paths: List[str]) -> Dict[bytes, List[str]]:
    """
    Groups files by full content hash.

    Parameters:
        paths (List[str]): List of file paths

    Returns:
        Dict[bytes, List[str]]: Dictionary mapping full hash → list of files
    """
    def get_key(path):
        return hasher.compute_full_hash(path)
    return group_files(paths, get_key)
