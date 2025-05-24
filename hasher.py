"""
hasher.py

This module provides functions for computing partial and full hashes of files.
It avoids redundant full hashing by storing computed hashes in a 'full_hash'
field within each file dictionary.

All hashers accept file dicts like:
    {
        "path": "/path/to/file",
        "size": 123456,
        "full_hash": None or b'\x01\x02\x03...'
    }
"""

import os
import xxhash
from typing import Optional, Dict, Any, Callable

# Files ≤ this size will be fully hashed
FULL_HASH_THRESHOLD: int = 254 * 1024  # 254 KB


def use_full_hash_if_small(func: Callable[[Dict[str, Any]], Optional[bytes]]) -> Callable:
    """
    Decorator that replaces partial hashing with full hash
    for files ≤ FULL_HASH_THRESHOLD.
    """
    def wrapper(file_info: Dict[str, Any]) -> Optional[bytes]:
        # If already full-hashed, return cached value
        if file_info.get("full_hash"):
            return file_info.get("_full_hash_value")

        # Get file size
        size = file_info["size"]

        # If small, compute and cache full hash
        if size <= FULL_HASH_THRESHOLD:
            return compute_full_hash(file_info)

        # Otherwise proceed with original function
        return func(file_info)
    return wrapper


def get_chunk_size(file_size: int) -> int:
    """
    Determines chunk size based on file size for partial hashing.
    """
    if file_size <= FULL_HASH_THRESHOLD:
        return file_size

    elif file_size <= 5 * 1024 * 1024:
        return 64 * 1024
    elif file_size <= 15 * 1024 * 1024:
        return 128 * 1024
    elif file_size <= 30 * 1024 * 1024:
        return 256 * 1024
    elif file_size <= 60 * 1024 * 1024:
        return 512 * 1024
    elif file_size <= 120 * 1024 * 1024:
        return 1 * 1024 * 1024
    else:
        return 2 * 1024 * 1024


def compute_hash_at_offset(file_info: Dict[str, Any], offset: int, chunk_size: int) -> Optional[bytes]:
    """
    Computes hash from a specific offset in the file.

    Args:
        file_info: Dictionary with at least "path"
        offset: Byte offset to start reading from
        chunk_size: Number of bytes to read

    Returns:
        bytes | None: 8-byte hash digest or None on error
    """
    try:
        with open(file_info["path"], 'rb') as f:
            f.seek(offset)
            chunk = f.read(chunk_size)
            if not chunk:
                return None
            return xxhash.xxh64(chunk).digest()
    except Exception:
        return None


def compute_full_hash(file_info: Dict[str, Any]) -> Optional[bytes]:
    """
    Computes full xxHash64 of entire file.
    Stores result in file_info["full_hash"] to prevent recomputation.

    Returns:
        bytes | None: Full hash digest or None on error
    """
    if file_info.get("full_hash"):
        return file_info.get("_full_hash_value")

    path = file_info["path"]
    try:
        with open(path, 'rb') as f:
            digest = xxhash.xxh64(f.read()).digest()
            file_info["_full_hash_value"] = digest
            file_info["full_hash"] = True
            return digest
    except Exception:
        return None


@use_full_hash_if_small
def compute_partial_hash(file_info: Dict[str, Any]) -> Optional[bytes]:
    """Computes hash of the first N bytes of a file."""
    size = file_info["size"]
    chunk_size = get_chunk_size(size)
    return compute_hash_at_offset(file_info, 0, chunk_size)


@use_full_hash_if_small
def compute_end_hash(file_info: Dict[str, Any]) -> Optional[bytes]:
    """Computes hash of the last N bytes of a file."""
    size = file_info["size"]
    chunk_size = get_chunk_size(size)
    offset = max(0, size - chunk_size)
    return compute_hash_at_offset(file_info, offset, chunk_size)


@use_full_hash_if_small
def compute_middle_hash(file_info: Dict[str, Any]) -> Optional[bytes]:
    """Computes hash of the central N bytes of a file."""
    size = file_info["size"]
    chunk_size = get_chunk_size(size)
    offset = max(0, (size - chunk_size) // 2)
    return compute_hash_at_offset(file_info, offset, chunk_size)


@use_full_hash_if_small
def compute_first_quarter_hash(file_info: Dict[str, Any]) -> Optional[bytes]:
    """Computes hash of the first 25% of the file."""
    size = file_info["size"]
    chunk_size = get_chunk_size(size)
    offset = max(0, size // 4 - chunk_size // 2)
    return compute_hash_at_offset(file_info, offset, chunk_size)


@use_full_hash_if_small
def compute_third_quarter_hash(file_info: Dict[str, Any]) -> Optional[bytes]:
    """Computes hash of the third quarter of the file."""
    size = file_info["size"]
    chunk_size = get_chunk_size(size)
    offset = max(0, (3 * size // 4) - chunk_size // 2)
    return compute_hash_at_offset(file_info, offset, chunk_size)