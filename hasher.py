import os
import xxhash
from typing import Optional
from functools import lru_cache


def get_chunk_size(file_size: int) -> int:
    """Determine chunk size based on file size."""
    if file_size <= 64 * 1024:  # 128KB
        return file_size
    elif file_size <= 1 * 1024 * 1024:  # 1MB
        # Reduce chunk size for files near 64KB boundary
        if file_size < 256 * 1024:
            return 32 * 1024
        return 64 * 1024
    elif file_size <= 10 * 1024 * 1024:  # 10MB
        return 128 * 1024
    elif file_size <= 100 * 1024 * 1024:  # 100MB
        return 256 * 1024
    elif file_size <= 1024 * 1024 * 1024:  # 1GB
        return 512 * 1024
    else:
        return 1024 * 1024  # 1MB


@lru_cache(maxsize=8192)
def _get_full_hash_for_small_file(path: str) -> Optional[bytes]:
    """
    Returns cached full hash for small files (<= 64KB).
    Returns None if file is not small or if reading fails.
    """
    try:
        size = os.path.getsize(path)
    except Exception:
        return None

    if size > 128 * 1024:
        return None  # Not a small file

    return compute_full_hash(path)


def compute_hash_at_offset(path: str, offset: int, chunk_size: int) -> Optional[bytes]:
    """
    Computes hash from a specific offset in the file.

    Parameters:
        path (str): Path to file
        offset (int): Byte offset to start reading from
        chunk_size (int): Number of bytes to read

    Returns:
        bytes | None: 8-byte hash digest or None on error
    """
    try:
        with open(path, 'rb') as f:
            f.seek(offset)
            chunk = f.read(chunk_size)
            if not chunk:
                return None
            return xxhash.xxh64(chunk).digest()
    except Exception:
        return None


def compute_partial_hash(path: str) -> Optional[bytes]:
    """Computes hash of the first N bytes of a file."""
    cached = _get_full_hash_for_small_file(path)
    if cached is not None:
        return cached

    try:
        size = os.path.getsize(path)
    except Exception:
        return None

    chunk_size = get_chunk_size(size)
    return compute_hash_at_offset(path, 0, chunk_size)


def compute_end_hash(path: str) -> Optional[bytes]:
    """Computes hash of the last N bytes of a file."""
    cached = _get_full_hash_for_small_file(path)
    if cached is not None:
        return cached

    try:
        size = os.path.getsize(path)
    except Exception:
        return None

    chunk_size = get_chunk_size(size)
    offset = max(0, size - chunk_size)
    return compute_hash_at_offset(path, offset, chunk_size)


def compute_middle_hash(path: str) -> Optional[bytes]:
    """Computes hash of the central N bytes of a file."""
    cached = _get_full_hash_for_small_file(path)
    if cached is not None:
        return cached

    try:
        size = os.path.getsize(path)
    except Exception:
        return None

    chunk_size = get_chunk_size(size)
    offset = max(0, (size - chunk_size) // 2)
    return compute_hash_at_offset(path, offset, chunk_size)


def compute_first_quarter_hash(path: str) -> Optional[bytes]:
    """Computes hash of the first 25% of the file."""
    cached = _get_full_hash_for_small_file(path)
    if cached is not None:
        return cached
    try:
        size = os.path.getsize(path)
    except Exception:
        return None

    chunk_size = get_chunk_size(size)
    offset = max(0, size // 4 - chunk_size // 2)
    return compute_hash_at_offset(path, offset, chunk_size)


def compute_third_quarter_hash(path: str) -> Optional[bytes]:
    """Computes hash of the third quarter of the file."""
    cached = _get_full_hash_for_small_file(path)
    if cached is not None:
        return cached

    try:
        size = os.path.getsize(path)
    except Exception:
        return None

    chunk_size = get_chunk_size(size)
    offset = max(0, (3 * size // 4) - chunk_size // 2)
    return compute_hash_at_offset(path, offset, chunk_size)


@lru_cache(maxsize=8192)  # Cache full hashes for up to 8192 files
def compute_full_hash(path: str) -> Optional[bytes]:
    """
    Computes full xxHash64 of entire file.
    Caches results to avoid redundant full hash computations.
    """
    try:
        with open(path, 'rb') as f:
            return xxhash.xxh64(f.read()).digest()
    except Exception:
        return None