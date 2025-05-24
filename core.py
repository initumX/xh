# core.py
import os
import sys
from typing import List, Dict, Any


def file_size_pass(size: int, min_size: int = None, max_size: int = None) -> bool:
    """
    Checks if a file's size is within the allowed range.
    Always excludes zero-byte files.

    Parameters:
        size (int): File size in bytes
        min_size (int): Minimum required file size (optional)
        max_size (int): Maximum allowed file size (optional)

    Returns:
        bool: True if file passes all size checks, False otherwise
    """
    if size == 0:
        return False # Skip blank files
    if min_size is not None and size < min_size:
        return False
    if max_size is not None and size > max_size:
        return False
    return True


def file_extension_pass(filename: str, extensions: List[str] = None) -> bool:
    """
    Checks if a file's extension matches any of the allowed extensions (case-insensitive).
    
    Skipped files:
        - Hidden files (starting with '.') only if extensions are specified
        - Files without an extension only if extensions are specified
    
    Parameters:
        filename (str): The full path or base name of the file
        extensions (List[str]): List of allowed extensions like ['.txt', '.tar.gz']
    
    Returns:
        bool: 
            - True if no extensions filter is used
            - True if file extension matches any of the allowed extensions
            - False otherwise
    """
    # No filter → allow all files
    if not extensions:
        return True

    base_name = os.path.basename(filename)

    # Skip hidden files (starting with '.') and files without extension
    if base_name.startswith("."):
        return False

    parts = base_name.split(".")

    # Must have at least one extension part after the last dot
    if len(parts) < 2:
        return False

    # Try to match multi-part extensions like '.tar.gz', '.min.js'
    for i in range(1, len(parts)):
        candidate = "." + ".".join(parts[i:])
        # Compare lowercase version of candidate and extensions
        if any(candidate.lower() == ext.lower() for ext in extensions):
            return True

    # No match found
    return False


def sort_files_by_size_desc(files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sorts a list of file dicts by size descending.
    
    Parameters:
        files (List[Dict]): List of dicts with 'path' and 'size'
    
    Returns:
        List[Dict]: Sorted list from largest to smallest
    """
    return sorted(files, key=lambda f: -f.get("size", 0))


def scan_directory(
    root_dir: str,
    min_size: int = None,
    max_size: int = None,
    extensions: List[str] = None
) -> List[Dict[str, str]]:
    """
    Recursively scans a directory and collects readable files.
    
    Applies optional filters:
        - By size (min_size, max_size)
        - By extension (e.g., ['.txt', '.tar.gz'])

    Parameters:
        root_dir (str): Root directory to scan
        min_size (int): Minimum file size in bytes
        max_size (int): Maximum file size in bytes
        extensions (List[str]): List of allowed extensions like ['.txt', '.tar.gz']

    Returns:
        List[Dict]: List of dicts with 'path' key for each matching file
    """
    files = []

    # Walk through the directory tree
    print(f"🔍 Scanning directory: {root_dir}")
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)

            try:
                # Check if file is readable before adding it to the list
                if not os.access(filepath, os.R_OK):
                    continue  # Skip files without read permissions
            except Exception as e:
                # Handle unexpected errors (e.g., broken symlinks, deleted files)
                print(f"⚠️ Can't access {filepath}: {e}")
                continue  # Skip this file and keep scanning others

            # Check extension
            if not file_extension_pass(filename, extensions):
                continue

            # Get size
            try:
                size = os.path.getsize(filepath)
            except Exception as e:
                print(f"⚠️ Can't get size of {filepath}: {e}")
                continue

            # Apply file size filter
            if not file_size_pass(size, min_size=min_size, max_size=max_size):
                continue

            # Add file info to the list
            files.append({
                'path': filepath,
                'size': size,
                'full_hash': None # Will be set later if computed
            })
    
    files = sort_files_by_size_desc(files)
    return files
