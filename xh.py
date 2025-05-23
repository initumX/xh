import os
import sys
import argparse
import time
from typing import List, Dict, Any, Callable, Optional
from collections import defaultdict

from core import (
    scan_directory,
    file_size_pass,
    file_extension_pass,
    sort_files_by_size_desc,
)

from grouper import (
    group_by_size,
    group_by_partial_hash,
    group_by_end_hash,
    group_by_middle_hash,
    group_by_first_quarter_hash,
    group_by_third_quarter_hash,
    group_by_full_hash,
)

import deduplicator


import hasher
"""
hasher provides the following functions:

- `compute_partial_hash(path: str) -> Optional[bytes]`
    Computes hash of the first N bytes of a file.

- `compute_end_hash(path: str) -> Optional[bytes]`
    Computes hash of the last N bytes of a file.

- `compute_middle_hash(path: str) -> Optional[bytes]`
    Computes hash of the central portion of a file.

- `compute_first_quarter_hash(path: str) -> Optional[bytes]`
    Computes hash around the first 25% of a file.

- `compute_third_quarter_hash(path: str) -> Optional[bytes]`
    Computes hash around the last 25% (third quarter) of a file.

- `compute_full_hash(path: str) -> Optional[bytes]`
    Computes full xxHash64 digest of an entire file.
"""

ENABLE_STATS = True  # Set to False to disable all stats

def parse_size(value: str) -> int:
    """
    Converts human-readable size string to bytes.
    Supports suffixes: K (KB), M (MB), G (GB), and allows float values.

    Example:
        '64K' → 65536
        '1.5M' → 1572864
        '0.5K' → 512
        '5G' → 536870912
    """
    units = {
        'B': 1,
        'K': 1024,
        'M': 1024 ** 2,
        'G': 1024 ** 3,
    }

    value = value.strip().upper()
    if not value:
        raise argparse.ArgumentTypeError("Size cannot be empty")

    # Extract unit suffix (if any)
    suffix = None
    number_str = value

    if len(value) > 1 and value[-1] in units:
        suffix = value[-1]
        number_str = value[:-1]
    else:
        suffix = 'B'
        number_str = value

    # Convert to float first, then int
    try:
        number = float(number_str)
        if number <= 0:
            raise argparse.ArgumentTypeError("Size must be greater than zero")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid size value: {value}")

    # Calculate final size in bytes
    multiplier = units[suffix]
    return int(number * multiplier)

def human_readable_size(size_bytes: int) -> str:
    """
    Converts a size in bytes to a human-readable string (with rounding).

    Parameters:
        size_bytes (int): Size in bytes

    Returns:
        str: Human-readable size like '1.00 KB', '2.50 MB', etc.
    """
    if size_bytes < 0:
        return "0 B"

    units = ["B", "KB", "MB", "GB"]
    index = 0

    while size_bytes >= 1024 and index < len(units) - 1:
        size_bytes /= 1024
        index += 1

    return f"{size_bytes:.2f} {units[index]}"


def main():
    """
    Main CLI entrypoint for scanning files and optionally finding duplicates.

    This function:
        1. Parses command-line arguments
        2. Validates directory access
        3. Scans for readable files with optional filters by:
            - Minimum and maximum file size
            - File extensions (including multi-part like .tar.gz)
        4. Optionally detects file duplicates using one of three hash pipelines:
            - Normal mode (--normal): size → partial → end → middle → first_quarter → third_quarter
            - Fast mode (--fast): size → partial → end → middle
            - Full mode (--full): size → partial → end → middle → full_hash

    Parameters:
        -w              If set, performs duplicate detection after filtering
        -s SIZE         Minimum file size (e.g., 64K, 1M)
        --S SIZE        Maximum file size (e.g., 5G)
        -e EXT          Comma-separated list of extensions to include (e.g., txt,pdf,tar.gz)
        --fast          Use fast hash mode (skips quarter-based hashes and full hash)
        --normal        Use normal cascade (default if no mode flag is provided)
        --full          Confirm matches using full xxHash64 digest (slowest, highest accuracy)

    Usage Examples:
        List all .txt files ≥ 64KB:
            python3 xh.py ~/Documents -s 64K -e txt

        Find duplicates among .jpg files ≥ 100KB (using normal mode):
            python3 xh.py ~/Pictures -s 100K -e jpg -w

        Scan all readable files in Downloads folder:
            python3 xh.py ~/Downloads

        Detect duplicates using fast mode:
            python3 xh.py ~/Data --fast -w

        Detect duplicates using full hash confirmation:
            python3 xh.py ~/Data --full -w

    Output:
        - By default: Lists all matching files
        - With -w: Optionally shows potential duplicate groups
        - With ENABLE_STATS = True: Shows detailed timing and group statistics
    """

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        prog="xh",
        description="Recursively scan a directory and optionally find duplicate files",
        epilog="Example: xh ~/Documents -s 64K -S 1M -e txt,csv"
    )

    # Positional argument
    parser.add_argument("directory", help="Directory to scan")

    # Optional filters
    parser.add_argument("-s", type=parse_size,  metavar="SIZE", default="3K",
                        help="Minimum file size (e.g., 64K, 1M, 2.5G)")
    parser.add_argument("-S", type=parse_size, metavar="SIZE", default="300M",
                        help="Maximum file size (e.g., 256K, 5G)")
    parser.add_argument("-e", metavar="EXT", 
                        help="Comma-separated list of extensions to include (no dot prefix needed). Use like: -e txt,csv,tar.gz")


    # Cascade mode selection (mutually exclusive)
    cascade_group = parser.add_mutually_exclusive_group()
    cascade_group.add_argument("--fast", action="store_true", 
                              help="Use fast hash mode (size → partial → end → middle)")
    cascade_group.add_argument("--normal", action="store_true",
                              help="Use normal cascade (size → partial → end → middle → first_quarter → third_quarter)")
    cascade_group.add_argument("--full", action="store_true",
                              help="Use full hash mode (size → partial → end → middle → full)")

    # Additional options
    parser.add_argument("-w", action="store_true",
                        help="Enable duplicate detection")
    


    args = parser.parse_args()

    root_dir = args.directory

    # Validate that the provided path is a valid and accessible directory
    if not os.path.isdir(root_dir):
        print(f"❌ Error: '{root_dir}' is not a valid directory.")
        sys.exit(1)

    # Check read permissions on root directory
    try:
        if not os.access(root_dir, os.R_OK):
            print(f"🔒 Error: No read permissions for '{root_dir}'.")
            sys.exit(1)
    except Exception as e:
        print(f"⚠️ Error checking directory permissions: {e}")
        sys.exit(1)

    # Parse extensions
    extensions = None
    if args.e:
        extensions = [f".{ext.strip()}" if not ext.startswith(".") else ext for ext in args.e.split(",")]
        extensions = list(set(extensions))  # Remove duplicates

    # Print applied filters
    filters = []
    if args.s:
        filters.append(f"Min.size: {human_readable_size(args.s)}")
    if args.S:
        filters.append(f"Max.size: {human_readable_size(args.S)}")
    if extensions:
        filters.append(f"Extensions: {', '.join(extensions)}")

    if filters:
        print(f"🔧 Applied filters: {', '.join(filters)}")

    if args.w:
        print("🛠️ Mode: Find duplicates")

    # Show which duplicate detection mode is used
    if args.fast:
        print("🚀 Cascade: Fast (size → front → end → middle)")
    elif args.full:
        print("🚀 Cascade: Full (size → front → end → middle → full)")
    else:
        print("🚀 Cascade: Normal (size → front → end → middle → first_quarter → third_quarter)")


    # Step 1. Scan directory 
    found_files = scan_directory(
        root_dir, 
        min_size=args.s, 
        max_size=args.S,
        extensions=extensions
    )

    if not found_files:
        print("❌ No files found matching criteria")
        sys.exit(0)


    # Step 2. Optionally detect duplicates
    if args.w:
        # Determine which mode to use (default to normal)
        mode = 'fast' if args.fast else 'full' if args.full else 'normal'
        
        # Import deduplicator logic
        from deduplicator import find_duplicates
        
        # Call deduplication pipeline
        if ENABLE_STATS:
            potential_duplicates, stats, total_time = find_duplicates(
                found_files, 
                mode=mode,
                enable_stats=True
            )
        else:
            potential_duplicates = find_duplicates(
                found_files, 
                mode=mode,
                enable_stats=False
            )
        
        # Output duplicates
        if potential_duplicates:
            print(f"✅ Found {len(potential_duplicates)} group(s) of duplicates:")
            for idx, group in enumerate(potential_duplicates, start=1):
                print(f"\n📁 Group {idx} — Size: {human_readable_size(group['size'])}")
                for path in group["files"]:
                    print(path)
        else:
            print("✅ No duplicates found")
        
        # Final stats output
        if ENABLE_STATS:
            print("\n📊 Statistics:")
            stage_labels = {
                'size': "📁 Size Groups",
                'partial': "📄 Front Hash Groups",
                'end': "🔚 End Hash Groups",
                'middle': "🧠 Middle Hash Groups",
                'first_quarter': "📅 First Quarter Hash Groups",
                'third_quarter': "📆 Third Quarter Hash Groups",
                'full': "🔍 Groups by Hash of Entire File"
            }
            for stage, data in stats.items():
                if data['groups'] > 0 or data['time'] > 0:
                    print(f"{stage_labels[stage]}: {data['groups']} (Total Files: {data['files']}) / Time: {data['time']:.3f}s")
            print(f"\n⏱️ Total Execution Time: {total_time:.3f}s")

if __name__ == "__main__":
    main()