"""
deduplicator.py

This module contains the main deduplication pipeline that identifies potential duplicate files.
It uses a multi-stage filtering process based on file size and partial/full hashes.

The stages are:
    - 'fast': size → partial → end → middle
    - 'normal': size → partial → end → middle → first_quarter → third_quarter
    - 'full': size → partial → end → middle → full_hash

Each stage progressively narrows down candidates using more accurate but slower hash strategies.

All file operations use extended file dictionaries like:
{
    "path": "/path/to/file",
    "size": 123456,
    "full_hash": None or b'\x01\x02\x03...'
}
"""
from grouper import (
    group_by_size,
    group_by_partial_hash,
    group_by_end_hash,
    group_by_middle_hash,
    group_by_first_quarter_hash,
    group_by_third_quarter_hash,
    group_by_full_hash,
)
import time
from typing import List, Dict, Any, Tuple, Optional

def find_duplicates(files: List[Dict[str, Any]], mode: str, enable_stats: bool) -> Any:
    """
    Main deduplication pipeline with optional stats tracking.

    Args:
        files: List of file dictionaries containing at least 'path' and 'size'
        mode: Deduplication strategy ('fast', 'normal', or 'full')
        enable_stats: Whether to collect and return statistics

    Returns:
        If enable_stats=True:
            Tuple[List[Dict], Dict, float] — list of duplicates, stats dict, total time
        Else:
            List[Dict] — list of duplicates
    """
    potential_duplicates = []
    
    # Stats setup
    STAGES = ['size', 'partial', 'end', 'middle', 
             'first_quarter', 'third_quarter', 'full']
    stats = {stage: {'groups': 0, 'files': 0, 'time': 0.0} for stage in STAGES}
    total_start_time = time.time() if enable_stats else None
    
    def update_stats(stage: str, duration: float, result: Dict[Any, List[Dict]]):
        """Update stats dictionary with group and file counts for a given stage."""
        if not enable_stats:
            return

        # Filter groups to only those with 2 or more files
        valid_groups = [group for group in result.values() if len(group) >= 2]

        stats[stage]['time'] += duration
        stats[stage]['groups'] += len(valid_groups)
        stats[stage]['files'] += sum(len(files) for files in valid_groups)
    
    # Stage 1: Size grouping
    start = time.time()
    size_groups = group_by_size(files)
    update_stats('size', time.time() - start, size_groups)

    # Process each size group
    for size, same_size_files in size_groups.items():
        if len(same_size_files) < 2:
            continue  # Skip single files

        # Stage 2: Partial hash grouping
        start = time.time()
        partial_hash_group = group_by_partial_hash(same_size_files)
        update_stats('partial', time.time() - start, partial_hash_group)

        for phash, partial_files in partial_hash_group.items():
            if len(partial_files) < 2:
                continue  # Skip non-candidates

            # Stage 3: End hash grouping
            start = time.time()
            end_hash_group = group_by_end_hash(partial_files)
            update_stats('end', time.time() - start, end_hash_group)

            for ehash, end_files in end_hash_group.items():
                if len(end_files) < 2:
                    continue  # Still not enough similarity

                # Stage 4: Middle hash grouping
                start = time.time()
                middle_hash_group = group_by_middle_hash(end_files)
                update_stats('middle', time.time() - start, middle_hash_group)

                for mhash, middle_files in middle_hash_group.items():
                    if len(middle_files) < 2:
                        continue  # Not likely duplicates

                    # Fast mode: Stop here and report as duplicates
                    if mode == 'fast':
                        potential_duplicates.append({
                            "size": size,
                            "files": [f["path"] for f in middle_files]
                        })
                        
                    elif mode == 'normal':
                        # Stage 5: First quarter hash
                        start = time.time()
                        fq_hash_group = group_by_first_quarter_hash(middle_files)
                        update_stats('first_quarter', time.time() - start, fq_hash_group)

                        # Count before filtering
                        total_groups_before = len(fq_hash_group)
                        total_files_before = sum(len(files) for files in fq_hash_group.values())

                        # Keep only valid groups
                        valid_fq_groups = {k: v for k, v in fq_hash_group.items() if len(v) >= 2}
                        total_groups_after = len(valid_fq_groups)
                        total_files_after = sum(len(files) for files in valid_fq_groups.values())

                        # Skip third quarter if no reduction happened
                        if total_groups_before == total_groups_after and total_files_before == total_files_after:
                            # No benefit from first_quarter; skip third_quarter
                            for fqkey, fqpaths in fq_hash_group.items():
                                if len(fqpaths) >= 2:
                                    potential_duplicates.append({
                                        "size": size,
                                        "files": [f["path"] for f in fqpaths]
                                    })
                        else:
                            # Proceed to third_quarter
                            for fqhash, fqpaths in fq_hash_group.items():
                                if len(fqpaths) < 2:
                                    continue

                                # Stage 6: Third quarter hash
                                start = time.time()
                                tq_hash_group = group_by_third_quarter_hash(fqpaths)
                                update_stats('third_quarter', time.time() - start, tq_hash_group)

                                for tqhash, tqpaths in tq_hash_group.items():
                                    if len(tqpaths) >= 2:
                                        potential_duplicates.append({
                                            "size": size,
                                            "files": [f["path"] for f in tqpaths]
                                        })

                    elif mode == 'full':
                        # Stage 5: Full hash grouping
                        start = time.time()
                        full_hash_group = group_by_full_hash(middle_files)
                        update_stats('full', time.time() - start, full_hash_group)

                        for fhash, full_paths in full_hash_group.items():
                            if len(full_paths) >= 2:
                                potential_duplicates.append({
                                    "size": size,
                                    "files": [f["path"] for f in full_paths]
                                })

    if enable_stats:
        total_time = time.time() - total_start_time
        return potential_duplicates, stats, total_time
    return potential_duplicates