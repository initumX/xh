# deduplicator.py
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

def find_duplicates(files, mode, enable_stats):
    """
    Main deduplication pipeline with optional stats tracking.
    
    Returns:
        tuple: (list of duplicates, stats_dict) if enable_stats=True
        list: only duplicates if enable_stats=False
    """
    potential_duplicates = []
    
    # Stats setup
    STAGES = ['size', 'partial', 'end', 'middle', 
             'first_quarter', 'third_quarter', 'full']
    
    stats = {stage: {'groups': 0, 'files': 0, 'time': 0.0} for stage in STAGES}
    total_start_time = time.time() if enable_stats else None
    
    def update_stats(stage, duration, result):
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
    
    # Process size groups
    for size, paths in size_groups.items():
        if len(paths) < 2:
            continue
            
        # Stage 2: Partial hash
        start = time.time()
        partial_hash_group = group_by_partial_hash(paths)
        update_stats('partial', time.time() - start, partial_hash_group)
        
        for phash, ppaths in partial_hash_group.items():
            if len(ppaths) < 2:
                continue
                
            # Stage 3: End hash
            start = time.time()
            end_hash_group = group_by_end_hash(ppaths)
            update_stats('end', time.time() - start, end_hash_group)
            
            for ehash, epaths in end_hash_group.items():
                if len(epaths) < 2:
                    continue
                    
                # Stage 4: Middle hash
                start = time.time()
                middle_hash_group = group_by_middle_hash(epaths)
                update_stats('middle', time.time() - start, middle_hash_group)
                
                for mhash, mpaths in middle_hash_group.items():
                    if len(mpaths) < 2:
                        continue
                        
                    if mode == 'fast':
                        potential_duplicates.append({
                            "size": size,
                            "files": mpaths
                        })
                        
                    elif mode == 'normal':
                        # Stage 5: First quarter
                        start = time.time()
                        fq_hash_group = group_by_first_quarter_hash(mpaths)
                        update_stats('first_quarter', time.time() - start, fq_hash_group)
                        
                        for fqhash, fqpaths in fq_hash_group.items():
                            if len(fqpaths) < 2:
                                continue
                                
                            # Stage 6: Third quarter
                            start = time.time()
                            tq_hash_group = group_by_third_quarter_hash(fqpaths)
                            update_stats('third_quarter', time.time() - start, tq_hash_group)
                            
                            for tqhash, tqpaths in tq_hash_group.items():
                                if len(tqpaths) >= 2:
                                    potential_duplicates.append({
                                        "size": size,
                                        "files": tqpaths
                                    })
                                    
                    elif mode == 'full':
                        # Stage 5: Full hash
                        start = time.time()
                        full_hash_group = group_by_full_hash(mpaths)
                        update_stats('full', time.time() - start, full_hash_group)
                        
                        for fhash, fpaths in full_hash_group.items():
                            if len(fpaths) >= 2:
                                potential_duplicates.append({
                                    "size": size,
                                    "files": fpaths
                                })
    
    if enable_stats:
        total_time = time.time() - total_start_time
        return potential_duplicates, stats, total_time
    return potential_duplicates