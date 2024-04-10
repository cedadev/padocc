__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

# Job Subset Allocation Script
# - Calculate Expected Utilisation for each Job (500 + Kerchunk Size*5 MB)

# First-Fit Bin Packing Algorithm
# - Sort Utilisations from largest to smallest
# - Bin Capacity is largest size rounded up to next memory cap (1, 2, 3, 4 GB)
# - Allocate item to first bin with space remaining
# - End with N bins (subsets) - write list of project codes for each subset to a separate file in proj_code_subsets/set_N.txt
# - Run array with number of subsets already set.

# Utilisation estimate is (total_chunks * 835) + 500 (MB)
"""
for proj_code in (repeat_id set):
    open detail-cfg (for this code)
    calculate utilisation
    add to dict [utilisation, proj_code]
    keep track of max/min
get bins using binpacking (pypi)
"""
import binpacking
import os

from pipeline.utils import get_codes, get_proj_file, get_proj_dir, times
from pipeline.errors import MissingKerchunkError
from pipeline.logs import get_log_status

def has_required_timings(detail) -> bool:
    """
    Check if the contents of this projects 'detail' dict has required timing
    estimates for allocation.

    :returns:   True or False depending on the above condition.
    """
    if 'timings' not in detail:
        return False
    if 'concat_estm' not in detail['timings']:
        return False
    return True

def assemble_allocations(args):
    proj_codes = get_codes(args.groupID, args.workdir, f'proj_codes/{args.repeat_id}')

    time_estms = {}
    time_defs_value = int(times[args.phase].split(':')[0])*2
    time_bands = {}

    for p in proj_codes:
        proj_dir       = get_proj_dir(p, args.workdir, args.groupID)
        detail         = get_proj_file(proj_dir, 'detail-cfg.json')

        lr = [None, None]
        if 'last_run' in detail:
            lr = detail['last_run']
        
        # Experimental values for time estimation
        if has_required_timings(detail) and args.phase == 'compute':
            time_estms[p] = 500 + (2.5 + 1.5*detail['timings']['convert_estm'])*detail['num_files']
        else:
            if lr[0] == args.phase and args.band_increase:
                next_band = int(lr[1].split(':')[0]) + time_defs_value
            else:
                next_band = time_defs_value

            if next_band in time_bands:
                time_bands[next_band].append(p)
            else:
                time_bands[next_band] = [p]

    if len(time_estms) > 5 and args.binpack:
        binsize = int(max(time_estms.values())*1.4/600)*600
        bins = binpacking.to_constant_volume(time_estms, binsize) # Rounded to 10 mins
    else:
        print('Skipped Job Allocations - using Bands-only.')
        bins = None
        for pc in time_estms.keys():
            time_estm = time_estms[pc]/60
            applied = False
            for tb in time_bands.keys():
                if time_estm < tb:
                    time_bands[tb].append(pc)
                    applied = True
                    break
            if not applied:
                next_band = time_defs_value
                i = 2
                while next_band < time_estm:
                    next_band = time_defs_value*i
                    i += 1
                time_bands[next_band] = [pc]

    allocs = []
    # Create allocations
    if bins:
        create_allocations(args.groupID, args.workdir, bins, args.repeat_id, dryrun=args.dryrun)
        if len(bins) > 0:
            allocs.append(('allocations','240:00',len(bins)))
    # Create array bands
    create_array_bands(args.groupID, args.workdir, time_bands, args.repeat_id, dryrun=args.dryrun)
        
    # Return a list of tuples: [ ('allocations', '240:00', 116), ...] 

    if len(time_bands) > 0:
        for b in time_bands:
            allocs.append((f"band_{b}", f'{b}:00', len(time_bands[b])))

    return allocs

def create_allocations(groupID, workdir, bins, repeat_id, dryrun=False) -> None:
    allocation_path = f'{workdir}/groups/{groupID}/proj_codes/{repeat_id}/allocations'
    if not os.path.isdir(allocation_path):
        if not dryrun:
            os.makedirs(allocation_path)
        else:
            print(f'Making directories: {allocation_path}')

    for idx, b in enumerate(bins):
        bset = b.keys()
        if not dryrun:
            os.system(f'touch {allocation_path}/{idx}.txt')
            with open(f'{allocation_path}/{idx}.txt','w') as f:
                f.write('\n'.join(bset))
        else:
            print(f'Writing {len(bset)} to file {idx}.txt')

def create_array_bands(groupID, workdir, bands, repeat_id, dryrun=False):
    bands_path = f'{workdir}/groups/{groupID}/proj_codes/{repeat_id}/'
    if not os.path.isdir(bands_path):
        if not dryrun:
            os.makedirs(bands_path)

    for b in bands:
        if not dryrun:
            os.system(f'touch {bands_path}/band_{b}.txt')
            with open(f'{bands_path}/band_{b}.txt','w') as f:
                    f.write('\n'.join(bands[b]))
        else:
            print(f'Writing {len(bands[b])} to file band_{b}.txt')
