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

from pipeline.utils import get_codes, get_proj_file, get_proj_dir
from pipeline.errors import MissingKerchunkError

def create_allocation(args):
    proj_codes = get_codes(args.groupID, args.workdir, f'proj_codes/{args.repeat_id}')

    time_estms = {}
    others = []

    for p in proj_codes:
        proj_dir = get_proj_dir(p, args.workdir, args.groupID)
        detail = get_proj_file(proj_dir, 'detail-cfg.json')
        if not detail:# or 'skipped' in detail:
            raise MissingKerchunkError(f"Detail file not found for {p} - cannot allocate all proj_codes")
        if args.phase == 'compute':
            # Experimental values for time estimation
            if 'timings' in detail:
                time_estms[p] = 500 + (2.5 + 1.5*detail['timings']['convert_estm'])*detail['num_files']
            elif 'skipped' in detail:
                others.append(p)

    bins = binpacking.to_constant_volume(time_estms, 4*3600)
    for b in bins:
        print(b)
        # Write out as allocations/{label}/bin_0.txt
        # Slurm ID is now the ID of the bin


