__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

# Job Subset Allocation Script
# - Calculate Expected Utilisation for each Job (500 + Kerchunk Size*5 MB)

# First-Fit Bin Packing Algorithm
# - Sort Utilisations from largest to smallest
# - Bin Capacity is largest size rounded up to next memory cap (1, 2, 3, 4 GB)
# - Allocate item to first bin with space remaining
# - End with N bins (subsets) - write list of project codes for each subset to a separate file in proj_code_subsets/set_N.txt
# - Run array with number of subsets already set.

import binpacking
import os

from padocc.core.utils import times

from padocc.operations import GroupOperation, ProjectOperation

def assemble_allocations(
        workdir, 
        groupID, 
        phase, 
        repeat_id='main', 
        band_increase=None,
        binpack=None,
        dryrun=None,
        **kwargs):
    """
    Function for assembling all allocations and bands for packing. Allocations contain multiple processes within
    a single SLURM job such that the estimates for time sum to less than the time allowed for that SLURM job. Bands
    are single process per job, based on a default time plus any previous attempts (use --allow-band-increase flag
    to enable band increases with successive attempts if previous jobs timed out)

    :returns:   A list of tuple objects such that each tuple represents an array to submit to slurm with
                the attributes (label, time, number_of_datasets). Note: The list of datasets to apply in
                each array job is typcially saved under proj_codes/<repeat_id>/<label>.txt (allocations use
                allocations/<x>.txt in place of the label)
    """

    group = GroupOperation(workdir, groupID=groupID, dryrun=dryrun, **kwargs)
    proj_codes = group.proj_codes[repeat_id]

    time_estms = {}
    time_defs_value = int(times[phase].split(':')[0])
    time_bands = {}

    for p in proj_codes:
        proj_op = ProjectOperation(p, workdir, groupID=groupID, dryrun=dryrun, **kwargs)
        detail  = proj_op.detail_cfg.get()

        # Determine last run if present for this job
        lr = [None, None]
        if 'last_run' in detail:
            lr = detail['last_run']
        
        if _has_required_timings(detail) and phase == 'compute':
            # Calculate time estimation (minutes) - experimentally derived equation
            time_estms[p] = (500 + (2.5 + 1.5*detail['timings']['convert_estm'])*detail['num_files'])/60 # Changed units to minutes for allocation
        else:
            # Increase from previous job run if band increase allowed (previous jobs ran out of time)
            if lr[0] == phase and band_increase:
                try:
                    next_band = int(lr[1].split(':')[0]) + time_defs_value
                except:
                    next_band = time_defs_value*2
            else:
                # Use default if no prior info found.
                next_band = time_defs_value

            # Thorough/Quality validation - special case.
            if 'quality_required' in detail and phase == 'validate':
                if detail['quality_required']:
                    # Hardcoded quality time 2 hours
                    next_band = max(next_band, 120) # Min 2 hours

            # Save code to specific band
            if next_band in time_bands:
                time_bands[next_band].append(p)
            else:
                time_bands[next_band] = [p]

    if len(time_estms) > 5 and binpack:
        binsize = int(max(time_estms.values())*1.4/600)*600
        bins = binpacking.to_constant_volume(time_estms, binsize) # Rounded to 10 mins
    else:
        # Unpack time_estms into established bands
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
        _create_allocations(groupID, workdir, bins, repeat_id, dryrun=dryrun)
        if len(bins) > 0:
            allocs.append(('allocations','240:00',len(bins)))

    # Create array bands
    _create_array_bands(groupID, workdir, time_bands, repeat_id, dryrun=dryrun)
        
    if len(time_bands) > 0:
        for b in time_bands:
            allocs.append((f"band_{b}", f'{b}:00', len(time_bands[b])))

    # Return list of tuples.
    return allocs

def _has_required_timings(detail) -> bool:
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

def _create_allocations(groupID: str, workdir: str, bins: list, repeat_id: str, dryrun=False) -> None:
    """
    Create allocation files (N project codes to each file) for later job runs.

    :returns: None
    """

    # Ensure directory already exists.
    allocation_path = f'{workdir}/groups/{groupID}/proj_codes/{repeat_id}/allocations'
    if not os.path.isdir(allocation_path):
        if not dryrun:
            os.makedirs(allocation_path)
        else:
            print(f'Making directories: {allocation_path}')

    for idx, b in enumerate(bins):
        bset = b.keys()
        if not dryrun:
            # Create a file for each allocation
            os.system(f'touch {allocation_path}/{idx}.txt')
            with open(f'{allocation_path}/{idx}.txt','w') as f:
                f.write('\n'.join(bset))
        else:
            print(f'Writing {len(bset)} to file {idx}.txt')

def _create_array_bands(groupID, workdir, bands, repeat_id, dryrun=False):
    """
    Create band-files (under repeat_id) for this set of datasets.

    :returns: None
    """
    # Ensure band directory exists
    bands_path = f'{workdir}/groups/{groupID}/proj_codes/{repeat_id}/'
    if not os.path.isdir(bands_path):
        if not dryrun:
            os.makedirs(bands_path)
        else:
            print(f'Making directories: {bands_path}')

    for b in bands:
        if not dryrun:
            # Export proj codes to correct band file
            os.system(f'touch {bands_path}/band_{b}.txt')
            with open(f'{bands_path}/band_{b}.txt','w') as f:
                    f.write('\n'.join(bands[b]))
        else:
            print(f'Writing {len(bands[b])} to file band_{b}.txt')
