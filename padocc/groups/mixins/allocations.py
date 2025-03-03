__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
from typing import Callable

import binpacking

from padocc import ProjectOperation
from padocc.core.filehandlers import ListFileHandler
from padocc.core.utils import BypassSwitch, times
from padocc.phases import KNOWN_PHASES


class AllocationsMixin:
    """
    Enables the use of Allocations for job deployments via Slurm.

    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: GroupOperation [ONLY]
    """

    @classmethod
    def help(cls, func: Callable = print):
        func('Allocations:')
        func(' > group.create_allocations() - Create a set of allocations, returns a binned list of bands')
        func(' > group.create_sbatch() - Create sbatch script for submitting to slurm.')

    def create_allocations(
            self,
            phase,
            repeat_id,
            band_increase=None,
            binpack=None,
            **kwargs,
        ) -> list:
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

        proj_codes = self.proj_codes[repeat_id]

        time_estms = {}
        time_defs_value = int(times[phase].split(':')[0])
        time_bands = {}

        for p in proj_codes:
            proj_op = ProjectOperation(p, self.workdir, groupID=self.groupID, dryrun=self.dryrun, **kwargs)
            lr      = proj_op.base_cfg['last_run']
            timings = proj_op.detail_cfg['timings']
            nfiles  = proj_op.detail_cfg['num_files']

            # Determine last run if present for this job
            
            if 'concat_estm' in timings and phase == 'compute':
                # Calculate time estimation (minutes) - experimentally derived equation
                time_estms[p] = (500 + (2.5 + 1.5*timings['convert_estm'])*nfiles)/60 # Changed units to minutes for allocation
            else:
                # Increase from previous job run if band increase allowed (previous jobs ran out of time)
                if lr[0] == phase and band_increase:
                    try:
                        next_band = int(lr[1].split(':')[0]) + time_defs_value
                    except IndexError:
                        next_band = time_defs_value*2
                else:
                    # Use default if no prior info found.
                    next_band = time_defs_value

                # Thorough/Quality validation - special case.
                #if 'quality_required' in detail and phase == 'validate':
                    #if detail['quality_required']:
                        # Hardcoded quality time 2 hours
                        #next_band = max(next_band, 120) # Min 2 hours

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
            _create_allocations(self.groupID, self.workdir, bins, repeat_id, dryrun=self.dryrun)
            if len(bins) > 0:
                allocs.append(('allocations','240:00',len(bins)))

        # Create array bands
        _create_array_bands(self.groupID, self.workdir, time_bands, repeat_id, dryrun=self.dryrun)
            
        if len(time_bands) > 0:
            for b in time_bands:
                allocs.append((f"band_{b}", f'{b}:00', len(time_bands[b])))

        # Return list of tuples.
        return allocs
    
    def create_sbatch(
            self,
            phase     : str,
            source    : str = None,
            venvpath  : str = None,
            band_increase : str = None,
            forceful   : bool = None,
            dryrun     : bool = None,
            quality    : bool = None,
            verbose    : int = 0,
            binpack    : bool = None,
            time_allowed : str = None,
            memory       : str = None,
            subset       : int = None,
            repeat_id    : str = 'main',
            bypass       : BypassSwitch = BypassSwitch(),
            mode         : str = 'kerchunk',
            new_version  : str = None,
        ) -> None:

        if phase not in KNOWN_PHASES:
            raise ValueError(
                f'"{phase}" not recognised, please select from {KNOWN_PHASES}'
            )
            return None

        array_job_kwargs = {
            'forceful': forceful,
            'dryrun'  : dryrun,
            'quality' : quality,
            'verbose' : verbose,
            'binpack' : binpack,
            'time_allowed' : time_allowed,
            'memory'  : memory,
            'subset'  : subset,
            'repeat_id' : repeat_id,
            'bypass' : bypass,
            'mode' : mode,
            'new_version' : new_version,
        }

        # Perform allocation assignments here.
        if not time_allowed:
            allocations = self.create_allocations(
                phase, repeat_id,
                band_increase=band_increase, binpack=binpack
            )

            for alloc in allocations:
                print(f'{alloc[0]}: ({alloc[1]}) - {alloc[2]} Jobs')

            deploy = input('Deploy the above allocated dataset jobs with these timings? (Y/N) ')
            if deploy != 'Y':
                raise KeyboardInterrupt

            for alloc in allocations:
                self._create_job_array(
                    phase, source, venvpath, alloc[2]
                    **array_job_kwargs,
                )
        else:
            num_datasets = len(self.proj_codes[repeat_id].get())
            self.logger.info(f'All Datasets: {time_allowed} ({num_datasets})')

            # Always check before deploying a significant number of jobs.
            deploy = input('Deploy the above allocated dataset jobs with these timings? (Y/N) ')
            if deploy != 'Y':
                raise KeyboardInterrupt

            self._create_job_array(
                    phase, source, venvpath, num_datasets,
                    **array_job_kwargs,
                )

    def _create_job_array(
            self,
            phase,
            source,
            venvpath,
            group_length=None,
            repeat_id='main',
            forceful=None,
            verbose=None,
            dryrun=None,
            quality=None,
            bypass=None,
            binpack=None,
            time_allowed=None,
            memory=None,
            subset=None,
            mode=None,
            new_version=None,
            time=None,
            joblabel=None,
        ):

        sbatch_dir = f'{self.dir}/sbatch/'
        if not joblabel:
            sbatch_file = f'{phase}.sbatch'
        else:
            sbatch_file = f'{phase}_{joblabel}.sbatch'
            repeat_id = f'{repeat_id}/{joblabel}'

        sbatch = ListFileHandler(sbatch_dir, sbatch_file, self.logger, dryrun=self._dryrun, forceful=self._forceful)

        master_script = f'{source}/single_run.py'

        if time is None:
            time = time_allowed or times[phase]
        mem = '2G' or memory

        jobname = f'PADOCC_{self.groupID}_{phase}'
        if joblabel:
            jobname = f'PADOCC_{joblabel}_{phase}_{self.groupID}'

        outfile = f'{self.dir}/outs/{jobname}_{repeat_id}'
        errfile = f'{self.dir}/errs/{jobname}_{repeat_id}'

        sbatch_kwargs = self._sbatch_kwargs(
            time,
            memory,
            repeat_id,
            bypass=bypass, 
            forceful= forceful or self._forceful, 
            verbose = verbose or self._verbose,
            quality = quality or self._quality, # Check
            dryrun = dryrun or self._dryrun,
            binpack = binpack,
            subset = subset,
            new_version = new_version,
            mode = mode,
        )
        
        sbatch_contents = [
            '#!/bin/bash',
            '#SBATCH --partition=short-serial',
            f'#SBATCH --job-name={jobname}',

            f'#SBATCH --time={time}',
            f'#SBATCH --mem={mem}',

            f'#SBATCH -o {outfile}',
            f'#SBATCH -e {errfile}',

            f'module add jaspy',
            f'source {venvpath}/bin/activate',

            f'export WORKDIR={self.workdir}',

            f'python {master_script} {phase} $SLURM_ARRAY_TASK_ID {sbatch_kwargs}',
        ]

        sbatch.update(sbatch_contents)
        sbatch.close()

        if self._dryrun:
            self.logger.info('DRYRUN: sbatch command: ')
            print(f'sbatch --array=0-{group_length-1} {sbatch.filepath()}')

    def _sbatch_kwargs(
            self, time, memory, repeat_id, verbose=None, bypass=None, 
            subset=None, new_version=None, mode=None, **bool_kwargs):
        sbatch_kwargs = f'-G {self.groupID} -t {time} -M {memory} -r {repeat_id}'

        bool_options = {
            'forceful' : '-f',
            'quality'  : '-Q',
            'dryrun'   : '-d',
            'binpack'  : '-A',
        }

        value_options = {
            'bypass' : ('-b',bypass),
            'subset' : ('-s',subset),
            'mode'   : ('-m',mode),
            'new_version': ('-n',new_version),
        }

        optional = []

        if verbose is not None:
            verb = 'v' * int(verbose)
            optional.append(f'-{verb}')

        for value in value_options.keys():
            if value_options[value][1] is not None:
                optional.append(' '.join(value_options[value]))

        for kwarg in bool_kwargs.keys():
            if kwarg not in bool_options:
                raise ValueError(
                    f'"{kwarg}" option not recognised - '
                    f'please choose from {list(bool_kwargs.keys())}'
                )
            optional.append(bool_options[kwarg])

        return sbatch_kwargs + ' '.join(optional)

    def _setup_slurm_directories(self):
        """
        Currently Unused function to set up 
        the slurm directories for a group."""

        for dirx in ['sbatch','errs']:
            if not os.path.isdir(f'{self.groupdir}/{dirx}'):
                if self._dryrun:
                    self.logger.debug(f"DRYRUN: Skipped creating {dirx}")
                    continue
                os.makedirs(f'{self.dir}/{dirx}')

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