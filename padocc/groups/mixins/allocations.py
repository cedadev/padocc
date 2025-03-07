__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
import json
from typing import Callable, Union

import binpacking

from padocc import ProjectOperation
from padocc.core.filehandlers import ListFileHandler
from padocc.core.utils import BypassSwitch, times, parallel_modes

def get_lotus_reqs(logger):
    """
    Extract Lotus config from filesystem.
    
    Use default if no config supplied.
    """
    refs = {}
    cfg  = os.environ.get('LOTUS_CFG',None)

    if cfg is not None:
        if os.path.isfile(cfg):
            with open(cfg) as f:
                refs = json.load(f)

    if refs.get('lotus_vn',None) == 2:
        logger.info("Using Lotus2 Deployment Specs.")
        return [
            f'#SBATCH --partition={refs.get("partition","standard")}',
            f'#SBATCH --account={refs.get("account","no-project")}',
            f'#SBATCH --qos={refs.get("qos","standard")}'
        ]

    else:
        logger.info("Using Lotus Legacy Deployment Specs.")
        return [
            f'#SBATCH --partition={refs.get("partition","short-serial")}'
        ]

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
        func(' > group.deploy_parallel() - Create sbatch script for submitting to slurm.')

    def create_allocations(
            self,
            phase       : str,
            repeat_id   : str,
            band_increase : Union[str,None] = None,
            binpack     : bool = None,
            **kwargs,
        ) -> list:
        """
        Function for assembling all allocations and bands for packing. 
        
        Allocations contain multiple processes within a single SLURM job such 
        that the estimates for time sum to less than the time allowed for that SLURM job. 
        Bands are single process per job, based on a default time plus any previous 
        attempts (use --allow-band-increase flag to enable band increases with successive 
        attempts if previous jobs timed out)

        :returns:   A list of tuple objects such that each tuple represents an array 
            to submit to slurm with the attributes (label, time, number_of_datasets). 
            Note: The list of datasets to apply in each array job is typcially saved 
            under proj_codes/<repeat_id>/<label>.txt (allocations use allocations/<x>.txt 
            in place of the label)
        """

        proj_codes = self.proj_codes[repeat_id]

        time_estms = {}
        time_defs_value = int(times[phase].split(':')[0])
        time_bands = {}

        for p in proj_codes:
            proj_op = ProjectOperation(p, self.workdir, groupID=self.groupID, dryrun=self._dryrun, **kwargs)
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
            self._create_allocations(bins, repeat_id)
            if len(bins) > 0:
                allocs.append(('allocations','240:00',len(bins)))

        # Create array bands
        self._create_array_bands(time_bands, repeat_id)
            
        if len(time_bands) > 0:
            for b in time_bands:
                allocs.append((f"band_{b}", f'{b}:00', len(time_bands[b])))

        # Return list of tuples.
        return allocs
    
    def deploy_parallel(
            self,
            phase           : str,
            source          : str,
            band_increase   : str = None,
            forceful        : bool = None,
            dryrun          : bool = None,
            thorough        : bool = None,
            verbose         : int = 0,
            binpack         : bool = None,
            time_allowed    : str = None,
            memory          : str = None,
            subset          : int = None,
            repeat_id       : str = 'main',
            bypass          : BypassSwitch = BypassSwitch(),
            mode            : str = 'kerchunk',
            new_version     : str = None,
            func            : Callable = print,
        ) -> None:
        """
        Organise parallel deployment via SLURM.
        """

        source = source or os.environ.get('VIRTUAL_ENV')

        if source is None:
            raise ValueError(
                'Source virtual environment is required.'
            )

        if phase not in parallel_modes:
            raise ValueError(
                f'"{phase}" not recognised, please select from {parallel_modes}'
            )

        sbatch_kwargs = {
            'forceful': forceful or self._forceful,
            'dryrun'  : dryrun or self._dryrun,
            'thorough' : thorough or self._thorough,
            'verbose' : verbose or self._verbose,
            'binpack' : binpack,
            'subset'  : subset,
            'bypass' : bypass,
            'mode' : mode,
            'new_version' : new_version,
        }

        # Ensure directories are created for logs
        self._setup_slurm_directories()
        sbatch_dir  = f'{self.groupdir}/sbatch'

        # Perform allocation assignments here.
        if binpack:
            allocations = self.create_allocations(
                phase, repeat_id,
                band_increase=band_increase, binpack=binpack
            )

            for alloc in allocations:
                func(f'{alloc[0]}: ({alloc[1]}) - {alloc[2]} Jobs')

            for aid, alloc in enumerate(allocations):

                sbatch = ListFileHandler(
                    sbatch_dir, 
                    f'{phase}_{repeat_id}_{aid}.sbatch', 
                    logger=self.logger, 
                    dryrun=self._dryrun, 
                    forceful=self._forceful)
                
                jobname = f'PADOCC_{self.groupID}-{repeat_id}_{aid}_{phase}'

                self._create_slurm_script(
                    phase, 
                    source, 
                    jobname,
                    repeat_id,
                    sbatch, 
                    group_length=alloc[2],
                    sbatch_kwargs=sbatch_kwargs,
                    time=alloc[1]
                )
        else:

            sbatch = ListFileHandler(
                sbatch_dir, 
                f'{phase}_{repeat_id}.sbatch', 
                logger=self.logger, 
                dryrun=self._dryrun, 
                forceful=self._forceful)
            
            jobname = f'PADOCC_{self.groupID}-{repeat_id}_{phase}'

            num_datasets = len(self.proj_codes[repeat_id].get())
            self.logger.info(f'All Datasets: {time_allowed} ({num_datasets})')

            self._create_slurm_script(
                    phase, 
                    source, 
                    jobname,
                    repeat_id,
                    sbatch,
                    group_length=num_datasets,
                    sbatch_kwargs=sbatch_kwargs,
                    time=time_allowed,
                    memory=memory
                )
            
    def _create_slurm_script(
            self,
            phase: str,
            source: str,
            jobname: str,
            repeat_id: str,
            sbatch: ListFileHandler,
            group_length: int,
            sbatch_kwargs: dict,
            time: Union[str,None] = None,
            memory: Union[str,None] = None
        ):
        """
        Create the sbatch content job array.
        
        This content is saved to the sbatch file
        for submission to SLURM.
        """

        time   = time or times[phase]
        memory = memory or '2G'

        outfile = f'{self.groupdir}/outs/{jobname}'
        errfile = f'{self.groupdir}/errs/{jobname}'

        sbatch_flags = self._sbatch_kwargs(time, memory, repeat_id, **sbatch_kwargs)

        lotus_requirements = get_lotus_reqs(self.logger)
  
        sbatch_contents = [
            '#!/bin/bash',
            *lotus_requirements,
            f'#SBATCH --job-name={jobname}',

            f'#SBATCH --time={time}',
            f'#SBATCH --mem={memory}',

            f'#SBATCH -o {outfile}',
            f'#SBATCH -e {errfile}',

            f'source {source}/bin/activate',

            f'export WORKDIR={self.workdir}',

            f'padocc {phase} -p $SLURM_ARRAY_TASK_ID {sbatch_flags}',
        ]

        sbatch.set(sbatch_contents)
        sbatch.close()

        self.logger.info(f'{jobname}: {time} ({group_length})')

        if self._dryrun:
            self.logger.info('DRYRUN: sbatch command: ')
            print(f'sbatch --array=0-{group_length-1} {sbatch.filepath}')
        else:
            os.system(f'sbatch --array=0-{group_length-1} {sbatch.filepath}')

    def _sbatch_kwargs(
            self, 
            time        : str, 
            memory      : str, 
            repeat_id   : str, 
            verbose     : bool = None, 
            bypass      : Union[BypassSwitch,None] = None, 
            subset      : Union[int,None] = None, 
            new_version : bool = None, 
            mode        : Union[str,None] = None, 
            **bool_kwargs
        ) -> str:
        """
        Assemble all flags and options for CLI via SLURM.
        """

        sbatch_kwargs = f'-G {self.groupID} -t {time} -M {memory} -r {repeat_id} '

        bool_options = {
            'forceful' : '-f',
            'thorough' : '-T',
            'dryrun'   : '-d',
            'binpack'  : '-A',
        }

        value_options = {
            'bypass' : ('-b',bypass),
            'subset' : ('-s',subset),
            'mode'   : ('-C',mode),
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
            if bool_kwargs[kwarg]:
                optional.append(bool_options[kwarg])

        return sbatch_kwargs + ' '.join(optional)

    def _setup_slurm_directories(self):
        """
        Create logging directories for this group.
        """

        for dirx in ['sbatch','errs','outs']:
            if not os.path.isdir(f'{self.groupdir}/{dirx}'):
                if self._dryrun:
                    self.logger.debug(f"DRYRUN: Skipped creating {dirx}")
                    continue
                os.makedirs(f'{self.groupdir}/{dirx}')

    def _create_allocations(
            self, 
            bins: list, 
            repeat_id: str
        ) -> None:
        """
        Create allocation files (N project codes to each file) for later job runs.

        :returns: None
        """

        # Ensure directory already exists.
        allocation_path = f'{self.groupdir}/proj_codes/{repeat_id}/allocations'
        if not os.path.isdir(allocation_path):
            if not self._dryrun:
                os.makedirs(allocation_path)
            else:
                print(f'Making directories: {allocation_path}')

        for idx, b in enumerate(bins):
            bset = b.keys()
            if not self._dryrun:
                # Create a file for each allocation
                os.system(f'touch {allocation_path}/{idx}.txt')
                with open(f'{allocation_path}/{idx}.txt','w') as f:
                    f.write('\n'.join(bset))
            else:
                print(f'Writing {len(bset)} to file {idx}.txt')

    def _create_array_bands(
            self, 
            bands: list, 
            repeat_id: str
        ) -> None:
        """
        Create band-files (under repeat_id) for this set of datasets.

        :returns: None
        """
        # Ensure band directory exists
        bands_path = f'{self.groupdir}/proj_codes/{repeat_id}/'
        if not os.path.isdir(bands_path):
            if not self._dryrun:
                os.makedirs(bands_path)
            else:
                print(f'Making directories: {bands_path}')

        for b in bands:
            if not self._dryrun:
                # Export proj codes to correct band file
                os.system(f'touch {bands_path}/band_{b}.txt')
                with open(f'{bands_path}/band_{b}.txt','w') as f:
                        f.write('\n'.join(bands[b]))
            else:
                print(f'Writing {len(bands[b])} to file band_{b}.txt')