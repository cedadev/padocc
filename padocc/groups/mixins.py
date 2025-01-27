__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
import logging
import glob
import json
import binpacking
import datetime

from typing import Union, Optional
from collections.abc import Callable

from padocc.core import (
    FalseLogger
)
from padocc.core.utils import extract_file, times, apply_substitutions, file_configs, format_str

from padocc.core import ProjectOperation

def _deformat_float(item: str) -> str:
    """
    Format byte-value with proper units.
    """
    units = ['','K','M','G','T','P']
    value, suffix = item.split(' ')

    ord = units.index(suffix)*1000
    return float(value)*ord

def _format_float(value: float) -> str:
    """
    Format byte-value with proper units.
    """

    if value is not None:
        unit_index = 0
        units = ['','K','M','G','T','P']
        while value > 1000:
            value = value / 1000
            unit_index += 1
        return f'{value:.2f} {units[unit_index]}B'
    else:
        return None

class InitialisationMixin:
    """
    Mixin container class for initialisation
    routines for groups via input files.
    
    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: GroupOperation [ONLY]
    """

    def init_from_stac(self):
        pass

    def init_from_file(self, input_file: str, substitutions: dict = None):
        """
        Run initialisation by loading configurations from input sources, determine
        input file type and use appropriate functions to instantiate group and project
        directories.
        
        :param input_file:      (str) Path to an input file from which to initialise the project.

        :returns:   None
        """

        substitutions = substitutions or {}

        self.logger.info('Starting initialisation')

        if not input_file:
            if self.groupID:
                self.logger.error('Initialisation requires input file in csv or txt format')
                return

            try:
                manual_config = _get_input(self.logger, self.workdir, forceful=self._forceful)
            except KeyboardInterrupt:
                self.logger.info('Aborting user input process and exiting')
                return
            except Exception as e:
                self.logger.error(f'User Input Error - {e}')
                return

            self._init_project(manual_config)
            return

        if not input_file.startswith('/'):
            pwd = os.getcwd()
            self.logger.info(f'Copying input file from relative path - resolved to {pwd}')
            input_file = os.path.join(pwd, input_file)

        if self.groupID:
            self.logger.debug('Starting group initialisation')
            if '.txt' in input_file:
                self.logger.debug('Converting text file to csv')
                textcontent  = extract_file(input_file)
                group_config = _create_csv_from_text(textcontent)

            elif '.csv' in input_file:
                self.logger.debug('Ingesting csv file')

                group_config = extract_file(input_file)
            self._init_group(group_config, substitutions=substitutions)

        else:
            # Only base-cfg style files are accepted here.
            self.logger.debug('Starting single project initialisation')

            if not input_file.endswith('.json'):
                self.logger.error(
                    'Format of input file not recognised.'
                    ' - single projects must be initialised using a ".json" file.')

            with open(input_file) as f:
                provided_config = json.load(f)
            self._init_project(provided_config)

    def _init_project(self, config: dict):
        """
        Create a first-time ProjectOperation and save created files. 
        """
        default_cfg = file_configs['base_cfg']
        default_cfg.update(config)

        proj_op = ProjectOperation(
            config['proj_code'],
            self.workdir,
            self.groupID,
            first_time = True,
            ft_kwargs=default_cfg,
            logger=self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful,
        )

        proj_op.save_files()

    def _init_group(self, datasets : list, substitutions: dict = None):
        """
        Create a new group within the working directory, and all 
        associated projects.
        """

        self.logger.info('Creating project directories')
        # Group config is the contents of datasets.csv
        if substitutions:
            datasets, status = apply_substitutions('init_file',subs=substitutions, content=datasets)
            if status:
                self.logger.warning(status)

        self.datasets.set(datasets)

        if 'proj_code' in datasets[0]:
            datasets = datasets[1:]
        
        def _open_json(file):
            with open(file) as f:
                return json.load(f)

        proj_codes = []
        for index in range(len(datasets)):
            cfg_values = {}
            ds_values  = datasets[index].split(',')

            proj_code = ds_values[0].replace(' ','')
            pattern   = ds_values[1].replace(' ','')

            if pattern.endswith('.txt') and substitutions:
                pattern, status = apply_substitutions('dataset_file', subs=substitutions, content=[pattern])
                pattern = pattern[0]
                if status:
                    self.logger.warning(status)
            elif pattern.endswith('.csv'):
                pattern = os.path.abspath(pattern)
            else:
                # Dont expand pattern if its not a csv
                pass

            if substitutions:
                cfg_values['substitutions'] = substitutions

            cfg_values['pattern'] = pattern
            proj_codes.append(proj_code)

            if len(ds_values) > 2:
                if os.path.isfile(ds_values[2]):
                    cfg_values['update'] = _open_json(ds_values[2])
                else:
                    cfg_values['update'] = ds_values[2]

            if len(ds_values) > 3:
                if os.path.isfile(ds_values[3]):
                    cfg_values['remove'] = _open_json(ds_values[3])
                else:
                    cfg_values['remove'] = ds_values[3]

            self.logger.info(f'Creating directories/filelists for {index+1}/{len(datasets)}')

            proj_op = ProjectOperation( 
                proj_code, 
                self.workdir,
                groupID=self.groupID,
                logger=self.logger,
                first_time=True,
                ft_kwargs=cfg_values,
                dryrun=self._dryrun,
                forceful=self._forceful,
            )

            proj_op.update_status('init','Success')
            proj_op.save_files()

        self.logger.info(f'Created {len(datasets)*6} files, {len(datasets)*2} directories in group {self.groupID}')
        self._add_proj_codeset('main',proj_codes)
        self.logger.info(f'Written as group ID: {self.groupID}')
        self.save_files()

class ModifiersMixin:
    """
    Modifiers to the group in terms of the projects associated,
    allows adding and removing projects.

    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: GroupOperation [ONLY]
    """

    def add_project(
            self,
            config: dict,
            ):
        """
        Add a project to this group. 
        """
        self._init_project(config)

        self.proj_codes['main'].append(config['proj_code'])

    def remove_project(self, proj_code: str) -> None:
        """
        Remove a project from this group
        Steps required:
        1. Remove the project directory including all internal files.
        2. Remove the project code from all project files.
        """
        for pset in self.proj_codes.values():
            if proj_code in pset:
                pset.remove(proj_code)

        if proj_code in self.datasets:
            self.datasets.pop(proj_code)
        if proj_code in self.faultlist:
            self.faultlist.pop(proj_code)

        proj_op = ProjectOperation(
            proj_code,
            self.workdir,
            groupID=self.groupID,
            forceful=self._forceful,
            dryrun=self._dryrun
        )

        proj_op.delete_project()

    def transfer_project(self, proj_code: str, receiver_group) -> None:
        """
        Transfer an existing project to a new group
        """

        for pset in self.proj_codes.values():
            if proj_code in pset:
                pset.remove(proj_code)

        proj_op = ProjectOperation(
            proj_code,
            self.workdir,
            self.groupID
        )

        proj_op.migrate(receiver_group.groupID)

        receiver_group.proj_codes['main'].append(proj_code)

    def get_stac_representation(
            self, 
            stac_mapping: dict, 
            repeat_id: str = 'main'
        ) -> list:
        """
        Obtain all records for all projects in this group.
        """
        record_set = []
        proj_list = self.proj_codes[repeat_id].get()

        for proj in proj_list:
            proj_op = self[proj]
            record_set.append(
                proj_op.get_stac_representation(stac_mapping)
            )
        return record_set

class EvaluationsMixin:
    """
    Group Mixin for methods to evaluate the status of a group.

    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: GroupOperation [ONLY]
    """

    def _assess_info(self):
        print('Assessment methods:')
        print(' > group.summarise_status() - Summarise the status of all group member projects.')
        print(' > group.summarise_data() - Get a printout summary of data representations in this group')
        print(' > group.repeat_by_status() - Create a new subset group to (re)run an operation, based on the current status')
        print(' > group.progress_display() - Get a human-readable display of progress within the group.')
        print(' > group.progress_repr() - Get a dict version of the progress report (for AirFlow)')

    def get_project(self, proj_code: str):
        """
        Get a project operation from this group
        """

        return ProjectOperation(
            proj_code,
            self.workdir,
            groupID=self.groupID,
            logger=self.logger,
            dryrun=True
        )

    def repeat_by_status(
            self, 
            status: str, 
            new_repeat_id: str, 
            phase: Optional[str] = None,
            old_repeat_id: str = 'main'
        ) -> None:
        """
        Group projects by their status, to then
        create a new repeat ID.
        """
        faultdict = self._get_fault_dict()
        status_dict = self._get_status_dict(
            old_repeat_id,
            faultdict,
            specific_phase=phase,
            specific_error=status
        )

        # New codes are in the status_dict
        new_codes = status_dict[phase][status]
        self._add_proj_codeset(
            new_repeat_id,
            new_codes
        )

        self._save_proj_codes()

    def remove_by_status(
            self, 
            status: str, 
            phase: Optional[str] = None,
            old_repeat_id: str = 'main'
        ) -> None:
        """
        Group projects by their status for
        removal from the group
        """
        faultdict = self._get_fault_dict()
        status_dict = self._get_status_dict(
            old_repeat_id,
            faultdict,
            specific_phase=phase,
            specific_error=status
        )

        for code in status_dict[phase][status]:
            self.remove_project(code)

        self.save_files()
        
    def merge_subsets(
            self,
            subset_list: list[str],
            combined_id: str,
            remove_after: False,
        ) -> None:
        """
        Merge one or more of the subsets previously created
        """
        newset = []

        for subset in subset_list:
            if subset not in self.proj_codes:
                raise ValueError(
                    f'Repeat subset "{subset}" not found in existing subsets.'
                )
            
            newset = newset + self.proj_codes[subset].get()

        self._add_proj_codeset(combined_id, newset)

        if remove_after:
            for subset in subset_list:
                self._delete_proj_codeset(subset)

        self._save_proj_codes()

    def summarise_data(self, repeat_id: str = 'main', func: Callable = print):
        """
        Summarise data stored across all projects, mostly
        concatenating results from the detail-cfg files from
        all projects.
        """
        import numpy as np

        # Cloud Formats and File Types
        # Source Data [Avg,Total]
        # Cloud Data [Avg,Total]
        # File Count [Avg,Total]

        cloud_formats: dict = {}
        file_types: dict = {}

        source_data: list = []
        cloud_data:  list = []
        file_count:  list = []
        
        # Chunk Info
        ## Chunks per file [Avg,Total]
        ## Total Chunks [Avg, Total]

        chunks_per_file: list = []
        total_chunks: list = []

        for proj_code in self.proj_codes[repeat_id]:
            op = ProjectOperation(
                proj_code,
                self.workdir,
                groupID=self.groupID,
                **self.fh_kwargs
            )

            if op.cloud_format in cloud_formats:
                cloud_formats[op.cloud_format] += 1
            else:
                cloud_formats[op.cloud_format] = 1

            if op.file_type in file_types:
                file_types[op.file_type] += 1
            else:
                file_types[op.file_type] = 1

            details = op.detail_cfg.get()

            if 'source_data' in details:
                source_data.append(
                    _deformat_float(details['source_data'])
                )
            if 'cloud_data' in details:
                cloud_data.append(
                    _deformat_float(details['cloud_data'])
                )

            file_count.append(int(details['num_files']))

            chunk_data = details['chunk_info']
            chunks_per_file.append(
                float(chunk_data['chunks_per_file'])
            )
            total_chunks.append(
                int(chunk_data['total_chunks'])
            )

        # Render Outputs
        ot = []
        
        ot.append(f'Summary Report: {self.groupID}')
        ot.append(f'Project Codes: {len(self.proj_codes[repeat_id])}')
        ot.append()
        ot.append(f'Source Files: {sum(file_count)} [Avg. {np.mean(file_count):.2f} per project]')
        ot.append(f'Source Data: {_format_float(sum(source_data))} [Avg. {np.mean(source_data):.2f} per project]')
        ot.append(f'Cloud Data: {_format_float(sum(cloud_data))} [Avg. {np.mean(cloud_data):.2f} per project]')
        ot.append()
        ot.append(f'Cloud Formats: {list(set(cloud_formats))}')
        ot.append(f'File Types: {list(set(file_types))}')
        ot.append()
        ot.append(
            f'Chunks per File: {_format_float(sum(chunks_per_file))} [Avg. {np.mean(chunks_per_file):.2f} per project]')
        ot.append(
            f'Total Chunks: {_format_float(sum(total_chunks))} [Avg. {np.mean(total_chunks):.2f} per project]')
        
        func('\n'.join(ot))

    def summarise_status(
            self, 
            repeat_id, 
            specific_phase: Union[str,None] = None,
            specific_error: Union[str,None] = None,
            long_display: Union[bool,None] = None,
            display_upto: int = 5,
            halt: bool = False,
            write: bool = False,
            fn: Callable = print,
        ) -> None:
        """
        Gives a general overview of progress within the pipeline
        - How many datasets currently at each stage of the pipeline
        - Errors within each pipeline phase
        - Allows for examination of error logs
        - Allows saving codes matching an error type into a new repeat group
        """

        faultdict = self._get_fault_dict()

        status_dict = self._get_status_dict(
            repeat_id, 
            faultdict=faultdict,
            specific_phase=specific_phase,
            specific_error=specific_error,
            halt=halt,
            write=write,
        )

        num_codes  = len(self.proj_codes[repeat_id])
        ot = []
        ot.append('')
        ot.append(f'Group: {self.groupID}')
        ot.append(f'  Total Codes: {num_codes}')
        ot.append()
        ot.append('Pipeline Current:')
        if long_display is None and longest_err > 30:
            longest_err = 30

        for phase, records in status_dict.items():

            if isinstance(records, dict):
                self._summarise_dict(phase, records, num_codes, status_len=longest_err, numbers=display_upto)
            else:
                ot.append()

        ot.append()
        ot.append('Pipeline Complete:')
        ot.append()

        complete = len(status_dict['complete'])

        complete_percent = format_str(f'{complete*100/num_codes:.1f}',4)
        ot.append(f'   complete  : {format_str(complete,5)} [{complete_percent}%]')

        for option, records in faultdict['faultlist'].items():
            self._summarise_dict(option, records, num_codes, status_len=longest_err, numbers=0)

        ot.append()
        fn('\n'.join(ot))

    def _get_fault_dict(self) -> dict:
        """
        Assemble the fault list into a dictionary
        with all reasons.
        """
        extras   = {'faultlist': {}}
        for code, reason in self.faultlist:
            if reason in extras['faultlist']:
                extras['faultlist'][reason].append(0)
            else:
                extras['faultlist'][reason] = [0]
            extras['ignore'][code] = True
        return extras

    def _get_status_dict(
            self,
            repeat_id, 
            faultdict: dict = None,
            specific_phase: Union[str,None] = None,
            specific_error: Union[str,None] = None,
            halt: bool = False,
            write: bool = False,
        ) -> dict:

        """
        Assemble the status dict, can be used for stopping and 
        directly assessing specific errors if needed.
        """

        faultdict = faultdict or {}
        
        proj_codes = self.proj_codes[repeat_id]

        if write:
            self.logger.info(
                'Write permission granted:'
                ' - Will seek status of unknown project codes'
                ' - Will update status with "JobCancelled" for >24hr pending jobs'
            )

        status_dict = {'init':{},'scan': {}, 'compute': {}, 'validate': {},'complete':[]}

        for idx, p in enumerate(proj_codes):
            if p in faultdict['ignore']:
                continue

            status_dict = self._assess_status_of_project(
                p, idx,
                status_dict,
                write=write,
                specific_phase=specific_phase,
                specific_error=specific_error,
                halt=halt
            )
        return status_dict

    def _assess_status_of_project(
            self, 
            proj_code: str, 
            pid: int,
            status_dict: dict,
            write: bool = False,
            specific_phase: Union[str,None] = None,
            specific_error: Union[str,None] = None,
            halt: bool = False,
            ) -> dict:
        """
        Assess the status of a single project
        """

        # Open the specific project
        proj_op = ProjectOperation(
            self.workdir,
            proj_code,
            groupID=self.groupID,
            logger=self.logger
        )

        current = proj_op.get_last_status()
        entry   = current.split(',')

        phase  = entry[0]
        status = entry[1]
        time   = entry[2]

        if len(status) > longest_err:
            longest_err = len(status)

        if status == 'pending' and write:
            timediff = (datetime.now() - datetime(time)).total_seconds()
            if timediff > 86400: # 1 Day - fixed for now
                status = 'JobCancelled'
                proj_op.update_status(phase, 'JobCancelled')
        
        match_phase = (specific_phase == phase)
        match_error = (specific_error == status)

        if bool(specific_phase) != (match_phase) or bool(specific_error) != (match_error):
            total_match = False
        else:
            total_match = match_phase or match_error

        if total_match:
            proj_op.show_log_contents(specific_phase, halt=halt)

        if status == 'complete':
            status_dict['complete'] += 1
        else:
            if status in status_dict[phase]:
                status_dict[phase][status].append(pid)
            else:
                status_dict[phase][status] = [pid]

        return status_dict

    def _summarise_dict(
            self,
            phase: str, 
            records: dict, 
            num_codes: int, 
            status_len: int = 5, 
            numbers: int = 0
        ) -> list:
        """
        Summarise information for a dictionary structure
        that contains a set of errors for a phase within the pipeline
        """
        ot = []

        pcount = len(list(records.keys()))
        num_types = sum([len(records[pop]) for pop in records.keys()])
        if pcount > 0:

            ot.append('')
            fmentry     = format_str(phase,10, concat=False)
            fmnum_types = format_str(num_types,5, concat=False)
            fmcalc      = format_str(f'{num_types*100/num_codes:.1f}',4, concat=False)

            ot.append(f'   {fmentry}: {fmnum_types} [{fmcalc}%] (Variety: {int(pcount)})')

            # Convert from key : len to key : [list]
            errkeys = reversed(sorted(records, key=lambda x:len(records[x])))
            for err in errkeys:
                num_errs = len(records[err])
                if num_errs < numbers:
                    ot.append(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs} (IDs = {list(records[err])})')
                else:
                    ot.append(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs}')
        return ot

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

def _get_updates(
        logger: logging.Logger | FalseLogger = FalseLogger()):
    """
    Get key-value pairs for updating in final metadata.
    """

    logger.debug('Getting update key-pairs')
    inp = None
    valsdict = {}
    while inp != 'exit':
        inp = input('Attribute: ("exit" to escape):')
        if inp != 'exit':
            val = input('Value: ')
            valsdict[inp] = val
    return valsdict

def _get_removals(
        logger: logging.Logger | FalseLogger = FalseLogger()):
    """
    Get attribute names to remove in final metadata.
    """

    logger.debug('Getting removals')
    valsarr = []
    inp = None
    while inp != 'exit':
        inp = input('Attribute: ("exit" to escape):')
        if inp != 'exit':
            valsarr.append(inp)
    return valsarr

def _get_proj_code(path: str, prefix: str = ''):
    """Determine project code from path (prefix removed), appropriate for CMIP6"""
    parts = path.replace(prefix,'').replace('/','_').split('_')
    if '*.' in parts[-1]:
        parts = parts[:-2]
    return '_'.join(parts)

def _create_csv_from_text(text, logger):
    """
    Padocc accepts a text file where the individual entries can be 
    broken down into DOIs for the different projects.
    """
    raise NotImplementedError
    return

    logger.debug('Converting text file to csv')

    if new_inputfile != input_file:
        if self._dryrun:
            self.logger.debug(f'DRYRUN: Skip copying input file {input_file} to {new_inputfile}')
        else:
            os.system(f'cp {input_file} {new_inputfile}')

    with open(new_inputfile) as f:
        datasets = [r.strip() for r in f.readlines()]

    if not os.path.isfile(f'{self.groupdir}/datasets.csv') or self._forceful:
        records = ''
        self.logger.info('Creating filesets for each dataset')
        for index, ds in enumerate(datasets):

            skip = False

            pattern = str(ds)
            if not (pattern.endswith('.nc') or pattern.endswith('.tif')):
                self.logger.debug('Identifying extension')
                fileset = [r.split('.')[-1] for r in glob.glob(f'{pattern}/*')]
                if len(set(fileset)) > 1:
                    self.logger.error(f'File type not specified for {pattern} - found multiple ')
                    skip = True
                elif len(set(fileset)) == 0:
                    skip = True
                else:
                    extension = list(set(fileset))[0]
                    pattern = f'{pattern}/*.{extension}'
                self.logger.debug(f'Found .{extension} common type')

            if not skip:
                proj_op = ProjectOperation(
                    self.workdir, 
                    _get_proj_code(ds, prefix=prefix),
                    groupID = self.groupID)
                
                self.logger.debug(f'Assembled project code: {proj_op}')

                if 'latest' in pattern:
                    pattern = pattern.replace('latest', os.readlink(pattern))

                records  += f'{proj_op},{pattern},,\n'
                self.logger.debug(f'Added entry and created fileset for {index+1}/{len(datasets)}')
        if self._dryrun:
            self.logger.debug(f'DRYRUN: Skip creating csv file {self.groupdir}/datasets.csv')    
        else:        
            with open(f'{self.groupdir}/datasets.csv','w') as f:
                f.write(records)
    else:
        self.logger.warn(f'Using existing csv file at {self.groupdir}/datasets.csv')

def _get_input(
        workdir : str,
        logger  : logging.Logger | FalseLogger = FalseLogger(), 
        forceful : bool = None):
    """
    Get command-line inputs for specific project configuration. 
    Init requires the following parameters: proj_code, pattern/filelist, workdir.
    """

    # Get basic inputs
    logger.debug('Getting user inputs for new project')

    if os.getenv('SLURM_JOB_ID'):
        logger.error('Cannot run input script as Slurm job - aborting')
        return None

    proj_code = input('Project Code: ')
    pattern   = input('Wildcard Pattern: (leave blank if not applicable) ')
    if pattern == '':
        filelist  = input('Path to filelist: ')
        pattern   = None
    else:
        filelist  = None

    if os.getenv('WORKDIR'):
        env_workdir = os.getenv('WORKDIR')

    if workdir and workdir != env_workdir:
        print('Environment workdir does not match provided address')
        print('ENV:',env_workdir)
        print('ARG:',workdir)
        choice = input('Choose to keep the ENV value or overwrite with the ARG value: (E/A) :')
        if choice == 'E':
            pass
        elif choice == 'A':
            os.environ['WORKDIR'] = workdir
            env_workdir = workdir
        else:
            print('Invalid input, exiting')
            return None

    proj_dir = f'{workdir}/in_progress/{proj_code}'
    if os.path.isdir(proj_dir):
        if forceful:
            pass
        else:
            print('Error: Directory already exists -',proj_dir)
            return None
    else:
        os.makedirs(proj_dir)

    config = {
        'proj_code': proj_code,
    }
    do_updates = input('Do you wish to add overrides to metadata values? (y/n): ')
    if do_updates == 'y':
        config['update'] = _get_updates()
    
    do_removals = input('Do you wish to remove known attributes from the metadata? (y/n): ')
    if do_removals == 'y':
        config['remove'] = _get_removals(remove=True)

    if pattern:
        config['pattern'] = pattern

    # Should return input content in a proper format (for a single project.)

    return config


    