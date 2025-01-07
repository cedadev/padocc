__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
import logging
import glob
import json
import binpacking

from padocc.core import (
    FalseLogger,
    LoggedOperation
)
from padocc.core.utils import extract_file, times, apply_substitutions, file_configs

from padocc.core.project import ProjectOperation

class InitialisationMixin:
    """
    Mixin container class for initialisation
    routines for groups via input files."""

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

    def add_project(self):
        """
        Add a project to this group
        """
        pass

    def remove_project(self):
        """
        Remove a project from this group
        """
        pass

"""
Replacement for assessor tool. Requires the following (public) methods:
 - progress (progress_check)
 - blacklist
 - upgrade (upgrade_version)
 - summarise (summary_data)
 - display (show_options)
 - cleanup (cleanup) - May not need since this is built into the group.
 - match ?
 - status (status_log)
 - allocations (assess_allocation)

 
Private methods suspected:
 - _get_rerun_command : To get a specific rerun for a dataset.
 - _merge_old_new     : Combine sets of project codes.
 - _save_project_codes : Depends how the group stuff works if we need this
 _ _analyse_data      : Connect to project codes and get a summary of each.
 - _force_datetime_decode : Decode datetimes.
"""

class EvaluationsMixin:
    """
    Group Mixin for methods to evaluate the status of a group.
    """
    def _assess_info(self):
        print('Assessment methods:')
        print(' > group.summary_data() - Get a printout summary of data representations in this group')
        print(' > group.remove_projects() - Remove projects fitting some parameters from this group')
        print(' > group.progress_display() - Get a human-readable display of progress within the group.')
        print(' > group.progress_repr() - Get a dict version of the progress report (for AirFlow)')

    def get_project(self, proj_code: str):
        return ProjectOperation(
            proj_code,
            self.workdir,
            groupID=self.groupID,
            logger=self.logger,
            dryrun=True
        )
        

    def summary_data(self):
        """
        Summarise data stored across all files
        """
        pass

    def remove_projects(self):
        """
        Delete a set of projects which match some criteria.
        """
        pass

    def progress(self, repeat_id, write=True):
        """Give a general overview of progress within the pipeline
        - How many datasets currently at each stage of the pipeline
        - Errors within each pipeline phase
        - Allows for examination of error logs
        - Allows saving codes matching an error type into a new repeat group
        """
        blacklist  = self.blacklist_codes
        proj_codes = self.proj_codes[repeat_id]

        if write:
            self.logger.info(
                'Write permission granted:'
                ' - Will seek status of unknown project codes'
                ' - Will update status with "JobCancelled" for >24hr pending jobs'
            )

        done_set = {}
        extras = {'blacklist': {}}
        complete = 0

        # Summarising the blacklist reasons
        for code, reason in blacklist:
            if reason in extras['blacklist']:
                extras['blacklist'][reason].append(0)
            else:
                extras['blacklist'][reason] = [0]
            done_set[code] = True

        phases = {'init':{}, 'scan': {}, 'compute': {}, 'validate': {}}
        savecodes = []
        longest_err = 0
        for idx, p in enumerate(proj_codes):

            proj_op = ProjectOperation(
                self.workdir,
                p,
                groupID=self.groupID,
                logger=self.logger
            )

            try:
                if p not in done_set:
                    proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{p}'
                    current = get_log_status(proj_dir)
                    if not current:
                        seek_unknown(proj_dir)
                        if 'unknown' in extras:
                            extras['unknown']['no data'].append(idx)
                        else:
                            extras['unknown'] = {'no data':[idx]}
                        continue
                    entry = current.split(',')
                    if len(entry[1]) > longest_err:
                        longest_err = len(entry[1])

                    if entry[1] == 'pending' and args.write:
                        timediff = (datetime.now() - force_datetime_decode(entry[2])).total_seconds()
                        if timediff > 86400: # 1 Day - fixed for now
                            entry[1] = 'JobCancelled'
                            log_status(entry[0], proj_dir, entry[1], FalseLogger())
                    
                    match_phase = (bool(args.phase) and args.phase == entry[0])
                    match_error = (bool(args.error) and any([err == entry[1].split(' ')[0] for err in args.error]))

                    if bool(args.phase) != (args.phase == entry[0]):
                        total_match = False
                    elif bool(args.error) != (any([err == entry[1].split(' ')[0] for err in args.error])):
                        total_match = False
                    else:
                        total_match = match_phase or match_error

                    if total_match:
                        if args.examine:
                            examine_log(args.workdir, p, entry[0], groupID=args.groupID, repeat_id=args.repeat_id, error=entry[1])
                        if args.new_id or args.blacklist:
                            savecodes.append(p)

                    merge_errs = True # Debug - add as argument later?
                    if merge_errs:
                        err_type = entry[1].split(' ')[0]
                    else:
                        err_type = entry[1]

                    if entry[0] == 'complete':
                        complete += 1
                    else:
                        if err_type in phases[entry[0]]:
                            phases[entry[0]][err_type].append(idx)
                        else:
                            phases[entry[0]][err_type] = [idx]
            except KeyboardInterrupt as err:
                raise err
            except Exception as err:
                examine_log(args.workdir, p, entry[0], groupID=args.groupID, repeat_id=args.repeat_id, error=entry[1])
                print(f'Issue with analysis of error log: {p}')
        num_codes  = len(proj_codes)
        print()
        print(f'Group: {args.groupID}')
        print(f'  Total Codes: {num_codes}')

        def summary_dict(pdict, num_codes, status_len=5, numbers=0):
            """Display summary information for a dictionary structure of the expected format."""
            for entry in pdict.keys():
                pcount = len(list(pdict[entry].keys()))
                num_types = sum([len(pdict[entry][pop]) for pop in pdict[entry].keys()])
                if pcount > 0:
                    print()
                    fmentry = format_str(entry,10, concat=False)
                    fmnum_types = format_str(num_types,5, concat=False)
                    fmcalc = format_str(f'{num_types*100/num_codes:.1f}',4, concat=False)
                    print(f'   {fmentry}: {fmnum_types} [{fmcalc}%] (Variety: {int(pcount)})')

                    # Convert from key : len to key : [list]
                    errkeys = reversed(sorted(pdict[entry], key=lambda x:len(pdict[entry][x])))
                    for err in errkeys:
                        num_errs = len(pdict[entry][err])
                        if num_errs < numbers:
                            print(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs} (IDs = {list(pdict[entry][err])})')
                        else:
                            print(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs}')
        if not args.new_id:
            print()
            print('Pipeline Current:')
            if not args.long and longest_err > 30:
                longest_err = 30
            summary_dict(phases, num_codes, status_len=longest_err, numbers=int(args.numbers))
            print()
            print('Pipeline Complete:')
            print()
            complete_percent = format_str(f'{complete*100/num_codes:.1f}',4)
            print(f'   complete  : {format_str(complete,5)} [{complete_percent}%]')
            summary_dict(extras, num_codes, status_len=longest_err, numbers=0)
            print()

        if args.new_id:
            logger.debug(f'Preparing to write {len(savecodes)} codes to proj_codes/{args.new_id}.txt')
            if args.write:
                save_selection(savecodes, groupdir, args.new_id, logger, overwrite=args.overwrite)
            else:
                print('Skipped writing new codes - Write flag not present')

        if args.blacklist:
            logger.debug(f'Preparing to add {len(savecodes)} codes to the blacklist')
            if args.write:
                add_to_blacklist(savecodes, args.groupdir, args.reason, logger)
            else:
                print('Skipped blacklisting codes - Write flag not present')

    def get_operation(self, opt):
        """Operation to perform - deprecated"""
        if hasattr(self, opt):
            try:
                getattr(self, opt)()
            except TypeError as err:
                self.logger.error(
                    f'Attribute "{opt}" is not callable'
                )
                raise err
            except KeyboardInterrupt as err:
                raise err
            except Exception as err:
                examine_log(args.workdir, p, entry[0], groupID=args.groupID, repeat_id=args.repeat_id, error=entry[1])
                print(f'Issue with analysis of error log: {p}')
        else:
            self.logger.error(
                'Unrecognised operation type for EvaluationOperation.')

class AllocationsMixin:

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


    