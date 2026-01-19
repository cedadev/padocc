__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import glob
import logging
import os
from typing import Callable, Union

import yaml
import json

from .errors import error_handler
from .filehandlers import (CSVFileHandler, JSONFileHandler, ListFileHandler,
                           LogFileHandler, KerchunkFile)
from .mixins import (DatasetHandlerMixin, DirectoryMixin, PropertiesMixin,
                     StatusMixin)
from .utils import (source_opts, BypassSwitch, apply_substitutions,
                    extract_file, file_configs, phases, print_fmt_str,
                    extract_json)

class ProjectOperation(
    DirectoryMixin, 
    DatasetHandlerMixin,
    StatusMixin,
    PropertiesMixin):
    """
    PADOCC Project Operation class.
    
    Able to access project files
    and perform some simple functions. Single-project operations
    always inherit from this class (e.g. Scan, Compute, Validate)
    """

    def __init__(
            self, 
            proj_code : str, 
            workdir   : str,
            groupID   : str = None, 
            first_time : bool = None,
            ft_kwargs  : dict = None,
            logger     : logging.Logger = None,
            bypass     : BypassSwitch = BypassSwitch(),
            label      : str = None,
            fh         : str = None,
            logid      : str = None,
            verbose    : int = 0,
            forceful   : bool = None,
            dryrun     : bool = None,
            thorough   : bool = None,
            diagnostic : bool = False,
            mem_allowed: Union[str,None] = None,
            remote_s3  : Union[dict, str, None] = None,
            xarray_kwargs: dict = None,
            new_version: bool = False,
        ) -> None:
        """
        Initialisation for a ProjectOperation object to handle all interactions
        with a single project. 

        :param proj_code:       (str) The project code in string format (DOI)

        :param workdir:         (str) Path to the current working directory.

        :param groupID:         (str) Name of current dataset group.

        :param first_time:      (bool) Activate for first-time setup of new project (creates files
            instead of reading empty ones), this is automatically activated when using the Group Init
            function.

        :param ft_kwargs:       (dict) Arguments provided on first time setup of the new project, stored
            in the project base config.

        :param logger:              (logging.Logger) Logger supplied to this Operation.
                                    
        :param bypass:              (BypassSwitch) instance of BypassSwitch class containing multiple
            bypass/skip options for specific events. See utils.BypassSwitch.

        :param label:               (str) The label to apply to the logger object.

        :param fh:                  (str) Path to logfile for logger object generated in this specific process.

        :param logid:               (str) ID of the process within a subset, which is then added to the name
            of the logger - prevents multiple processes with different logfiles getting
            loggers confused.

        :param verbose:         (int) Level of verbosity for log messages (see core.init_logger).

        :param forceful:        (bool) Continue with processing even if final output file 
            already exists.

        :param dryrun:          (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.

        :param thorough:        (bool) From args.quality - if True will create all files 
            from scratch, otherwise saved refs from previous runs will be loaded.

        :param remote_s3:       (dict | str) Path to config file or dict containing remote s3
            configurations.

        :returns: None

        """

        self.proj_code = proj_code

        self.mem_allowed = mem_allowed
        self._allow_new_version = new_version

        if label is None:
            label = 'project-operation'

        super().__init__(
            workdir,
            groupID=groupID, 
            forceful=forceful,
            dryrun=dryrun,
            thorough=thorough,
            logger=logger,
            bypass=bypass,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose)
    
        if not os.path.isdir(self.groupdir):
            raise ValueError(
                f'The group "{groupID}" has not been initialised - not present in the working directory'
            )

        # Need a first-time initialisation implementation for some elements.

        if fh is not None:
            self.logger.info('Filesystem Logging: Enabled')
    
        self._create_dirs(first_time=first_time)

        self.logger.debug(f'Creating operator for project {self.proj_code}')
        # Project FileHandlers
        self.base_cfg   = JSONFileHandler(self.dir, 'base-cfg', conf=file_configs['base_cfg'], logger=self.logger, **self.fh_kwargs)
        self.detail_cfg = JSONFileHandler(self.dir, 'detail-cfg', conf=file_configs['detail_cfg'], logger=self.logger, **self.fh_kwargs)
        self.allfiles   = ListFileHandler(self.dir, 'allfiles', logger=self.logger, **self.fh_kwargs)

        # ft_kwargs <- stored in base_cfg after this point.
        if first_time:

            if isinstance(ft_kwargs, dict):
                self._setup_config(**ft_kwargs)
            self._configure_filelist()

            if len(self.allfiles) < 1:
                raise ValueError(f'Project {self.proj_code} contains no files')

        # ProjectOperation attributes
        self.status_log = CSVFileHandler(self.dir, 'status_log', logger=self.logger, **self.fh_kwargs)

        self.phase_logs = {}
        for phase in ['scan', 'compute', 'validate']:
            self.phase_logs[phase] = LogFileHandler(
                self.dir,
                phase, 
                logger=self.logger, 
                extra_path='phase_logs/', 
                **self.fh_kwargs
            )

        self._kfile  = None
        self._kstore = None
        self._zstore = None
        self._cfa_dataset = None
        self._remote = False

        self._diagnostic = diagnostic

        self._is_trial = False

        self.stage = None

        # Used for all phases, apply at runtime.
        self._xarray_kwargs = xarray_kwargs or {}

        # Remote s3 direct connection
        if isinstance(remote_s3,str):
            remote_s3 = extract_json()

        if remote_s3 is not None:
            self.base_cfg['remote_s3'] = remote_s3
            self.base_cfg.save()

    def __str__(self):
        """String representation of project"""
        return f'<PADOCC Project: {self.proj_code} ({self.groupID})>'
    
    def __repr__(self):
        """Yaml info dump representation"""
        return yaml.dump(self.info())

    def info(self):
        """
        Display some info about this particular project.
        """
        return {
            self.proj_code: {
                'Group':self.groupID,
                'Phase': self._get_phase(),
                'File count': len(self.allfiles),
                'Revision': self.revision
                
            }
        }
    
    @classmethod
    def help(self, func: Callable = print_fmt_str):
        """
        Public user functions for the project operator.

        :param func:        (Callable) provide an alternative to 'print' function
            for displaying help information.
        """
        func('Project Operator:')
        func(' > project.info() - Get some information about this project')
        func(' > project.get_version() - Get the version number for the output product')
        func(' > project.save_files() - Save all open files related to this project')

        for cls in ProjectOperation.__bases__:
            cls.help(func)
        
    def run(
            self,
            mode: str = None,
            bypass: Union[BypassSwitch,None] = None,
            forceful : bool = None,
            thorough : bool = None,
            verbose: bool = None,
            dryrun : bool = None,
            parallel: bool = False,
            **kwargs
        ) -> str:
        """
        Main function for running any project operation. 
        
        All subclasses act as plugins for this function, and require a
        ``_run`` method called from here. This means all error handling
        with status logs and log files can be dealt with here.
        
        To find the parameters for a specific operation (e.g. compute 
        with kerchunk mode), see the additional parameters of ``run`` in
        the source code for the phase you are running. In this example, 
        see ``padocc.phases.compute:KerchunkDS._run``

        :param mode:            (str) Cloud format to use for any operations. Default value is 
            'kerchunk' and any changes via the 'cloud_format' parameter to this project are taken
            into account. Note: Setting the mode for a specific operation using THIS argument,
            will reset the cloud format stored property for this class.

        :param bypass:          (BypassSwitch) instance of BypassSwitch class containing multiple
            bypass/skip options for specific events. See utils.BypassSwitch.

        :param forceful:        (bool) Continue with processing even if final output file 
            already exists.

        :param dryrun:          (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.

        :param thorough:        (bool) From args.quality - if True will create all files 
            from scratch, otherwise saved refs from previous runs will be loaded.
        
        """
        if not hasattr(self,'phase'):
            raise ValueError(
                '"Run" Operation cannot be performed on the base ProjectOperation class. ',
                'Please use one of the phased operators or the `Group.run` method instead.'
            )

        self._bypass = bypass or self._bypass

        if parallel:
            self.logger.info('Parallel Operation: Enabled')
        else:
            self.logger.info('Parallel Operation: Disabled')

        # Reset flags given specific runs
        if forceful is not None:
            self._forceful = forceful
        if thorough is not None:
            self._thorough = thorough
        if dryrun is not None:
            self._dryrun = dryrun
        if verbose is not None:
            self._verbose = verbose

        mode = mode or self.cloud_format

        if self.cloud_format != mode:
            self.logger.info(
                f'Switching cloud format to {mode}'
            )
            self.cloud_format = mode
            self.save_files()
            
        try:
            status = self._run(mode=mode, **kwargs)
            # Reset cloud format and save files
            self.cloud_format = mode
            self.save_files()
            return status
        except Exception as err:
            agg_shorthand = self.get_agg_shorthand()
            return error_handler(
                err, self.logger, self.phase,
                jobid=self._logid,
                subset_bypass=self._bypass.skip_subsets,
                status_fh=self.status_log,
                agg_shorthand=agg_shorthand)

    def _run(self, **kwargs) -> None:
        # Default project operation run.
        self.logger.info("Nothing to run with this setup!")

    @property
    def dir(self):
        """Project directory property, relative to workdir."""
        if self.groupID:
            return f'{self.workdir}/in_progress/{self.groupID}/{self.proj_code}'
        else:
            return f'{self.workdir}/in_progress/general/{self.proj_code}'
        
    def get_cfa_cache_files(self, get_missing: bool = False):
        """
        Get the correctly ordered set of CFA cache files
        """
        subset_total = self.detail_cfg.get('compute_subsets')
        num_files    = self.detail_cfg.get('num_files')

        if subset_total is None:
            raise ValueError(
                'No subset was recorded, unable to continue'
            )
        
        if num_files is None:
            num_files = len(self.allfiles.get())
            self.detail_cfg['num_files'] = num_files
        
        self.logger.debug(f'Checking {self.dir}/cfacache')

        cfafiles = []
        missing = []
        group_size = int(num_files)/int(subset_total)
        for cfa_i in range(0, int(subset_total)):
            if os.path.isfile(f'{self.dir}/cfacache/{cfa_i*int(group_size)}.nca'):
                cfafiles.append(f'{self.dir}/cfacache/{cfa_i*int(group_size)}.nca')
            else:
                self.logger.debug(f'Missing: {cfa_i}, {cfa_i*int(group_size)}')
                missing.append(f'{self.dir}/cfacache/{cfa_i*int(group_size)}.nca')

        if get_missing:
            return missing
        
        return cfafiles

    def get_agg_shorthand(self) -> None:
        """
        Get Aggregation shorthand"""

        status_msg = self.get_last_status().split(',')
        if status_msg[0] == 'validate' or (status_msg[0] == 'compute' and status_msg[1] == 'Success'):
            if self.padocc_aggregation:
                return '(P>VK)'
            elif self.virtualizarr:
                return '(V>K)'
            elif self.kerchunk_aggregation:
                return '(K)'
            else:
                return '(X)'
        else:
            return '(PVK)'

    def diagnostic(self, message: str):
        """
        Diagnostic mode, enable skipping specific features.
        """

        if not self._diagnostic:
            return False
        
        ask = input(f'Skip feature: {message}? (Y/N): ')
        if ask == 'Y':
            return True
        else:
            return False

    def file_exists(self, file : str):
        """
        Check if a named file exists (without extension).

        This can be any generic filehandler attached.
        """
        if hasattr(self, file):
            fhandle = getattr(self, file)
        return fhandle.file_exists()
    
    def delete_project(self, ask: bool = True):
        """
        Delete a project

        :param ask: (bool) Will ask an 'are you sure' message if not False.
        """
        if self._dryrun:
            self.logger.info('Skipped Deleting directory in dryrun mode.')
            return
        if ask:
            inp = input(f'Are you sure you want to delete {self.proj_code}? (Y/N) ')
            if inp != 'Y':
                self.logger.warning(f'Skipped Deleting directory (User entered {inp})')
                return
            
        os.system(f'rm -rf {self.dir}')
        self.logger.info(f'All internal files for {self.proj_code} deleted.')

    def switch_local(self):
        """
        Switch back to local version of kerchunk file if it exists.
        """

        if not self.remote:
            self.logger.warning("Project is already/still local - nothing to do")
            return
        
        self._kfile = None
        self.remote = False
        self.save_files()

    def switch_remote(
            self,
            **kwargs
        ):
        """
        Function to create remote copy of a dataset if relevant.

        Kerchunk - create new remote version.

        Zarr - Ignore
        """

        if self.remote:
            self.logger.warning("Project has already been switched to remote")
            return
        
        ds = self.dataset
        if not isinstance(ds, KerchunkFile):
            return
        
        self.logger.debug('Switching to remote file version')

        new_rev  = ''.join((self.cloud_format[0],'r',self.version_no))
        new_path = os.path.splitext(ds.filepath)[0].replace(self.revision, new_rev) # No extension

        if self._thorough:
            os.system(f'rm {new_path}*')

        self.remote = True
        if not glob.glob(f'{new_path}*'):
            self.logger.debug('Creating new remote kerchunk file.')
            self.dataset.spawn_copy(new_path)

            # Need to refresh the kfile filehandler
            self._kfile = None
            
            self.logger.debug('Applying remote criteria to kerchunk file.')
            # Reinstantiate new filehandler + add download_link in place
            self.dataset.add_download_link(**kwargs)
        
        else:
            # Refresh kfile handler (different order to above.)
            self._kfile = None
            _ = self.dataset

        self.save_files()

    def complete_project(
            self, 
            move_to: str,
            thorough: bool = False,
            **kwargs) -> None:
        """
        Move project to a completeness directory

        :param move_to:     (str) Path to completeness directory to extract content.
        """

        self.logger.debug(f' > {self.proj_code} [{self.cloud_format}]')

        status = self.get_last_status()

        if status is None:
            self.logger.warning(
                f'Most recent phase for {self.proj_code} is unconfirmed. - '
                'please ensure re-validation of any changes or ensure products are otherwise validated.'
            )
        elif 'validate' not in status and 'complete' not in status:
            self.logger.warning(
                f'Most recent phase for {self.proj_code} is NOT validation/completion - '
                'please ensure re-validation of any changes or ensure products are otherwise validated.'
            )
        else:
            pass

        self.save_files()

        if thorough:
            # Switch to remote version before completion
            self.switch_remote(**kwargs)

        history = self.dataset_attributes.get('history','')
        if isinstance(history,str):
            history = history.split('\n')

        hist    = '\n'.join([h for h in history if h != ''])
        new_hist = self.base_cfg.get('internal_history',None)
        if new_hist:
            hist += '\n' + '\n'.join(new_hist)

        self.update_attribute('history', hist)
        self.dataset.save()

        data_move = f'{move_to}/data'
        if not os.path.isdir(data_move):
            os.makedirs(data_move)

        report_move = f'{move_to}/reports'
        if not os.path.isdir(report_move):
            os.makedirs(report_move)

        # Spawn copy of dataset
        complete_dataset = f'{data_move}/{self.complete_product}'

        self.dataset.spawn_copy(complete_dataset)

        # Spawn copy of cfa dataset
        if self.cfa_enabled and self.cfa_complete and self.cloud_format != 'CFA':
            complete_cfa = self.cfa_path.replace(self.dir, data_move) + '_' + self.version_no
            self.cfa_dataset.spawn_copy(complete_cfa)

        if not self._dryrun:
            self.update_status('complete','Success')

        self.save_files()

    def migrate(cls, newgroupID: str):
        """
        Migrate this project to a new group.

        Moves the whole project directory on the filesystem and 
        moves all associated filehandlers (individually).

        :param newgroupID:  (str) ID of new group to move this project to.
        """
        cls.logger.info(f'Migrating project {cls.proj_code}')
        
        # 1. Determine the new location
        new_dir = str(cls.dir).replace(cls.groupID, newgroupID)
        cls.logger.debug(cls.dir)
        cls.logger.debug(new_dir)

        # 2. Save all open files with current content
        cls.save_files()
        
        # 3. Move the project 
        
        # Destination may not exist yet
        if not os.path.isdir(new_dir):
            os.makedirs(new_dir)

        os.system(f'mv {cls.dir}/* {new_dir}')
        os.system(f'rm -rf {cls.dir}')

        # 4. Create a new basic project instance
        new_cls = ProjectOperation(
            cls.proj_code,
            cls.workdir,
            groupID=newgroupID,
            logger=cls.logger,
            **cls.fh_kwargs
        )

        # 5. Delete the old instance
        del cls
        return new_cls

    def save_files(self) -> None:
        """
        Save all filehandlers associated with this group.
        """
        self.base_cfg.save()
        self.detail_cfg.save()
        self.allfiles.save()
        self.status_log.save()

        # Save dataset filehandlers
        self.save_ds_filehandlers()

    def _get_phase(self) -> str:
        """
        Gets the highest phase this project has currently undertaken successfully
        """

        max_sid = 0
        for row in self.status_log:
            status = row[0]
            if status != 'Success':
                continue

            phase = row[1]
            sid = phases.index(phase)
            max_sid = max(sid, max_sid)
        return phases[max_sid]

    def _configure_filelist(self) -> None:
        """
        Configure the filelist for this project.

        Set the contents of the filelist based on
        the values provided in the base config,
        either filepath to a text file or a pattern.
        """
        pattern = self.base_cfg['pattern']

        if not pattern:
            raise ValueError(
                '"pattern" attribute missing from base config.'
            )
        
        if isinstance(pattern, list):
            # New feature to handle the moles-format data.
            fileset = pattern
        elif pattern.endswith('.txt'):
            fileset = extract_file(pattern)
        else:

            fileset = sorted(glob.glob(pattern, recursive=True))

            # Pattern is a wildcard set of files
            if 'latest' in pattern:
                fileset = [
                    os.path.abspath(
                        os.path.join(
                            fp.split('latest')[0], 'latest',os.readlink(fp)
                        ) 
                    )for fp in fileset
                ]
            
            if len(fileset) == 0:
                raise ValueError(f'pattern {pattern} returned no files.')
        
        if 'substitutions' in self.base_cfg:
            fileset, status = apply_substitutions('datasets', subs=self.base_cfg['substitutions'], content=fileset)
            if status:
                self.logger.warning(status)

        # Data sanitisation - unless skipped
        if not self._bypass.skip_filechecks:
            for x, file in enumerate(fileset):
                if not os.path.isfile(file):
                    raise ValueError(
                        f'File {file} ({x}) for project {self.proj_code}'
                        'not found on file system.')
                if file.split('.')[-1] not in source_opts:
                    raise ValueError(
                        f'File {file} ({x}) for project {self.proj_code}, extension'
                        f' {file.split(".")[-1]} not allowed - must be one of {source_opts}')
        
        self.allfiles.set(fileset) 
        self.detail_cfg['num_files'] = len(fileset)
        self.detail_cfg.save()

    def _setup_config(
            self, 
            pattern : Union[str,list,None] = None, 
            updates : Union[str,None] = None, 
            removals : Union[str,None] = None,
            substitutions: Union[dict,None] = None,
            **kwargs,
        ) -> None:
        """
        Create base cfg json file with all required parameters.

        :param pattern:     (str) File pattern or path to file containing
            a list of source files that this project includes.

        :param updates:     (str) Path to json file containing updates. Updates
            should be of the form ``{'update_attribute': 'update_value'}``.

        :param removals:    (str) Path to json file containing removals. Removals
            should be of the form ``{'remove_attribute'}``

        :param substitutions: (dict) The substitutions applied to the set of files
            identified by the pattern.
        """

        self.logger.debug(f'Constructing the config file for {self.proj_code}')
        if pattern or updates or removals:
            config = {
                'proj_code':self.proj_code,
                'pattern':pattern,
                'updates':updates,
                'removals':removals,
            }
            if substitutions:
                config['substitutions'] = substitutions
            self.base_cfg.set(config | kwargs)

    def _dir_exists(
            self, 
            checkdir: Union[str,None] = None) -> bool:
        """
        Check a directory exists on the filesystem.

        :param checkdir:    (str) Check this directory exists. Defaults
            to the project directory for this project.
        """
        if not checkdir:
            checkdir = self.dir

        if os.path.isdir(checkdir):
            return True
        return False

    def _create_dirs(
            self, 
            first_time : bool = None
        ) -> None:
        """
        Create Project directory and other required directories.

        :param first_time:  (bool) Skip creating existing directories if this is true.
        """
        if not self._dir_exists():
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making project directory for: "{self}"')
            else:
                os.makedirs(self.dir)
        else:
            if first_time:
                self.logger.warning(f'"{self.dir}" already exists.')

        logdir = f'{self.dir}/phase_logs'
        if not self._dir_exists(logdir):
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making phase_logs directory for: "{self}"')
            else:
                os.makedirs(logdir)
        else:
            if first_time:
                self.logger.warning(f'"{logdir}" already exists.')

    def export_report(
            self,
            new_location: str
        ):
        """
        Export report to a new location from within the pipeline.
        """

        final_report = {}
        for report in ['data_report.json','metadata_report.json']:
            if os.path.isfile(f'{self.dir}/{report}'):

                with open(f'{self.dir}/{report}') as r:
                    rep = json.load(r)

                final_report[report.split('_')[0]] = rep

        if final_report != {}:
            os.system(f'touch {new_location}/reports/{self.proj_code}_{self.revision}_report.json')
            with open(f'{new_location}/reports/{self.proj_code}_{self.revision}_report.json','w') as f:
                f.write(json.dumps(final_report))

    def aggregation_method(self) -> str:
        """
        Returns the current aggregation method used.
        """

        if self.padocc_aggregation:
            return 'padocc'
        if self.virtualizarr:
            return 'virtualizarr'
        if self.kerchunk_aggregation:
            return 'kerchunk'
        
        for log in self.status_log.get():
            if 'compute' in log:
                return 'unable'
        return None

    def minor_version_increment(self, addition: Union[str,None] = None):
        """
        Increment the minor x.Y number for the version.

        Use this function for when properties of the cloud file have been changed.

        :param addition:    (str) Reason for version change; attribute change or otherwise.
        """

        major, minor = self.version_no.split('.')
        minor = str(int(minor)+1)

        self.base_cfg['version_no'] = f'{major}.{minor}'
        self._disconnect_ds_filehandlers()

    def major_version_increment(self):
        """
        Increment the major X.y part of the version number.

        Use this function for major changes to the cloud file 
        - e.g. replacement of source file data.
        """
        raise NotImplementedError
    
        major, minor = self.version_no.split('.')
        major = str(int(major)+1)

        self.version_no = f'{major}.{minor}'