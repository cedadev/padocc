__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import glob
import logging
import os
from typing import Callable, Union

import yaml

from .errors import error_handler
from .filehandlers import (CSVFileHandler, JSONFileHandler, ListFileHandler,
                           LogFileHandler)
from .mixins import (DatasetHandlerMixin, DirectoryMixin, PropertiesMixin,
                     StatusMixin)
from .utils import (FILE_DEFAULT, BypassSwitch, apply_substitutions,
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
            mem_allowed: Union[str,None] = None,
            remote_s3  : Union[dict, str, None] = None,
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
            self.logger.info('Logging to filesystem file is enabled')
    
        self._create_dirs(first_time=first_time)

        self.logger.debug(f'Creating operator for project {self.proj_code}')
        # Project FileHandlers
        self.base_cfg   = JSONFileHandler(self.dir, 'base-cfg', logger=self.logger, conf=file_configs['base_cfg'], **self.fh_kwargs)
        self.detail_cfg = JSONFileHandler(self.dir, 'detail-cfg', logger=self.logger, conf=file_configs['detail_cfg'], **self.fh_kwargs)
        self.allfiles   = ListFileHandler(self.dir, 'allfiles', logger=self.logger, **self.fh_kwargs)

        # ft_kwargs <- stored in base_cfg after this point.
        if first_time:
            if isinstance(ft_kwargs, dict):
                self._setup_config(**ft_kwargs)
            self._configure_filelist()

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

        self._is_trial = False

        self.stage = None

        # Remote s3 direct connection
        if isinstance(remote_s3,str):
            remote_s3 = extract_json()

        if remote_s3 is not None:
            self.base_cfg['remote_s3'] = remote_s3
            self.base_cfg.close()

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
            mode: str = 'kerchunk',
            bypass: Union[BypassSwitch,None] = None,
            forceful : bool = None,
            thorough : bool = None,
            dryrun : bool = None,
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

        self._bypass = bypass or self._bypass

        # Reset flags given specific runs
        if forceful is not None:
            self._forceful = forceful
        if thorough is not None:
            self._thorough = thorough
        if dryrun is not None:
            self._dryrun = dryrun

        if self.cloud_format != mode:
            self.logger.info(
                f'Switching cloud format to {mode}'
            )
            self.cloud_format = mode
            self.file_type = FILE_DEFAULT[mode]
            
        try:
            status = self._run(mode=mode, **kwargs)
            self.save_files()
            return status
        except Exception as err:
            return error_handler(
                err, self.logger, self.phase,
                jobid=self._logid,
                subset_bypass=self._bypass.skip_subsets,
                status_fh=self.status_log)

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

    def complete_project(self, move_to: str) -> None:
        """
        Move project to a completeness directory

        :param move_to:     (str) Path to completeness directory to extract content.
        """

        self.logger.debug(f' > {self.proj_code} [{self.cloud_format}]')

        status = self.get_last_status()
        if 'validate' not in status:
            self.logger.warning(
                f'Most recent phase for {self.proj_code} is NOT validation - '
                'please re-validate any changes or ensure products are otherwise validated.'
            )

        self.save_files()

        # Spawn copy of dataset
        complete_dataset = f'{move_to}/{self.complete_product}'
        self.dataset.spawn_copy(complete_dataset)

        # Spawn copy of cfa dataset
        if self.detail_cfg.get('CFA',False):
            complete_cfa = self.cfa_path.replace(self.dir, move_to) + '_' + self.version_no
            self.cfa_dataset.spawn_copy(complete_cfa)

        if not self._dryrun:
            self.update_status('complete','Success')

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

        os.system(f'mv {cls.dir} {new_dir}')

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
        self.base_cfg.close()
        self.detail_cfg.close()
        self.allfiles.close()
        self.status_log.close()

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
            # Pattern is a wildcard set of files
            if 'latest' in pattern:
                pattern = pattern.replace('latest', os.readlink(pattern))
            

            fileset = sorted(glob.glob(pattern, recursive=True))
            if len(fileset) == 0:
                raise ValueError(f'pattern {pattern} returned no files.')
        
        if 'substitutions' in self.base_cfg:
            fileset, status = apply_substitutions('datasets', subs=self.base_cfg['substitutions'], content=fileset)
            if status:
                self.logger.warning(status)
        self.allfiles.set(fileset) 

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
