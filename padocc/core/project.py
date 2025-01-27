__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
import glob
import yaml
import logging

from typing import Union, Callable

from .errors import error_handler
from .utils import (
    extract_file, 
    BypassSwitch, 
    apply_substitutions, 
    phases, 
    file_configs, 
    FILE_DEFAULT,
    print_fmt_str
)
from .logs import reset_file_handler

from .mixins import DirectoryMixin, DatasetHandlerMixin, StatusMixin, PropertiesMixin
from .filehandlers import (
    JSONFileHandler, 
    CSVFileHandler,
    ListFileHandler,
    LogFileHandler,
)

class ProjectOperation(
    DirectoryMixin, 
    DatasetHandlerMixin,
    StatusMixin,
    PropertiesMixin):
    """
    PADOCC Project Operation class, able to access project files
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
        ) -> None:
        """
        Initialisation for a ProjectOperation object to handle all interactions
        with a single project. 

        :param proj_code:       (str) The project code in string format (DOI)

        :param workdir:         (str) Path to the current working directory.

        :param groupID:         (str) Name of current dataset group.

        :param first_time:

        :param ft_kwargs:

        :param logger:
                                    
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

        :returns: None

        """

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
        
        self.proj_code = proj_code

        # Need a first-time initialisation implementation for some elements.

        if fh == 'PhaseLog':
            if not hasattr(self, 'phase'):
                raise ValueError(
                    'Running jobs with no phase operation is not supported'
                )
            
            fh = f'{self.dir}/phase_logs/{self.phase}.log'
            self.logger = reset_file_handler(self.logger, verbose=verbose, fh=fh)
    
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

    def __str__(self):
        return f'<PADOCC Project: {self.proj_code} ({self.groupID})>'
    
    def __repr__(self):
        return yaml.dump(self.info())

    def info(self):
        """
        Display some info about this particular project
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
        Main function for running any project operation. All 
        subclasses act as plugins for this function, and require a
        ``_run`` method called from here. This means all error handling
        with status logs and log files can be dealt with here.
        
        To find the parameters for a specific operation (e.g. compute 
        with kerchunk mode), see the additional parameters of ``run`` in
        the source code for the phase you are running. In this example, 
        see ``padocc.phases.compute:KerchunkDS._run``
        
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

    def move_to(self, new_directory: str) -> None:
        """
        Move all associated files across to new directory.
        """

    def _run(self, **kwargs) -> None:
        # Default project operation run.
        self.logger.info("Nothing to run with this setup!")

    @property
    def dir(self):
        if self.groupID:
            return f'{self.workdir}/in_progress/{self.groupID}/{self.proj_code}'
        else:
            return f'{self.workdir}/in_progress/general/{self.proj_code}'

    def file_exists(self, file : str):
        """
        Check if a named file exists (without extension).
        This can be any generic filehandler attached."""
        if hasattr(self, file):
            fhandle = getattr(self, file)
        return fhandle.file_exists()
    
    def delete_project(self, ask: bool = True):
        """
        Delete a project
        """
        if self._dryrun:
            self.logger.info('Skipped Deleting directory in dryrun mode.')
            return
        if ask:
            inp = input(f'Are you sure you want to delete {self.proj_code}? (Y/N)?')
            if inp != 'Y':
                self.logger.info(f'Skipped Deleting directory (User entered {inp})')
                return
            
        os.system(f'rm -rf {self.dir}')
        self.logger.info(f'All internal files for {self.proj_code} deleted.')

    def migrate(self, newgroupID: str):
        pass

    def update_status(
            self, 
            phase : str, 
            status: str, 
            jobid : str = ''
        ) -> None: 
        self.status_log.update_status(phase, status, jobid=jobid)

    def save_files(self):
        # Add all files here.
        self.base_cfg.close()
        self.detail_cfg.close()
        self.allfiles.close()
        self.status_log.close()

    def _get_phase(self):
        """
        Gets the highest phase this project has currently undertaken successfully"""

        max_sid = 0
        for row in self.status_log:
            status = row[0]
            if status != 'Success':
                continue

            phase = row[1]
            sid = phases.index(phase)
            max_sid = max(sid, max_sid)
        return phases[max_sid]

    def _configure_filelist(self):
        pattern = self.base_cfg['pattern']

        if not pattern:
            raise ValueError(
                '"pattern" attribute missing from base config.'
            )
        
        if pattern.endswith('.txt'):
            content = extract_file(pattern)
            if 'substitutions' in self.base_cfg:
                content, status = apply_substitutions('datasets', subs=self.base_cfg['substitutions'], content=content)
                if status:
                    self.logger.warning(status)
            self.allfiles.set(content) 
        else:
            # Pattern is a wildcard set of files
            if 'latest' in pattern:
                pattern = pattern.replace('latest', os.readlink(pattern))

            self.allfiles.set(sorted(glob.glob(pattern, recursive=True)))

    def _setup_config(
            self, 
            pattern : str = None, 
            update : str = None, 
            remove : str = None,
            substitutions: dict = None,
        ) -> None:
        """
        Create base cfg json file with all required parameters.
        """

        self.logger.debug('Constructing the config file.')
        if pattern or update or remove:
            config = {
                'proj_code':self.proj_code,
                'pattern':pattern,
                'updates':update,
                'removals':remove,
            }
            if substitutions:
                config['substitutions'] = substitutions
            self.base_cfg.set(config)

    def _dir_exists(self, checkdir : str = None):
        """
        Check a directory exists on the filesystem
        """
        if not checkdir:
            checkdir = self.dir

        if os.path.isdir(checkdir):
            return True
        return False

    def _create_dirs(self, first_time : bool = None):
        """
        Create Project directory and other required directories
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
