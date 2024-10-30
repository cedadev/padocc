__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
import glob
import logging

from .errors import error_handler
from .utils import extract_file, BypassSwitch
from .logs import reset_file_handler

from .mixins import DirectoryMixin, EvaluationsMixin
from .filehandlers import (
    JSONFileHandler, 
    CSVFileHandler,
    TextFileHandler,
    LogFileHandler,
    KerchunkFile
)

          
class ProjectOperation(DirectoryMixin, EvaluationsMixin):
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
            thorough   : bool = None
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

        self.logger.debug(f'Creating operator for project "{self.proj_code}')
        # Project FileHandlers
        self.base_cfg   = JSONFileHandler(self.dir, 'base-cfg', self.logger, **self.fh_kwargs)
        self.detail_cfg = JSONFileHandler(self.dir, 'detail-cfg', self.logger, **self.fh_kwargs)
        self.allfiles   = TextFileHandler(self.dir, 'allfiles', self.logger, **self.fh_kwargs)

        # ft_kwargs <- stored in base_cfg after this point.
        if first_time:
            if isinstance(ft_kwargs, dict):
                self._setup_config(**ft_kwargs)
            self._configure_filelist()

        # ProjectOperation attributes
        self.status_log = CSVFileHandler(self.dir, 'status_log', self.logger, **self.fh_kwargs)

        self.phase_logs = {}
        for phase in ['scan', 'compute', 'validate']:
            self.phase_logs[phase] = LogFileHandler(
                self.dir,
                phase, 
                self.logger, 
                extra_path='phase_logs/', 
                **self.fh_kwargs
            )

        self.kfile  = None
        self.kstore = None
        self.zstore = None

        self._outfile = None

    def __str__(self):
        return f'<PADOCC Project: {self.groupID}>'
    
    def __repr__(self):
        return str(self)

    def run(
            self,
            mode: str,
            subset_bypass: bool = False, 
            forceful : bool = None,
            thorough : bool = None,
            dryrun : bool = None,
        ) -> str:
        """
        Main function for running any project operation. All 
        subclasses act as plugins for this function, and require a
        ``_run`` method called from here. This means all error handling
        with status logs and log files can be dealt with here."""

        # Reset flags given specific runs
        if forceful is not None:
            self._forceful = forceful
        if thorough is not None:
            self._thorough = thorough
        if dryrun is not None:
            self._dryrun = dryrun

        try:
            status = self._run(mode)
            self.save_files()
            return status
        except Exception as err:
            
            return error_handler(
                err, self.logger, self.phase,
                jobid=self._logid, dryrun=self._dryrun, 
                subset_bypass=subset_bypass,
                status_fh=self.status_log)

    def _run(self, **kwargs):
        # Default project operation run.
        self.logger.info("Nothing to run with this setup!")

    def create_new_kfile(self, product : str):
        self.kfile = KerchunkFile(
            self.dir,
            product,
            self.logger,
            **self.fh_kwargs
        )

    def create_new_kstore(self, product : str):
        raise NotImplementedError

    def get_version(self):
        """
        Get the current version of the output file.
        """
        return self.detail_cfg['version_no'] or 1

    @property
    def cfa_path(self):
        return f'{self.dir}/{self.proj_code}.nca'

    @property
    def outfile(self):
        if self._outfile:
            return self._outfile
        
        # Assemble the outfile
        return None
    
    @outfile.setter
    def outfile(self, value : str):
        self._outfile = value

    def __str__(self):
        return self.proj_code

    def dir_exists(self, checkdir : str = None):
        if not checkdir:
            checkdir = self.dir

        if os.path.isdir(checkdir):
            return True
        return False

    def file_exists(self, file : str):
        """Check if a named file exists (without extension)"""
        if hasattr(self, file):
            fhandle = getattr(self, file)
        return fhandle.file_exists()

    def update_status(
            self, 
            phase : str, 
            status: str, 
            jobid : str = '', 
            dryrun: str = ''
        ) -> None: 
        self.status_log.update_status(phase, status, jobid=jobid, dryrun=dryrun)

    def save_files(self):
        # Add all files here.
        self.base_cfg.save_file()
        self.detail_cfg.save_file()
        self.allfiles.save_file()
        self.status_log.save_file()

    def _configure_filelist(self):
        pattern = self.base_cfg['pattern']

        if not pattern:
            raise ValueError(
                '"pattern" attribute missing from base config.'
            )
        
        if pattern.endswith('.txt'):
            self.allfiles.set(extract_file(pattern)) 
        else:
            # Pattern is a wildcard set of files
            if 'latest' in pattern:
                pattern = pattern.replace('latest', os.readlink(pattern))

            self.allfiles.set(glob.glob(pattern))

    def _setup_config(
            self, 
            pattern : str = None, 
            update : str = None, 
            remove : str = None
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
            self.base_cfg.set(config)

    @property
    def dir(self):
        if self.groupID:
            return f'{self.workdir}/in_progress/{self.groupID}/{self.proj_code}'
        else:
            return f'{self.workdir}/in_progress/general/{self.proj_code}'

    def _create_dirs(self, first_time : bool = None):
        if not self.dir_exists():
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making project directory for: "{self}"')
            else:
                os.makedirs(self.dir)
        else:
            if first_time:
                self.logger.warning(f'"{self.dir}" already exists.')

        logdir = f'{self.dir}/phase_logs'
        if not self.dir_exists(logdir):
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making phase_logs directory for: "{self}"')
            else:
                os.makedirs(logdir)
        else:
            if first_time:
                self.logger.warning(f'"{logdir}" already exists.')
