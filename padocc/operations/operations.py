__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from padocc.core import (
    FalseLogger,
    LoggedOperation, 
    BypassSwitch
)

from padocc.operations.filehandlers import (
    JSONFileHandler, 
    CSVFileHandler,
    TextFileHandler,
    LogFileHandler,
    KerchunkFile,
)
from padocc.core.utils import extract_file

import os
import logging

class DirectoryOperation(LoggedOperation):
    """
    Container class for Operations which require functionality to create
    directories (workdir, groupdir, cache etc.)
    """

    def __init__(
            self, 
            workdir, 
            groupID=None, forceful=None, dryrun=None, 
            thorough=None, logger=None, bypass=None, label=None, 
            fh=None, logid=None, verbose=None):
        
        self.workdir = workdir
        self.groupID = groupID

        self._thorough = thorough
        self._bypass   = bypass

        self._set_fh_kwargs(forceful=forceful, dryrun=dryrun)

        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose)

    @property
    def fh_kwargs(self):
        return {
            'dryrun': self._dryrun,
            'forceful': self._forceful,
            'verbose': self._verbose,
        }
    
    @fh_kwargs.setter
    def fh_kwargs(self, value):
        self._set_fh_kwargs(**value)

    def _set_fh_kwargs(self, forceful=None, dryrun=None, verbose=None):
        self._forceful = forceful
        self._dryrun   = dryrun
        self._verbose  = verbose

    def _setup_workdir(self):
        if not os.path.isdir(self.workdir):
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making workdir {self.workdir}')
            else:
                os.makedirs(self.workdir)

    def _setup_groupdir(self):
        if self.groupID:  
            # Create group directory
            if not os.path.isdir(self.groupdir):
                if self._dryrun:
                    self.logger.debug(f'DRYRUN: Skip making groupdir {self.groupdir}')
                os.makedirs(self.groupdir)

    def _setup_directories(self):
        self._setup_workdir()
        self._setup_groupdir()

    def _setup_cache(self):
        self.cache = f'{self.dir}/cache'

        if not os.path.isdir(self.cache):
            os.makedirs(self.cache) 
        if self._thorough:
            os.system(f'rm -rf {self.cache}/*')

    @property
    def groupdir(self):
        if self.groupID:
            return f'{self.workdir}/groups/{self.groupID}'
        else:
            raise ValueError(
                'Operation has no "groupID" so cannot construct a "groupdir".'
            )

class GroupOperation(DirectoryOperation):

    def __init__(
            self, 
            groupID : str,
            workdir : str = None, 
            forceful : bool = None,
            dryrun   : bool = None,
            thorough : bool = None,
            logger   : logging.logger | FalseLogger = FalseLogger(),
            bypass   : BypassSwitch = BypassSwitch(),
            label    : str = None,
            fh       : str = None,
            logid    : str = None,
            verbose  : int = None,
        ) -> None:
        """
        Initialisation for a GroupOperation object to handle all interactions
        with all projects within a group. 

        :param groupID:         (str) Name of current dataset group.

        :param workdir:         (str) Path to the current working directory.

        :param forceful:        (bool) Continue with processing even if final output file 
            already exists.

        :param dryrun:          (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.

        :param thorough:        (bool) From args.quality - if True will create all files 
            from scratch, otherwise saved refs from previous runs will be loaded.

        :param logger:          (logging.logger | FalseLogger) An existing logger object.
                                    
        :param bypass:              (BypassSwitch) instance of BypassSwitch class containing multiple
                                    bypass/skip options for specific events. See utils.BypassSwitch.

        :param label:       (str) The label to apply to the logger object.

        :param fh:          (str) Path to logfile for logger object generated in this specific process.

        :param logid:       (str) ID of the process within a subset, which is then added to the name
            of the logger - prevents multiple processes with different logfiles getting loggers confused.

        :param verbose:         (int) Level of verbosity for log messages (see core.init_logger).

        :returns: None

        """

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

        self.proj_codes      = {}
        self.blacklist_codes = CSVFileHandler(
            self.groupdir,
            'blacklist_codes',
            self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful,
        )

        self.datasets = CSVFileHandler(
            self.groupdir,
            'datasets',
            self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful,
        )

        self._load_proj_codes()

    def run(self):
        raise NotImplementedError
    
    def add_project(self):
        pass

    def save_proj_codes(self):
        for pc in self.proj_codes:
            pc.save_file()

    def save_files(self):
        self.blacklist_codes.save_file()
        self.datasets.save_file()
        self.save_proj_codes(self)

    def add_proj_codeset(self, name : str, newcodes : list):
        self.proj_codes[name] = TextFileHandler(
            self.proj_codes_dir,
            name,
            self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful
        )

        self.proj_codes[name].update(newcodes)

    def get_datasets(self):

        contents = self.datasets.get()
        datasets = {r.strip().split(',')[0]:r.strip().split(',')[1:] for r in contents[:]}
        return datasets

    @property
    def proj_codes_dir(self):
        return f'{self.groupdir}/proj_codes'

    @property
    def new_inputfile(self):
        if self.groupID:
            return f'{self.workdir}/groups/filelists/{self.groupID}.txt'
        else:
            raise NotImplementedError
        
    def _load_proj_codes(self):
        import glob
        # Check filesystem for objects
        proj_codes = [g.split('/')[-1].strip('.txt') for g in glob.glob(f'{self.proj_codes_dir}/*.txt')]

        for p in proj_codes:
            self.proj_codes[p] = TextFileHandler(
                self.proj_codes_dir, 
                p, 
                self.logger,
                dryrun=self._dryrun,
                forceful=self._forceful,
            )

    def _setup_groupdir(self):
        super()._setup_groupdir()

        # Create proj-codes folder
        codes_dir = f'{self.groupdir}/proj_codes'
        if not os.path.isdir(codes_dir):
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making codes-dir for {self.groupID}')
            os.makedirs(codes_dir)
            
class ProjectOperation(DirectoryOperation):
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
            logger     : logging.logger = None,
            bypass     : BypassSwitch = BypassSwitch(),
            label      : str = None,
            fh         : str = None,
            logid      : str = None,
            verbose    : bool = None,
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
    
        self._create_dirs(first_time=first_time)

        # Project FileHandlers
        self.base_cfg   = JSONFileHandler(self.dir, 'base-cfg', self.logger, **self.fh_kwargs)
        self.detail_cfg = JSONFileHandler(self.dir, 'detail-cfg', self.logger, **self.fh_kwargs)
        self.allfiles   = TextFileHandler(self.dir, 'allfiles', self.logger, **self.fh_kwargs)

        # ft_kwargs <- stored in base_cfg after this point.
        if first_time:
            if isinstance(ft_kwargs, dict):
                self._setup_config(**ft_kwargs)
            self.configure_filelist()

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

    def create_new_kfile(self, product : str):
        self.kfile = KerchunkFile(
            self.dir,
            product,
            self.logger,
            **self.fh_kwargs
        )

    def create_new_kstore(self, product : str):
        raise NotImplementedError

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

    def set_last_run(self, phase: str, time : str) -> None:
        lr = (phase, time)
        self.detail_cfg.set(lr, 'last_run')

    def get_last_run(self) -> tuple:
        return self.detail_cfg.get('last_run')

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

    def _configure_filelist(self):
        pattern = self.base_cfg.get('pattern')

        if not pattern:
            raise ValueError(
                '"pattern" attribute missing from base config.'
            )
        
        if not pattern.endswith('.txt'):
            # Expand pattern
            raise NotImplementedError
        
            if 'latest' in pattern:
                pattern = pattern.replace('latest', os.readlink(pattern))
        else:
            self.allfiles = extract_file(pattern)

    def _setup_config(
            self, 
            pattern : str = None, 
            update : str = None, 
            remove : str = None
        ) -> None:
        """
        Create base cfg json file with all required parameters.
        """
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
            raise NotImplementedError

    def _create_dirs(self, first_time : bool = None):
        if not self.dir_exists():
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making project directory for: "{self}"')
            os.makedirs(self.dir)
        else:
            if first_time:
                self.logger.warning(f'"{self.dir}" already exists.')

        logdir = f'{self.dir}/phase_logs'
        if not self.dir_exists(logdir):
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making phase_logs directory for: "{self}"')
            os.makedirs(logdir)
        else:
            if first_time:
                self.logger.warning(f'"{logdir}" already exists.')

    def _get_log(self, phase: str):
        """
        Fetch a log file from a previous run for this 
        dataset for a particular phase.
        """
        if phase in self.phase_logs:
            return self.phase_logs[phase]

        self.logger.error(
            f'Phase "{phase}" does not have an associated log file '
            'or is not a recognised phase within the pipeline.'
        )
        return '!No log data!'

    def _examine_log(self, phase : str, error=None, rerun=None, halt=False):
        """
        Examine the log file for this project given a specific phase, possibly looking for a 
        specific error.
        """
        print(self._get_log(phase))

        self.logger.info(f'Project Code: {self.proj_code}')
        if error:
            self.logger.info(f'Error: {error}')
        if rerun:
            self.logger.info(rerun)

        if halt:
            paused = input('Type "E" to exit assessment:')
            if paused == 'E':
                raise KeyboardInterrupt



    