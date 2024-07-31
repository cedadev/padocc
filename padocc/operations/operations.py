from padocc.logs import init_logger
from .filehandlers import (
    PadoccFileHandler,
    JSONFileHandler, 
    CSVFileHandler,
    TextFileHandler,
    LogFileHandler,
)
from padocc.utils import extract_file

import os
import json

class LoggedOperation:
    
    def __init__(self):
        raise NotImplementedError

    def _load_logger(self, logger, label=None, fh=None, logid=None, verbose=0):
        self._verbose = verbose
        if hasattr(self, 'logger'):
            return
        if not logger:
            self.logger = init_logger(
                self._verbose, 
                label,
                fh=fh, 
                logid=logid)
        else:
            self.logger = logger

class DirectoryOperation(LoggedOperation):
    def __init__(self, workdir, groupID=None, forceful=None, dryrun=None, logger=None, **kwargs):
        self.workdir = workdir
        self.groupID = groupID

        self._forceful = forceful
        self._dryrun   = dryrun

        self._load_logger(logger, **kwargs)

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

    @property
    def groupdir(self):
        if self.groupID:
            return f'{self.workdir}/groups/{self.groupID}'
        else:
            raise ValueError(
                'Operation has no "groupID" so cannot construct a "groupdir".'
            )

class GroupOperation(DirectoryOperation):

    def __init__(self, workdir=None, **kwargs):
        super().__init__(workdir=workdir, **kwargs)

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

    def save_proj_codes(self):
        for pc in self.proj_codes:
            pc.save_file()

    def save_files(self):
        self.blacklist_codes.save_file()
        self.datasets.save_file()
        self.save_proj_codes(self)

    def add_proj_codeset(self, name, newcodes):
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
    
    # Replacing the below using FileHandlers
    def get_codes(self, repeat_id) -> list:
        """
        Returns a list of the project codes given a filename (repeat id)

        :param repeat_id:       (str) Identifier for a subset list of project codes

        :returns: A list of codes if the file is found, an empty list otherwise.
        """
        return self.proj_codes[repeat_id].get
        
    def set_codes(self, filename: str, contents, extension='.txt', overwrite=0) -> None:
        """
        Returns a list of the project codes given a filename (repeat id)

        :param filename:    (str) Name of text file to access within group (or path
                            within the groupdir to the text file
        
        :param contents:    (str) Combined contents to write to the file.

        :param extension:   (str) For the specific case of non-text-files.

        :param overwrite:   (str) Specifier for open() built-in python method, completely
                            overwrite the file contents or append to existing values.

        :returns: None
        """
        codefile = f'{self.groupdir}/{filename}{extension}'

        ow = 'w'
        if overwrite == 1:
            ow = 'w+'

        with open(codefile, ow) as f:
            f.write(contents)
        
    def get_blacklist(self) -> list:
        """
        Returns a list of the project codes given a filename (repeat id)

        :param group:       (str) Name of current group or path to group directory
                            (groupdir) in which case workdir can be left as None.

        :param workdir:     (str) Path to working directory or None. If this is None,
                            group value will be assumed as the groupdir path.

        :returns: A list of codes if the file is found, an empty list otherwise.
        """
        codefile = f'{self.groupdir}/blacklist_codes.txt'
        if os.path.isfile(codefile):
            with open(codefile) as f:
                contents = [r.strip().split(',') for r in f.readlines()]
                if type(contents[0]) != list:
                    contents = [contents]
                return contents
        else:
            return []

class ProjectOperation(DirectoryOperation):
    def __init__(
        self, 
        workdir, 
        proj_code, 
        groupID=None, 
        logger=None, 
        pattern=None,
        first_time=None, 
        **kwargs):
        self._proj_code = proj_code

        super().__init__(workdir, groupID=groupID, logger=None)

        # Need a first-time initialisation implementation for some elements.

        self._setup_dir()
        self._create_dirs(first_time=first_time)

        # Project FileHandlers

        fh_kwargs = {
            'dryrun':self._dryrun,
            'forceful':self._forceful,
        }

        # ProjectOperation properties
        self.base_cfg   = JSONFileHandler(self.dir, 'base-cfg', self.logger, **fh_kwargs)
        self.detail_cfg = JSONFileHandler(self.dir, 'detail-cfg', self.logger, **fh_kwargs)
        self.allfiles   = TextFileHandler(self.dir, 'allfiles', self.logger, **fh_kwargs)

        if pattern and first_time:
            self.configure_filelist(pattern)

        # ProjectOperation attributes
        self.status_log = CSVFileHandler(self.dir, 'status_log', self.logger, **fh_kwargs)

        self.phase_logs = {}
        for phase in ['scan', 'compute', 'validate']:
            self.phase_logs[phase] = LogFileHandler(
                self.dir,
                phase, 
                self.logger, 
                extra_path='phase_logs/', 
                **fh_kwargs
            )
        if first_time:
            self._setup_config(**kwargs)
            if pattern:
                self._configure_filelist(pattern)

    def __str__(self):
        return self._proj_code

    def dir_exists(self, checkdir=None):
        if not checkdir:
            checkdir = self.dir

        if os.path.isdir(checkdir):
            return True
        return False

    def file_exists(self, file):
        if hasattr(self, file):
            fhandle = getattr(self, file)
        return fhandle.file_exists()

    def set_last_run(self, phase, time) -> None:
        lr = (phase, time)
        self.detail_cfg.set(lr, 'last_run')

    def get_last_run(self) -> tuple:
        return self.detail_cfg.get('last_run')

    def update_status(self, phase, status, jobid='', dryrun=''):
        self.status_log.update_status(phase, status, jobid=jobid, dryrun=dryrun)

    def save_files(self):
        # Add all files here.
        self.base_cfg._set_content()
        self.detail_cfg._set_content()
        self.allfiles._set_content()

    def _configure_filelist(self, pattern):
        if not pattern.endswith('.txt'):
            # Expand pattern
            raise NotImplementedError
        
            if 'latest' in pattern:
                pattern = pattern.replace('latest', os.readlink(pattern))
        else:
            self.allfiles = extract_file(pattern)

    # Base cfg
    @property
    def base_cfg(self):
        return self._base_cfg

    @base_cfg.setter
    def base_cfg(self, obj):
        if isinstance(obj, PadoccFileHandler):
            self._base_cfg = obj
        else:
            print(type(obj))
            self._base_cfg.set(obj)

    @property
    def detail_cfg(self):
        return self._detail_cfg

    @detail_cfg.setter
    def detail_cfg(self, obj):
        if isinstance(obj, PadoccFileHandler):
            self._detail_cfg = obj
        else:
            self._detail_cfg.set(obj)

    @property
    def allfiles(self):
        return self._allfiles

    @allfiles.setter
    def allfiles(self, obj):
        if isinstance(obj, PadoccFileHandler):
            self._allfiles = obj
        else:
            self._allfiles.set(obj)

    def _setup_config(self, pattern=None, update=None, remove=None):
        if pattern or update or remove:
            config = {
                'proj_code':self.proj_code,
                'pattern':pattern,
                'updates':update,
                'removals':remove,
            }
            self.base_cfg = config

    def _setup_dir(self):
        if self.groupID:
            self.dir = f'{self.workdir}/in_progress/{self.groupID}/{self._proj_code}'
        else:
            raise NotImplementedError
        
    def _create_dirs(self, first_time=None):
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

    def _get_log(self, phase):
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

    def _examine_log(self, phase, error=None, rerun=None, halt=False):
        """
        Examine the log file for this project given a specific phase, possibly looking for a 
        specific error.
        """
        print(self._get_log(phase))

        self.logger.info(f'Project Code: {proj_code}')
        if error:
            self.logger.info(f'Error: {error}')
        if rerun:
            self.logger.info(rerun)

        if halt:
            paused = input('Type "E" to exit assessment:')
            if paused == 'E':
                raise KeyboardInterrupt



    