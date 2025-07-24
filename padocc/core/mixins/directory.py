__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging
import os
from typing import Callable

from ..logs import LoggedOperation, levels
from ..utils import BypassSwitch


class DirectoryMixin(LoggedOperation):
    """
    Container class for Operations which require functionality to create
    directories (workdir, groupdir, cache etc.)

    This Mixin enables all child classes the ability
    to manipulate the filesystem to create new directories
    as required, and handles the so-called fh-kwargs, which
    relate to forceful overwrites of filesystem objects, 
    skipping creation or starting from scratch, all relating
    to the filesystem.

    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: ProjectOperation, GroupOperation
    """

    def __init__(
            self, 
            workdir : str, 
            groupID : str = None, 
            forceful: bool = None, 
            dryrun  : bool = None, 
            thorough: bool = None, 
            logger : logging.Logger = None, 
            bypass : BypassSwitch = None, 
            label : str = None, 
            fh : str = None, 
            logid : str = None, 
            verbose : int = 0
        ):
        """
        Directory Mixin Initialisation
        
        :param workdir: (str) Path to the current working directory.
        
        :param groupID: (str) Name of current dataset group.
        
        :param forceful:    (bool) Continue with processing even if final output file 
            already exists.
        
        :param dryrun:      (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.
        
        :param thorough:    (bool) From args.quality - if True will create all files 
            from scratch, otherwise saved refs from previous runs will be loaded.
        
        :param logger:      (logging.Logger) Logger supplied to this Operation.
        
        :param bypass:      (BypassSwitch) instance of BypassSwitch class containing multiple
            bypass/skip options for specific events. See utils.BypassSwitch.

        :param label:       (str) The label to apply to the logger object.

        :param fh:          (str) Path to logfile for logger object generated in this specific process.
            If the fh parameter is set to 'PhaseLog' which occurs from the GroupOperation's config options,
            the fh parameter will be reset in this mixin to point at the Project's correct phase log file.
        
        :param logid:       (str) ID of the process within a subset, which is then added to the name
            of the logger - prevents multiple processes with different logfiles getting
            loggers confused.
        
        :param verbose:     (int) Level of verbosity for log messages (see core.init_logger).
        """
        
        self.workdir = workdir
        self.groupID = groupID

        self._bypass   = bypass

        if verbose in levels:
            verbose = levels.index(verbose)

        if fh == 'PhaseLog':
            if not hasattr(self, 'phase'):
                raise ValueError(
                    'Running jobs with no phase operation is not supported'
                )
            
            fh = f'{self.dir}/phase_logs/{self.phase}.log'

        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose,
            forceful=forceful,
            dryrun=dryrun,
            thorough=thorough)
        
        self.logger.debug(f'Logs to {fh}')
    
    @classmethod
    def help(cls, func: Callable = print):
        """
        No public methods
        """
        pass

    def _setup_workdir(self):
        """
        Setup working directory for this object.

        The directory will be created if it does not 
        already exist.
        """

        if self.workdir is None:
            raise ValueError(
                'Working directory not defined.'
                'If using the CLI tool, please specify working directory with -w'
            )

        if not os.path.isdir(self.workdir):
            if self._dryrun:
                self.logger.debug(f'DRYRUN: Skip making workdir {self.workdir}')
            else:
                os.makedirs(self.workdir)

    def _setup_groupdir(self):
        """
        Setup group directory for this object.

        The directory will be created if it does not 
        already exist.
        """
        if self.groupID:  
            # Create group directory
            if not os.path.isdir(self.groupdir):
                if self._dryrun:
                    self.logger.debug(f'DRYRUN: Skip making groupdir {self.groupdir}')
                else:
                    os.makedirs(self.groupdir)

    def _setup_directories(self):
        """
        Ensure working and group directories are created.
        """
        self._setup_workdir()
        self._setup_groupdir()

    def _setup_cache(self, dir: str):
        """
        Set up the personal cache for this directory object.
        
        Note: Typically only Project Operators use a file
        cache, but this feature could be implemented for 
        Groups in the future.

        :param dir:     (str) Cache directory (normally just the project 
            directory)
        """
        self.cache = f'{dir}/cache'

        if not os.path.isdir(self.cache):
            os.makedirs(self.cache) 
        if self._thorough:
            os.system(f'rm -rf {self.cache}/*')

    @property
    def groupdir(self):
        """
        Group directory property
        """
        if self.groupID:
            return f'{self.workdir}/groups/{self.groupID}'
        else:
            raise ValueError(
                'Operation has no "groupID" so cannot construct a "groupdir".'
            )