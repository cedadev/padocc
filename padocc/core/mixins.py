__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
import logging

from .logs import LoggedOperation, levels
from .utils import BypassSwitch

class DirectoryMixin(LoggedOperation):
    """
    Container class for Operations which require functionality to create
    directories (workdir, groupdir, cache etc.)
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
        
        self.workdir = workdir
        self.groupID = groupID

        self._thorough = thorough
        self._bypass   = bypass

        if verbose in levels:
            verbose = levels.index(verbose)

        self._set_fh_kwargs(forceful=forceful, dryrun=dryrun)

        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose)

    def values(self):
        print(f' - forceful: {bool(self._forceful)}')
        print(f' - thorough: {bool(self._thorough)}')
        print(f' - dryrun: {bool(self._dryrun)}')

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
                else:
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

    def setup_slurm_directories(self):
        # Make Directories
        for dirx in ['sbatch','errs']:
            if not os.path.isdir(f'{self.dir}/{dirx}'):
                if self._dryrun:
                    self.logger.debug(f"DRYRUN: Skipped creating {dirx}")
                    continue
                os.makedirs(f'{self.dir}/{dirx}')

class EvaluationsMixin:

    def set_last_run(self, phase: str, time : str) -> None:
        """
        Set the phase and time of the last run for this project.
        """
        lr = (phase, time)
        self.detail_cfg['last_run'] = lr

    def get_last_run(self) -> tuple:
        """
        Get the tuple-value for this projects last run."""
        return self.detail_cfg['last_run']

    def get_last_status(self) -> str:
        """
        Gets the last line of the correct log file
        """
        return self.status_log[-1]

    def get_log_contents(self, phase: str) -> str:
        """
        Get the contents of the log file as a string
        """

        if phase in self.phase_logs:
            return str(self.phase_logs[phase])
        self.logger.warning(f'Phase "{phase}" not recognised - no log file retrieved.')
        return ''

    def show_log_contents(self, phase: str, halt : bool = False):
        """
        Format the contents of the log file to print.
        """

        logfh = self.get_log_contents(phase=phase)
        status = self.status_log[-1].split(',')
        self.logger.info(logfh)

        self.logger.info(f'Project Code: {self.proj_code}')
        self.logger.info(f'Status: {status}')

        self.logger.info(self._rerun_command())

        if halt:
            paused = input('Type "E" to exit assessment:')
            if paused == 'E':
                raise KeyboardInterrupt

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

    def _rerun_command(self):
        """
        Setup for running this specific component interactively.
        """
        return ''