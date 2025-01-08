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
        print(f' - verbose: {bool(self._verbose)}')
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
        self.base_cfg['last_run'] = lr

    def get_last_run(self) -> tuple:
        """
        Get the tuple-value for this projects last run."""
        return self.base_cfg['last_run']

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

class PropertiesMixin:

    def _check_override(self, key, mapper) -> str:
        if self.base_cfg['override'][key] is not None:
            return self.base_cfg['override'][key]
        
        if self.detail_cfg[mapper] is not None:
            self.base_cfg['override'][key] = self.detail_cfg[mapper]
            self.base_cfg.close()
            return self.base_cfg['override'][key]
        
        return None
    
    @property
    def outpath(self):
        return f'{self.dir}/{self.outproduct}'
    
    @property
    def outproduct(self):
        if self.stage == 'complete':
            return f'{self.proj_code}.{self.revision}.{self.file_type}'
        else:
            vn = f'{self.revision}a'
            if self._is_trial:
                vn = f'trial-{vn}'
            return f'{vn}.{self.file_type}'
    
    @property
    def revision(self) -> str:

        if self.cloud_format is None:
            raise ValueError(
                'Cloud format not set, revision is unknown'
            )
        
        if self.file_type is not None:
            return ''.join((self.cloud_format[0],self.file_type[0],self.version_no))
        else:
            return ''.join((self.cloud_format[0],self.version_no))
        
    @property
    def version_no(self) -> str:

        return self.base_cfg['version_no']

    @property
    def cloud_format(self) -> str:
        return self._check_override('cloud_type','scanned_with') or 'kerchunk'

    @cloud_format.setter
    def cloud_format(self, value):
        self.base_cfg['override']['cloud_type'] = value

    @property
    def file_type(self) -> str:
        """
        Return the file type for this project.
        """

        return self._check_override('file_type','type')
    
    @file_type.setter
    def file_type(self, value):
        
        type_map = {
            'kerchunk': ['json','parq'],
        }
        
        if self.cloud_format in type_map:
            if value in type_map[self.cloud_format]:
                self.base_cfg['override']['file_type'] = value
            else:
                raise ValueError(
                    f'Could not set property "file_type:{value} - accepted '
                    f'values for format: {self.cloud_format} are {type_map.get(self.cloud_format,None)}.'
                )
        else:
            raise ValueError(
                f'Could not set property "file_type:{value}" - cloud format '
                f'{self.cloud_format} does not accept alternate types.'
            )

    @property
    def source_format(self) -> str:
        return self.detail_cfg.get(index='driver', default=None)
    
    def minor_version_increment(self):
        """
        Use this function for when properties of the cloud file have been changed."""
        raise NotImplementedError
    
    def major_version_increment(self):
        """
        Use this function for major changes to the cloud file 
        - e.g. replacement of source file data."""
        raise NotImplementedError
