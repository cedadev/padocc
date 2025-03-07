__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging
import os
from typing import Callable, Optional, Union

import yaml

from padocc.core import BypassSwitch, FalseLogger, ProjectOperation
from padocc.core.filehandlers import CSVFileHandler, ListFileHandler
from padocc.core.mixins import DirectoryMixin
from padocc.core.utils import format_str, print_fmt_str
from padocc.phases import (KNOWN_PHASES, ComputeOperation, KerchunkDS,
                           ScanOperation, ValidateOperation, ZarrDS)

from .mixins import (AllocationsMixin, EvaluationsMixin, InitialisationMixin,
                     ModifiersMixin)

COMPUTE = {
    'kerchunk':KerchunkDS,
    'zarr':ZarrDS,
    'CFA': ComputeOperation,
}

class GroupOperation(
        AllocationsMixin, 
        DirectoryMixin, 
        InitialisationMixin, 
        EvaluationsMixin,
        ModifiersMixin
    ):

    def __init__(
            self, 
            groupID : str,
            workdir : str = None, 
            forceful : bool = None,
            dryrun   : bool = None,
            thorough : bool = None,
            logger   : logging.Logger | FalseLogger = None,
            bypass   : BypassSwitch = BypassSwitch(),
            label    : str = None,
            fh       : str = None,
            logid    : str = None,
            verbose  : int = 0,
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

        :param logger:          (logging.Logger | FalseLogger) An existing logger object.
                                    
        :param bypass:              (BypassSwitch) instance of BypassSwitch class containing multiple
                                    bypass/skip options for specific events. See utils.BypassSwitch.

        :param label:       (str) The label to apply to the logger object.

        :param fh:          (str) Path to logfile for logger object generated in this specific process.

        :param logid:       (str) ID of the process within a subset, which is then added to the name
            of the logger - prevents multiple processes with different logfiles getting loggers confused.

        :param verbose:         (int) Level of verbosity for log messages (see core.init_logger).

        :returns: None

        """

        if label is None:
            label = 'group-operation'

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

        self._setup_directories()

        self.proj_codes      = {}
        self.faultlist = CSVFileHandler(
            self.groupdir,
            'faultlist',
            logger=self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful,
        )

        self.datasets = CSVFileHandler(
            self.groupdir,
            'datasets',
            logger=self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful,
        )

        self._load_proj_codes()
    
    def __str__(self):
        return f'<PADOCC Group: {self.groupID}>'
    
    def __repr__(self):
        return yaml.dump(self.info())

    def __len__(self):
        """
        Shorthand for length of the list-like group.
        """
        return len(self.proj_codes['main'])
    
    def __getitem__(self, index: Union[int,str]) -> ProjectOperation:
        """
        Indexable group allows access to individual projects
        """
        if isinstance(index, int):
            proj_code = self.proj_codes['main'][index]
        else:
            proj_code = index
            
        return self.get_project(proj_code)
    
    def complete_group(
            self, 
            move_to: str,
            repeat_id: str = 'main'
        ):
        """
        Complete all projects for a group.
        """

        self.logger.info("Verifying completion directory exists")
        if not os.path.isdir(move_to):
            os.makedirs(move_to)

        if not os.access(move_to, os.W_OK):
            raise OSError(
                f'Directory {move_to} is not writable'
            )
        
        proj_list = self.proj_codes[repeat_id].get()
        self.logger.info(
            f"Completing {len(proj_list)}/{len(self)} "
            f"projects for {self.groupID}"
        )

        for proj in proj_list:
            proj_op = self[proj]
            proj_op.complete_project(move_to)

    
    def get_stac_representation(
            self, 
            stac_mapping: dict, 
            repeat_id: str = 'main'
        ) -> list:
        """
        Obtain all records for all projects in this group.
        """
        record_set = []
        proj_list = self.proj_codes[repeat_id].get()

        for proj in proj_list:
            proj_op = self[proj]
            record_set.append(
                proj_op.get_stac_representation(stac_mapping)
            )
        return record_set
    
    @property
    def proj_codes_dir(self):
        return f'{self.groupdir}/proj_codes'

    def info(self) -> dict:
        """
        Obtain a dictionary of key values for this object.
        """
        values = {
            'workdir': self.workdir,
            'groupdir': self.groupdir,
            'projects': len(self.proj_codes['main']),
            'logID': self._logid,
        }

        return {
            self.groupID: values
        }

    @classmethod
    def help(cls, func: Callable = print_fmt_str):
        func('Group Operator')
        func(
            ' > group.get_stac_representation() - Provide a mapper and obtain values '
            'in the form of STAC records for all projects'
        )
        func(' > group.info() - Obtain a dictionary of key values')
        func(' > group.run() - Run a specific operation across part of the group.')
        func(' > group.save_files() - Save any changes to any files in the group as part of an operation')
        func(' > group.check_writable() - Check if all directories are writable for this group.')

        for cls in GroupOperation.__bases__:
            cls.help(func)

    def run(
            self,
            phase: str,
            mode: str = 'kerchunk',
            repeat_id: str = 'main',
            proj_code: Optional[str] = None,
            subset: Optional[str] = None,
            bypass: Union[BypassSwitch, None] = None,
            forceful: Optional[bool] = None,
            thorough: Optional[bool] = None,
            dryrun: Optional[bool] = None,
            run_kwargs: Union[dict,None] = None,
            **kwargs,
        ) -> dict[str]:

        bypass = bypass or self._bypass
        run_kwargs = run_kwargs or {}

        self._set_fh_kwargs(forceful=forceful, dryrun=dryrun, thorough=thorough)

        phases = {
            'scan': self._scan_config,
            'compute': self._compute_config,
            'validate': self._validate_config,
        }

        jobid = None
        if os.getenv('SLURM_ARRAY_JOB_ID'):
            jobid = f"{os.getenv('SLURM_ARRAY_JOB_ID')}-{os.getenv('SLURM_ARRAY_TASK_ID')}"

        # Select set of datasets from repeat_id

        if phase not in phases:
            self.logger.error(f'Unrecognised phase "{phase}" - choose from {phases.keys()}')
            return
        
        codeset = self.proj_codes[repeat_id].get()
        if subset is not None:
            codeset = self._configure_subset(codeset, subset, proj_code)

        if proj_code is not None:
            if proj_code in codeset:
                self.logger.info(f'Project code: {proj_code}')
                codeset = [proj_code]
            elif proj_code.isnumeric():
                if abs(int(proj_code)) > len(codeset):
                    raise ValueError(
                        'Invalid project code specfied. If indexing, '
                        f'must be less than {len(codeset)-1}'
                    )
                # Perform by index
                codeset = [codeset[int(proj_code)]]

        func = phases[phase]

        results = {}
        for id, proj_code in enumerate(codeset):
            self.logger.info(f'Starting operation: {id+1}/{len(codeset)} ({format_str(proj_code, 15, concat=True, shorten=True)})')
        
            fh = 'PhaseLog'

            logid = id
            if jobid is not None:
                logid = jobid

            status = func(
                proj_code, 
                mode=mode, 
                logid=logid, 
                label=phase, 
                fh=fh, 
                bypass=bypass,
                run_kwargs=run_kwargs,
                **kwargs)
            
            if status in results:
                results[status] += 1
            else:
                results[status] = 1

        self.logger.info("Pipeline execution finished")
        for r in results.keys():
            self.logger.info(f'{r}: {results[r]}')

        self.save_files()
        return results

    def _scan_config(
            self,
            proj_code: str,
            mode: str = 'kerchunk',
            bypass: Union[BypassSwitch,None] = None,
            run_kwargs: Union[dict,None] = None,
            **kwargs
        ) -> None:
        """
        Configure scanning and access main section, ensure a few key variables are set
        then run scan_dataset.
        
        :param args:        (obj) Set of command line arguments supplied by argparse.

        :param logger:      (obj) Logging object for info/debug/error messages. Will create a new 
                            logger object if not given one.

        :param fh:          (str) Path to file for logger I/O when defining new logger.

        :param logid:       (str) If creating a new logger, will need an id to distinguish this logger
                            from other single processes (typically n of N total processes.)

        :returns:   None
        """
        scan = ScanOperation(
            proj_code, self.workdir, groupID=self.groupID,
            verbose=self._verbose, bypass=bypass, 
            dryrun=self._dryrun, **kwargs)
        status = scan.run(mode=mode, **self.fh_kwargs, **run_kwargs)
        scan.save_files()
        return status

    def _compute_config(
            self, 
            proj_code: str,
            mode: str = 'kerchunk',
            bypass: Union[BypassSwitch,None] = None,
            run_kwargs: Union[dict,None] = None,
            **kwargs
        ) -> None:
        """
        serves as main point of configuration for processing/conversion runs. Can
        set up kerchunk or zarr configurations, check required files are present.

        :param args:        (obj) Set of command line arguments supplied by argparse.

        :param logger:      (obj) Logging object for info/debug/error messages. Will create a new 
                            logger object if not given one.

        :param fh:          (str) Path to file for logger I/O when defining new logger.

        :param logid:       (str) If creating a new logger, will need an id to distinguish this logger
                            from other single processes (typically n of N total processes.)

        :param overide_type:    (str) Set as JSON/parq/zarr to specify output cloud format type to use.
        
        :returns:   None
        """

        self.logger.debug('Finding the suggested mode from previous scan where possible')

        mode = mode or self[proj_code].cloud_format
        if mode is None:
            mode = 'kerchunk'

        if mode not in COMPUTE:
            raise ValueError(
                f'Mode "{mode}" not recognised, must be one of '
                f'"{list(COMPUTE.keys())}"'
            )
        
        # Compute uses separate classes per mode (KerchunkDS, ZarrDS)
        # So the type of ds that the project operation entails is different.
        # We then don't need to provide the 'mode' to the .run function because
        # it is implicit for the DS class.

        ds = COMPUTE[mode]

        compute = ds(
            proj_code, self.workdir, groupID=self.groupID,
            verbose=self._verbose, bypass=bypass,
            dryrun=self._dryrun, **kwargs
        )

        status = compute.run(mode=mode, **self.fh_kwargs, **run_kwargs)
        compute.save_files()
        return status
    
    def _validate_config(
            self, 
            proj_code: str,  
            mode: str = 'kerchunk',
            bypass: Union[BypassSwitch,None] = None,
            run_kwargs: Union[dict,None] = None,
            **kwargs
        ) -> None:

        self.logger.debug(f"Starting validation for {proj_code}")

        try:
            valid = ValidateOperation(
                proj_code, self.workdir, groupID=self.groupID,
                verbose=self._verbose, bypass=bypass,
                dryrun=self._dryrun, **kwargs)
        except TypeError:
            raise ValueError(
                f'{proj_code}, {self.groupID}, {self.workdir}'
            )
        
        status = valid.run(
            mode=mode,
            **self.fh_kwargs,
            **run_kwargs
        )
        valid.save_files()
        return status

    def _save_proj_codes(self):
        for pc in self.proj_codes.keys():
            self.proj_codes[pc].close()

    def save_files(self):
        """
        Save all files associated with this group.
        """
        self.faultlist.close()
        self.datasets.close()
        self._save_proj_codes()

    def _add_proj_codeset(self, name : str, newcodes : list):
        self.proj_codes[name] = ListFileHandler(
            self.proj_codes_dir,
            name,
            init_value=newcodes,
            logger=self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful
        )
    
    def _delete_proj_codeset(self, name: str):
        """
        Delete a project codeset
        """

        if name == 'main':
            self.logger.debug(
                'Removing the main codeset '
                'cannot be achieved using this function - skipped.'
            )
            return
        
        if name not in self.proj_codes:
            self.logger.warning(
                f'Subset ID "{name}" could not be deleted - no matching subset.'
            )

        self.proj_codes[name].remove_file()
        self.proj_codes.pop(name)

    def check_writable(self):
        if not os.access(self.workdir, os.W_OK):
            self.logger.error('Workdir provided is not writable')
            raise IOError("Workdir not writable")
        
        if not os.access(self.groupdir, os.W_OK):
            self.logger.error('Groupdir provided is not writable')
            raise IOError("Groupdir not writable")

    def _load_proj_codes(self):
        """
        Load all current project code files for this group
        into Filehandler objects
        """
        import glob

        # Check filesystem for objects
        proj_codes = [g.split('/')[-1].strip('.txt') for g in glob.glob(f'{self.proj_codes_dir}/*.txt')]

        if not proj_codes:
            # Running for the first time
            self._add_proj_codeset(
                'main', 
                self.datasets
            )
            
        for p in proj_codes:
            self.proj_codes[p] = ListFileHandler(
                self.proj_codes_dir, 
                p, 
                logger=self.logger,
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
            else:
                os.makedirs(codes_dir)

    def _configure_subset(self, main_set, subset_size: int, subset_id: int):
        # Configure subset controls
        
        start = subset_size * subset_id
        if start < 0:
            raise ValueError(
                f'Improperly configured subset size: "{subset_size}" (1+)'
                f' or id: "{subset_id}" (0+)'
            )
        
        end = subset_size * (subset_id + 1)
        if end > len(main_set):
            end = len(main_set)

        if end < start:
            raise ValueError(
                f'Improperly configured subset size: "{subset_size}" (1+)'
                f' or id: "{subset_id}" (0+)'
            )

        return main_set[start:end]

