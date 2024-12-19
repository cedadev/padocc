__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
import logging

from padocc.core import BypassSwitch, FalseLogger
from padocc.core.utils import format_str, times
from padocc.core import ProjectOperation
from padocc.phases import (
    ScanOperation,
    KerchunkDS, 
    ZarrDS, 
    cfa_handler,
    KNOWN_PHASES,
)
from padocc.core.mixins import DirectoryMixin
from padocc.core.filehandlers import CSVFileHandler, TextFileHandler

from .mixins import AllocationsMixin, InitialisationMixin, EvaluationsMixin

COMPUTE = {
    'kerchunk':KerchunkDS,
    'zarr':ZarrDS,
    'cfa': cfa_handler,
}

class GroupOperation(
        AllocationsMixin, 
        DirectoryMixin, 
        InitialisationMixin, 
        EvaluationsMixin
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
    
    def __str__(self):
        return f'<PADOCC Group: {self.groupID}>'
    
    def __repr__(self):
        return str(self)

    @property
    def proj_codes_dir(self):
        return f'{self.groupdir}/proj_codes'

    @property
    def new_inputfile(self):
        if self.groupID:
            return f'{self.workdir}/groups/filelists/{self.groupID}.txt'
        else:
            raise NotImplementedError
        
    def values(self):
        print(f'Group: {self.groupID}')
        print(f' - Workdir: {self.workdir}')
        print(f' - Groupdir: {self.groupdir}')

        super().values()

    def info(self):
        print(f'Group: {self.groupID}')
        print('General Methods:')
        print(f' > group.run() - Run a specific operation across part of the group.')
        print(f' > group.init_from_file() - Initialise the group based on an input csv file')
        print(f' > group.init_from_stac() - Initialise the group based on a STAC index')
        print(f' > group.add_project() - Add an new project/dataset to this group')
        print(f' > group.save_files() - Save any changes to any files in the group as part of an operation')
        print(f' > group.check_writable() - Check if all directories are writable for this group.')
        
        self._assess_info()

    def run(
            self,
            phase,
            mode='kerchunk',
            repeat_id='main',
            proj_code=None,
            subset=None,
            **kwargs
        ):

        phases = {
            'scan': self._scan_config,
            'compute': self._compute_config,
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
            codeset = self.configure_subset(codeset, subset, proj_code)

        elif proj_code in codeset:
            self.logger.info(f'Project code: {proj_code}')
            codeset = [proj_code]

        func = phases[phase]

        results = {}
        for id, proj_code in enumerate(codeset):
            self.logger.info(f'Starting operation: {id+1}/{len(codeset)} ({format_str(proj_code, 15, concat=True, shorten=True)})')
        
            fh = None

            logid = id
            if jobid is not None:
                logid = jobid
                fh = 'PhaseLog'

            status = func(proj_code, mode=mode, logid=logid, label=phase, fh=fh, **kwargs)
            if status in results:
                results[status] += 1
            else:
                results[status] = 1

        self.logger.info("Pipeline execution finished")
        for r in results.keys():
            self.logger.info(f'{r}: {results[r]}')

        self.save_files()

    def _scan_config(
            self,
            proj_code,
            mode='kerchunk',
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

        so = ScanOperation(
            proj_code, self.workdir, groupID=self.groupID,
            verbose=self._verbose, **kwargs, dryrun=self._dryrun)
        status = so.run(mode=mode, subset_bypass=False)
        so.save_files()
        return status

    def _compute_config(
            self, 
            proj_code,  
            mode=None,
            subset_bypass=False,
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
        proj_op = ProjectOperation(
            proj_code,
            self.workdir,
            groupID=self.groupID,
            logger=self.logger,
            **kwargs,
        )

        mode = proj_op.cloud_format
        if mode is None:
            mode = 'kerchunk'

        del proj_op

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

        proj_op = ds(
            proj_code,
            self.workdir,
            groupID=self.groupID,
            logger=self.logger,
            **kwargs
        )
        status = proj_op.run(subset_bypass=subset_bypass)
        proj_op.save_files()
        return status
    
    def add_project(self):
        pass

    def _save_proj_codes(self):
        for pc in self.proj_codes.keys():
            self.proj_codes[pc].close()

    def save_files(self):
        self.blacklist_codes.close()
        self.datasets.close()
        self._save_proj_codes()

    def _add_proj_codeset(self, name : str, newcodes : list):
        self.proj_codes[name] = TextFileHandler(
            self.proj_codes_dir,
            name,
            self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful
        )

        self.proj_codes[name].set(newcodes)

    def check_writable(self):
        if not os.access(self.workdir, os.W_OK):
            self.logger.error('Workdir provided is not writable')
            raise IOError("Workdir not writable")
        
        if not os.access(self.groupdir, os.W_OK):
            self.logger.error('Groupdir provided is not writable')
            raise IOError("Groupdir not writable")

    def create_sbatch(
            self,
            phase     : str,
            source    : str = None,
            venvpath  : str = None,
            band_increase : str = None,
            forceful   : bool = None,
            dryrun     : bool = None,
            quality    : bool = None,
            verbose    : int = 0,
            binpack    : bool = None,
            time_allowed : str = None,
            memory       : str = None,
            subset       : int = None,
            repeat_id    : str = 'main',
            bypass       : BypassSwitch = BypassSwitch(),
            mode         : str = 'kerchunk',
            new_version  : str = None,
        ) -> None:

        if phase not in KNOWN_PHASES:
            raise ValueError(
                f'"{phase}" not recognised, please select from {KNOWN_PHASES}'
            )
            return None

        array_job_kwargs = {
            'forceful': forceful,
            'dryrun'  : dryrun,
            'quality' : quality,
            'verbose' : verbose,
            'binpack' : binpack,
            'time_allowed' : time_allowed,
            'memory'  : memory,
            'subset'  : subset,
            'repeat_id' : repeat_id,
            'bypass' : bypass,
            'mode' : mode,
            'new_version' : new_version,
        }

        # Perform allocation assignments here.
        if not time_allowed:
            allocations = self.create_allocations(
                phase, repeat_id,
                band_increase=band_increase, binpack=binpack
            )

            for alloc in allocations:
                print(f'{alloc[0]}: ({alloc[1]}) - {alloc[2]} Jobs')

            deploy = input('Deploy the above allocated dataset jobs with these timings? (Y/N) ')
            if deploy != 'Y':
                raise KeyboardInterrupt

            for alloc in allocations:
                self._create_job_array(
                    phase, source, venvpath, alloc[2]
                    **array_job_kwargs,
                )
        else:
            num_datasets = len(self.proj_codes[repeat_id].get())
            self.logger.info(f'All Datasets: {time_allowed} ({num_datasets})')

            # Always check before deploying a significant number of jobs.
            deploy = input('Deploy the above allocated dataset jobs with these timings? (Y/N) ')
            if deploy != 'Y':
                raise KeyboardInterrupt

            self._create_job_array(
                    phase, source, venvpath, num_datasets,
                    **array_job_kwargs,
                )

    def _create_job_array(
            self,
            phase,
            source,
            venvpath,
            group_length=None,
            repeat_id='main',
            forceful=None,
            verbose=None,
            dryrun=None,
            quality=None,
            bypass=None,
            binpack=None,
            time_allowed=None,
            memory=None,
            subset=None,
            mode=None,
            new_version=None,
            time=None,
            joblabel=None,
        ):

        sbatch_dir = f'{self.dir}/sbatch/'
        if not joblabel:
            sbatch_file = f'{phase}.sbatch'
        else:
            sbatch_file = f'{phase}_{joblabel}.sbatch'
            repeat_id = f'{repeat_id}/{joblabel}'

        sbatch = TextFileHandler(sbatch_dir, sbatch_file, self.logger, dryrun=self._dryrun, forceful=self._forceful)

        master_script = f'{source}/single_run.py'

        if time is None:
            time = time_allowed or times[phase]
        mem = '2G' or memory

        jobname = f'PADOCC_{self.groupID}_{phase}'
        if joblabel:
            jobname = f'PADOCC_{joblabel}_{phase}_{self.groupID}'

        outfile = f'{self.dir}/outs/{jobname}_{repeat_id}'
        errfile = f'{self.dir}/errs/{jobname}_{repeat_id}'

        sbatch_kwargs = self._sbatch_kwargs(
            time,
            memory,
            repeat_id,
            bypass=bypass, 
            forceful= forceful or self._forceful, 
            verbose = verbose or self._verbose,
            quality = quality or self._quality, # Check
            dryrun = dryrun or self._dryrun,
            binpack = binpack,
            subset = subset,
            new_version = new_version,
            mode = mode,
        )
        
        sbatch_contents = [
            '#!/bin/bash',
            '#SBATCH --partition=short-serial',
            f'#SBATCH --job-name={jobname}',

            f'#SBATCH --time={time}',
            f'#SBATCH --mem={mem}',

            f'#SBATCH -o {outfile}',
            f'#SBATCH -e {errfile}',

            f'module add jaspy',
            f'source {venvpath}/bin/activate',

            f'export WORKDIR={self.workdir}',

            f'python {master_script} {phase} $SLURM_ARRAY_TASK_ID {sbatch_kwargs}',
        ]

        sbatch.update(sbatch_contents)
        sbatch.close()

        if self._dryrun:
            self.logger.info('DRYRUN: sbatch command: ')
            print(f'sbatch --array=0-{group_length-1} {sbatch.filepath()}')

    def _sbatch_kwargs(
            self, time, memory, repeat_id, verbose=None, bypass=None, 
            subset=None, new_version=None, mode=None, **bool_kwargs):
        sbatch_kwargs = f'-G {self.groupID} -t {time} -M {memory} -r {repeat_id}'

        bool_options = {
            'forceful' : '-f',
            'quality'  : '-Q',
            'dryrun'   : '-d',
            'binpack'  : '-A',
        }

        value_options = {
            'bypass' : ('-b',bypass),
            'subset' : ('-s',subset),
            'mode'   : ('-m',mode),
            'new_version': ('-n',new_version),
        }

        optional = []

        if verbose is not None:
            verb = 'v' * int(verbose)
            optional.append(f'-{verb}')

        for value in value_options.keys():
            if value_options[value][1] is not None:
                optional.append(' '.join(value_options[value]))

        for kwarg in bool_kwargs.keys():
            if kwarg not in bool_options:
                raise ValueError(
                    f'"{kwarg}" option not recognised - '
                    f'please choose from {list(bool_kwargs.keys())}'
                )
            optional.append(bool_options[kwarg])

        return sbatch_kwargs + ' '.join(optional)

    def _load_proj_codes(self):
        import glob
        # Check filesystem for objects
        proj_codes = [g.split('/')[-1].strip('.txt') for g in glob.glob(f'{self.proj_codes_dir}/*.txt')]

        if not proj_codes:
            # Running for the first time
            self._add_proj_codeset(
                'main', 
                self.datasets.get()
            )
            
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
