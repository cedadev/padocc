__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import argparse
import glob
import json
import time
from datetime import datetime
import os
from typing import Union
import random
import yaml
import string
import importlib

from padocc.core.logs import LoggedOperation, clear_loggers
from padocc.core.utils import phases, BypassSwitch, times, format_str

from .group import GroupOperation

"""
SHEPARD: (v1.0)
Serialised Handler for Enabling Padocc Aggregations via Recurrent Deployment
"""

def random_hash(length: int):
    """
    Where necessary, generate a random hash for unique labelling.
    """
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


class ShepardTask:
    """
    Store task-specific features in a Task Object.
    """
    def __init__(
            self, 
            fid: int, 
            groupID: str, 
            old_phase: str, 
            codeset: list, 
            old_allocation: Union[str,None] = None,
            redo: bool = False
        ):
        """
        :param fid:     (int) Ordinal number for a given flock within the current set of flocks.
        
        :param groupID:     (str) ID of the flock on which this task is to be performed.
        
        :param old_phase:   (str) The label for the last phase run for this group subset.
        
        :param codeset:     (list) The set of Project ID codes which this task applies to (group-specific)
        
        :param old_allocation: (str) The last parallel allocation used in deployment."""

        self.fid = fid
        self.groupID = groupID
        self.old_phase = old_phase
        self.codeset = codeset
        self.old_allocation = old_allocation or None

        if redo: 
            self.new_phase = old_phase
        elif old_allocation is None and old_phase != 'validate':
            self.new_phase = phases[phases.index(old_phase) + 1]
        else:
            self.new_phase = old_phase

        self.allowed = True
        self.time, self.memory = self.get_allocation()

    @property
    def uid(self):
        """
        Gives a unique id for each task based on the flock and the allocation.
        
        The combination is unique for each task.
        """
        return f'{self.fid}-{self.old_allocation}'

    def get_allocation(self) -> tuple:
        """
        Determine allocation values for time/memory for this task.
        """
        time_up, mem_up = True, True
        if self.old_allocation is not None:

            # Establish new time allocation
            old_time  = int(self.old_allocation.split(',')[0].split(':')[0])
            increment = int(times[self.new_phase].split(':')[0])

            # Recomputing - special case (first time failed - try only slightly longer.)
            if self.new_phase == 'compute' and old_time == increment:
                new_t = old_time + 10
            else:
                new_t = old_time + increment
                if new_t > 12*60:
                    # Report this as an issue - somehow.
                    time_up = False
                    new_t = 12*60
            new_time = str(new_t) + ':00'

            # Establish new memory allocation
            old_mem = self.old_allocation.split(',')[1]
            new_mem = int(old_mem[0])*2
            if new_mem > 16:
                # Report this as an issue - somehow.
                mem_up = False
                new_mem = 16
            new_memory = str(new_mem) + 'G'
        else:
            new_time = times[self.new_phase]
            new_memory = '2G'

        if not time_up and not mem_up:
            # Allow time/memory increase up to the limit
            self.allowed = False

        return new_time, new_memory
        
class ShepardOperator(LoggedOperation):
    """
    Operator class for Shepard deployments.
    """
    def __init__(
            self, 
            mode: Union[str, None] = None, 
            conf: Union[dict,str,None] = None, 
            verbose: int = 0,
            parallel: bool = False,
            autolog: bool = False,
        ) -> None:
        """
        Initialise a shepard deployment.
        
        :param mode:    (str) Mode of operation, either ``continuous`` or ``batch`` mode.
        
        :param conf:    (str) Path to conf json file or loaded dict, including specific
            SHEPARD parameters.
            
        :param verbose: (int) Level of verbosity for SHEPARD logging.

        :param parallel: (bool) Switch for parallel or serial task handling.
        """

        self.log_label = 'shepard-deploy'

        self.mode = mode

        if isinstance(conf, str):
            self.conf = self._load_config(conf)
        else:
            self.conf = conf

        if self.conf is None:
            raise NotImplementedError(
                'Shepard use without a config file is not enabled.'
            )
        
        # Directory that serves as the working directory for all pipeline operations
        self.flock_dir    = self.conf.get('flock_dir',None)

        # Location to output completed products
        self.complete_dir = self.conf.get('complete_dir',None)

        # Path to a validation template file for use with skipping/bypassing errors in validation.
        self.common_valid = self.conf.get('common_valid',None)

        if self.flock_dir is None:
            raise ValueError(
                'Missing "flock_dir" from config.'
            )
        
        if self.complete_dir is None:
            raise ValueError(
                'Missing "complete_dir" from config.'
            )

        fh = None
        if autolog:
            now = datetime.now()
            if not os.path.isdir(f'{self.flock_dir}/shp_logs'):
                os.makedirs(f'{self.flock_dir}/shp_logs')
            fh = f"{self.flock_dir}/shp_logs/shp_{datetime.strftime(now,'%H%M_%d%m%Y')}"

        super().__init__(label=self.log_label, fh=fh, verbose=verbose)
        
        self.parallel = parallel
        if parallel and not os.environ.get('LOTUS_CFG'):
            raise ValueError(
                'Lotus Configurations missing - please set '
                'LOTUS_CFG environment variable'
            )

        # Create output directories if they do not already exist.
        if not os.path.isdir(f'{self.complete_dir}/summaries'):
            os.makedirs(f'{self.complete_dir}/summaries')
        if not os.path.isdir(f'{self.complete_dir}/reports'):
            os.makedirs(f'{self.complete_dir}/reports')
        if not os.path.isdir(f'{self.complete_dir}/data'):
            os.makedirs(f'{self.complete_dir}/data')

        self.batch_limit = self.conf.get('batch_limit',None) or 100
        self.source_venv = self.conf.get('source_venv', self.default_source)

    @property
    def default_source(self) -> str:
        """
        Default virtual environment is the one currently activated.
        """
        return os.getenv('VIRTUAL_ENV')

    @property
    def bypass(self) -> BypassSwitch:
        """
        Standard bypass switch for shepard operations
        """
        return BypassSwitch('DFLS')

    @property
    def cycle_limit(self) -> int:
        """
        Limit for cycling operations
        """
        return 1000
    
    @property
    def cycle_delay(self) -> int:
        """
        Delay between cycling operations
        """
        return 10
    
    def summarise_flocks(self) -> None:
        """
        Get a top-level view of all flocks.
        
        Single line of information about each flock."""

        total_errs = 0
        for flock in self._init_all_flocks():

            info = []

            flock_err = 0

            status_dict = flock.get_codes_by_status()
            for phase in status_dict.keys():
                if phase == 'complete':
                    info.append('c:' + str(len(status_dict['complete'])))
                    continue

                errored, pending, successful = 0,0,0
                for status in status_dict[phase]:
                    if 'Failed' in status or 'Cancelled' in status or 'Error' in status:
                        errored += len(status_dict[phase][status])
                    elif 'Pending' in status or 'Redo' in status:
                        pending += len(status_dict[phase][status])
                    else:
                        successful += len(status_dict[phase][status])

                msg = f'{phase[0]}: {format_str(errored,3)}'
                msg += format_str(pending,3)
                msg += format_str(successful,3)
                info.append(format_str(msg, 12))

                flock_err += errored
            print(f'{format_str(flock.groupID,10)} -> {" |".join(info)}, ET: {flock_err}')
            total_errs += flock_err
        print(f'Errors: {total_errs}')

    def scrub_errors(self) -> None:
        """
        Identify in each flock any projects that have produced an unexpected error.
        
        These projects are backtracked to the start. WARNING: This is an experimental
        function used to reset the status of whole deployments. Use with great care.
        """

        for flock in self._init_all_flocks():
            quarantine_codes = flock.determine_status_sets('!Pending&!Success')
            self.logger.info(f"Scrub: {len(quarantine_codes)} from {flock.groupID}")
            # Do this before removing any projects
            proj_codes = [flock.proj_codes['main'][qc] for qc in quarantine_codes]

            for proj in proj_codes:
                project = flock[proj]
                project.status_log.set(
                    [project.status_log.get()[0]]
                )
                project.save_files()

    def quarantine(self) -> None:
        """
        Identify in each flock any projects that have produced an unexpected error.
        
        These projects are transferred from their host flock to the quarantined flock.
        """

        quart = GroupOperation(
            'shp_quarantine',
            self.flock_dir,
            label=f'shepard->Qrnt',
            logid='shepard',
            verbose=self._verbose,
        )

        for flock in self._init_all_flocks():
            quarantine_codes = flock.determine_status_sets('!Pending&!Success')
            self.logger.info(f"Quarantine: {len(quarantine_codes)} from {flock.groupID}")
            # Do this before removing any projects
            proj_codes = [flock.proj_codes['main'][qc] for qc in quarantine_codes]

            for pc in proj_codes:
                flock.transfer_project(
                    pc,
                    quart
                )
            
            quart.save_files()
            flock.save_files()

    def delete_logs(self) -> None:
        """
        Delete logs for all groups
        """
        for flock in self._init_all_flocks():
            flock.delete_logs()

    def activate(self) -> None:
        """
        Main operation function to activate the deployment
        """

        mode = self.mode

        if mode == 'batch':
            self.logger.info('Running in single batch mode')
            self.run_batch()
        else:
            self.logger.info('Running in continuous cycle mode')
            for cycle in range(1, self.cycle_limit+1):
                self.logger.info(f'Cycle {cycle}/{self.cycle_limit}')

                # Continuous processing runs all flocks through all processes
                # with each cycle.
                self.run_batch(cycle=cycle)

                time.sleep(self.cycle_delay)
            self.logger.info(f'Cycle limit reached - exiting on {cycle}')

    def run_batch(
            self, 
            cycle: int = 1) -> None:
        """
        Run a batch of processes.
        """

        flocks = self._init_all_flocks()

        if len(flocks) == 0:
            self.logger.info("Exiting - no flocks identified")
            return
        
        self.logger.info("All flocks initialised")

        task_list, total_processes = self._assemble_task_list(flocks, self.batch_limit)

        current = datetime.strftime(datetime.now(), "%y/%m/%d %H:%M:%S")

        if len(task_list) == 0:
            self.logger.info(f'No processes identified: {current}')
            self._complete_flocks(flocks)
            return

        self.logger.info(
            f'Shepard Batch {cycle}: {current} ({total_processes} processes)'
        )
        for task in task_list:

            label = 'Progression'
            if task.old_phase == task.new_phase:
                label = "Redo"

            self.logger.info(
                f' > Group: {task.groupID}, '
                f'{label}: {task.old_phase} -> '
                f'{task.new_phase} [{task.codeset}] ({task.time},{task.memory})'
            )

        self.logger.info('Starting processing jobs')

        for task in task_list:
            self._process_task(task, flocks[task.fid])

        self.logger.info('Finished processing jobs')
        self._complete_flocks(flocks)

    def _complete_flocks(self, flocks: list[GroupOperation]) -> None:
        """
        Run completion steps for candidate flocks.
        
        Will only complete a whole group at a time, so that the group can be deleted.
        """

        for flock in flocks:

            completes = flock.get_codes_by_status()['complete']

            # Candidates for completeness have been validated (not Pending) and succeeded (not Fatal).
            complete_candidates = flock.determine_status_sets(
                '!Fatal&!Pending&!Failed&!ValidationError&!AggregationError', 'validate')

            if len(complete_candidates) + len(completes) != len(flock):
                self.logger.info(f'Flock {flock.groupID}: Not all flock components are in a ready state.')
                continue
            else:
                self.logger.info(f'Flock {flock.groupID}: Accepted for Completion workflow')

                # Summarise and save
                summary = flock.summarise_data(func=None)
                self._write_summary(flock.groupID, summary)

                ## Pre-completion scripting
                if self._pre_completion is not None:
                    mod = importlib.import_module(self._pre_completion['module'])
                    func = getattr(mod, self._pre_completion['function'])
                    func(flock)

                # Complete with thoroughness - complete as job.
                flock.deploy_parallel(
                    'complete',
                    time_allowed='5:00',
                    memory='1G',
                    thorough=True,
                )

        # Separate check for deletion
        for flock in flocks:
                
            complete = flock.get_codes_by_status()['complete']
            if len(complete) != len(flock):
                self.logger.info(f'Flock {flock.groupID}: Not all projects ready for deletion.')
                continue

            # Delete group
            self.logger.info(f'Flock {flock.groupID}: Accepted for deletion.')
            flock.delete_group(ask=False)
            
    def _write_summary(
            self,
            groupID: str,
            summary: str
        ) -> None:
        """
        Write the summary for a group out to some location.
        """
        with open(f'{self.complete_dir}/summaries/{groupID}_summary.txt','w') as f:
            f.write(summary)

    def _process_task(
            self, 
            task: ShepardTask, 
            flock: GroupOperation):
        """
        Process Individual Task Objects.

        A Shepard Task can be processed to act on a specific group.
        """

        if not task.allowed:
            self.logger.error(f'Task {task.uid} not allowed - time or memory allocation exceeded.')
            return

        new_repeat_id = 'main'
        # If we are dealing with a subset.
        if len(task.codeset) != len(flock):
            new_repeat_id = f'progression_{task.new_phase}_{random_hash(6)}'
            # Create the new repeat group
            flock.add_repeat_by_id(
                new_repeat_id,
                task.codeset
            )

        # Non-parallel deployment.
        if not self.parallel:
            flock.run(
                task.new_phase,
                repeat_id=new_repeat_id,
                bypass=self.bypass,
                run_kwargs=self._phase_specific_kwargs(task.new_phase)
            )

        # Parallel deployment
        else:
            self.logger.info(f'{flock.groupID}:{new_repeat_id} - parallel')
            flock.deploy_parallel(
                task.new_phase,
                self.source_venv,
                verbose=self._verbose,
                repeat_id=new_repeat_id,
                joblabel='SHEPARD',
                time_allowed=task.time,
                memory=task.memory,
                **self._phase_specific_kwargs(task.new_phase)
            )

    def _phase_specific_kwargs(
            self,
            phase: str
        ) -> dict:
        """
        Deliver phase-specific run kwargs.
        """

        if phase == 'validate':
            return {'valid':self.common_valid}
        
        if phase == 'scan':
            return {'thorough': True}
        
        if phase == 'compute':
            return {'thorough':False}

        return {}

    def _assemble_task_list(
            self, 
            flocks: list[GroupOperation], 
            batch_limit: int) -> tuple:
        """
        Assemble the task list for the retrieved flocks.
        """

        task_list = []
        processed_flocks = {}
        proj_count = 0
        while proj_count < batch_limit and len(processed_flocks.keys()) < len(flocks):

            fid = random.randint(0, len(flocks)-1)
            while fid in processed_flocks:
                fid = random.randint(0, len(flocks)-1)

            # Extract a random flock at a time.
            flock = flocks[fid]

            # Randomise the set of flocks so we're not missing out any particular flock.
            status_dict = flock.get_codes_by_status(write=True)

            self.logger.debug(f'Obtained status for flock {fid}')
            num_datasets = 0
            for phase in ['init','scan','compute','validate']:

                if phase not in status_dict:
                    continue

                old_allocations = {}

                if 'JobCancelled' in status_dict[phase]:
                    # Need to know what was previously run for this phase for this flock.
                    # Extract last_allocation from each project that can then be incremented.

                    for proj_id in status_dict[phase]['JobCancelled']:
                        project = flock[proj_id]

                        old_allocation = project.base_cfg.get('last_allocation','')
                        if old_allocation in old_allocations:
                            old_allocations[old_allocation].append(proj_id)
                        else:
                            old_allocations[old_allocation] = [proj_id]

                if 'Redo' in status_dict[phase]:
                    task_list.append(
                        ShepardTask(fid, flock.groupID, phase, status_dict[phase]['Redo'], redo=True)
                    )
                    num_datasets += len(status_dict[phase]['Redo'])

                for alloc, codes in old_allocations.items():
                    task_list.append(
                        ShepardTask(fid, flock.groupID, phase, codes, old_allocation=alloc)
                    )
                    num_datasets += len(codes)

                if phase == 'validate':
                    # Cannot progress validation here.
                    continue

                if 'Success' not in status_dict[phase]:
                    # Cannot progress if there are no successful datasets
                    continue

                num_codes = len(status_dict[phase]['Success'])
                if num_codes == 0:
                    continue
                num_datasets += num_codes

                task_list.append(
                        ShepardTask(fid, flock.groupID, phase, status_dict[phase]['Success'])
                    )

            self.logger.debug(f'Obtained task list for flock {fid}')

            processed_flocks[fid] = num_datasets
            proj_count += num_datasets

        for task in task_list:
            self.logger.debug(f'{task.new_phase}: {task.codeset}')
            
        return task_list, proj_count

    def _init_all_flocks(self) -> list[GroupOperation]:
        """
        Find and initialise all flocks.
        """
        group_proj_codes = self._find_flocks()
        shp_flock = []
        self.logger.info(f'Discovering {len(group_proj_codes)} flocks')
        for idx, flock_path in enumerate(group_proj_codes):
            # Flock path is the path to the main.txt proj_code 
            # document for each group.
            self.logger.debug(f'Locating flock {idx}')

            groupdir = flock_path.replace('/proj_codes/main.txt','')

            if self._flock_quarantined(groupdir):
                # Skip quarantined flocks.
                self.logger.info(f' > Skipping flock {idx}')
                continue

            self.logger.debug(f'Instantiating flock {idx}')
            group = groupdir.split('/')[-1]

            flock = GroupOperation(
                group,
                self.flock_dir,
                label=f'shepard->{group}',
                logid='shepard',
                verbose=self._verbose,
            )

            # Delete all previous flock repeat_ids
            flock.delete_all_repeat_ids()

            shp_flock.append(flock)

        return shp_flock

    def _find_flocks(self) -> list[str]:
        """
        Locate all directories with the proj_codes/main.txt file.
        """
        
        if not os.path.isdir(self.flock_dir):
            raise ValueError(
                f'Flock Directory: {self.flock_dir} - inaccessible.'
            )
        
        return glob.glob(f'{self.flock_dir}/**/proj_codes/main.txt', recursive=True)

    def _flock_quarantined(self, groupdir):
        """
        Determine if a given flock has a .shpignore file in its 
        group directory."""

        return os.path.isfile(os.path.join(groupdir,'.shpignore'))

    def _load_config(self, conf: str) -> Union[dict,None]:
        """
        Load a conf.yaml file to a dictionary
        """

        if conf is None:
            return None

        if os.path.isfile(conf):
            with open(conf) as f:
                config = yaml.safe_load(f)
            return config
        else:
            raise FileNotFoundError(f'Config file {conf} unreachable')

def _get_cmdline_args():
    """
    Get command line arguments passed to shepard
    """

    parser = argparse.ArgumentParser(description='Entrypoint for SHEPARD module')
    parser.add_argument('mode', type=str, help='Operational mode, either `batch` or `continuous`')
    parser.add_argument('--conf',type=str, help='Config file as part of deployment')
    parser.add_argument('-v','--verbose', action='count', default=0, help='Set level of verbosity for logs')
    parser.add_argument('--parallel', dest='parallel',action='store_true',help='Add for parallel deployment with SLURM')
    parser.add_argument('--autolog', dest='autolog',action='store_true',help='Auto-generate logs (CRON)')

    args = parser.parse_args()

    return {
        'mode': args.mode,
        'conf': args.conf,
        'verbose': args.verbose,
        'parallel': args.parallel,
        'autolog': args.autolog
    }

def main():

    kwargs = _get_cmdline_args()

    shepherd = ShepardOperator(**kwargs)
    if shepherd.mode == 'delete':
        shepherd.delete_logs()
    elif shepherd.mode == 'quarantine':
        shepherd.quarantine()
    elif shepherd.mode == 'scrub':
        shepherd.scrub_errors()
    elif shepherd.mode == 'status':
        shepherd.summarise_flocks()
    else:
        shepherd.activate()

if __name__ == '__main__':
    main()