__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

"""
SHEPARD:
Serialised Handler for Enabling Padocc Aggregations via Recurrent Deployment
"""

import argparse
import glob
import json
import time
from datetime import datetime
import os
from typing import Union
import random
import yaml

from padocc.core.logs import LoggedOperation, clear_loggers
from padocc.core.utils import phases, BypassSwitch

from .group import GroupOperation

shepard_template = {
    'workdir': '/my/workdir',
    'group_file': '/my/group/file.csv',
    'groupID': 'my-group1',
    'substitutions':['a','b']
}

class ShepardTask:
    def __init__(self, fid: str, groupID: str, old_phase: str, num_codes: int):

        self.fid = fid
        self.groupID = groupID
        self.old_phase = old_phase
        self.new_phase = phases[phases.index(old_phase) + 1]
        self.num_codes = num_codes

class ShepardOperator(LoggedOperation):
    """
    Operator class for Shepard deployments
    """

    def __init__(
            self, 
            mode: Union[str, None] = None, 
            conf: Union[dict,str,None] = None, 
            verbose: int = 0
        ) -> None:

        self.log_label = 'shepard-deploy'

        super().__init__(label=self.log_label,verbose=verbose)

        self.mode = mode

        if isinstance(conf, str):
            self.conf = self._load_config(conf)
        else:
            self.conf = conf

        if self.conf is None:
            raise NotImplementedError(
                'Shepard use without a config file is not enabled.'
            )
        
        self.flock_dir = self.conf.get('flock_dir',None)
        self.batch_limit = self.conf.get('batch_limit',None) or 100
        self.source_venv = self.conf.get('source_venv', self.default_source)
        if self.flock_dir is None:
            raise ValueError(
                'Missing "flock_dir" from config.'
            )

        # Shepard Files
        # - workdir for operations.
        # - path to a group file.

    @property
    def default_source(self):
        return os.getenv('VIRTUAL_ENV')

    @property
    def bypass(self):
        """
        Standard bypass switch for shepard operations
        """
        return BypassSwitch('DFLS')

    @property
    def cycle_limit(self):
        """
        Limit for cycling operations
        """
        return 1000
    
    @property
    def cycle_delay(self):
        """
        Delay between cycling operations
        """
        return 10

    def activate(self, mode: str = None):
        """
        Main operation function to activate the deployment
        """

        mode = mode or self.mode

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
            return

        self.logger.info(
            f'Shepard Batch {cycle}: {current} ({total_processes} processes)'
        )
        for task in task_list:
            self.logger.info(
                f' > Group: {task.groupID}, '
                f'Progression: {task.old_phase} -> '
                f'{task.new_phase} [{task.num_codes}]'
            )

        self.logger.info('Starting processing jobs')

        for task in task_list:
            flock = flocks[task.fid]
            self._process_task(task, flock)

        self.logger.info('Finished processing jobs')

        # Follow through with completion/deletion workflows
        self._complete_flocks(flocks)

        del flocks
        del task_list
        #clear_loggers(ignore=[self.log_label])

    def _complete_flocks(self, flocks):
        """
        Identify completed flocks.
        
        Will only complete a whole group at a time, so that the group can be deleted.
        """

        for flock in flocks:

            # Complete candidate check.
            # All datasets in the group must have passed the validate section.
            #  - To have passed, the status of validation must either be:
            #     > Passed with Warnings - Warn
            #     > Success
            #  - If any datasets have not passed these criteria the group is skipped (for now)
            # Note that this is already skipping quarantined flocks with the .shpignore file.

            is_complete = True
            status_dict = flock.get_codes_by_status()
            complete_candid = status_dict.get('validate',{})
            ccount = 0
            for candidate in complete_candid.keys():
                if 'Warn' not in candidate and 'Success' not in candidate:
                    is_complete = False
                else:
                    ccount += 1
            if ccount < len(flock) or not is_complete:
                continue

            self.logger.info('Flock now acceptable for Completion workflow')

    def _process_task(
            self, 
            task: ShepardTask, 
            flock: GroupOperation):
        """
        Process Individual Tasks.
        """

        # Create Repeat ID for the given task
        # Execute the correct phase with that given repeat ID

        new_repeat_id = f'progression_{task.new_phase}'

        flock.repeat_by_status(
            'Success',
            new_repeat_id,
            task.old_phase
        )

        # Non-parallel deployment.
        flock.run(
            task.new_phase,
            repeat_id=new_repeat_id,
            bypass=self.bypass
        )

        # Parallel deployment
        flock.deploy_parallel(
            task.new_phase,
            self.source_venv,
            verbose=self._verbose,
            repeat_id=new_repeat_id,
        )

    def _assemble_task_list(
            self, 
            flocks: list[GroupOperation], 
            batch_limit: int) -> tuple:
        """
        Assemble the task list for the retrieved flocks
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
            status_dict = flock.get_codes_by_status()

            self.logger.debug(f'Obtained status for flock {fid}')
            num_datasets = 0
            for phase in ['init','scan','compute']:
                if 'Success' not in status_dict[phase]:
                    continue

                num_codes = len(status_dict[phase]['Success'])

                if num_codes == 0:
                    continue

                task_list.append(
                        ShepardTask(fid, flock.groupID, phase, num_codes)
                    )

                num_datasets += num_codes

            self.logger.debug(f'Obtained task list for flock {fid}')

            processed_flocks[fid] = num_datasets
            proj_count += num_datasets

        return task_list, proj_count

    def _init_all_flocks(self):
        """
        Initialise and find all flocks
        """
        group_proj_codes = self.find_flocks()
        missed_flocks = []
        shp_flock = []
        self.logger.info(f'Discovering {len(group_proj_codes)} flocks')
        for idx, flock_path in enumerate(group_proj_codes):
            # Flock path is the path to the main.txt proj_code 
            # document for each group.

            groupdir = flock_path.replace('/proj_codes/main.txt','')
            group = groupdir.split('/')[-1]

            flock = GroupOperation(
                group,
                self.flock_dir,
                label=f'shepard->{group}',
                logid='shepard',
                verbose=self._verbose,
            )

            if self._flock_quarantined(groupdir):
                # Skip quarantined flocks.
                continue

            # Skip Creating flocks as they must be created using the normal creation mechanism.
            #if not flock.datasets.get():
            #    self.logger.info(f' > Creating flock {idx+1}: {flock_file}')
            #    flock.init_from_file(fconf['group_file'], substitutions=fconf['substitutions'])
            #else:
            #    self.logger.debug(f' > Skipped creating existing flock: {fconf["groupID"]}')

            shp_flock.append(flock)

        # Handle missed flocks here.

        return shp_flock

    def find_flocks(self):
        
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

    args = parser.parse_args()

    return {
        'mode': args.mode,
        'conf': args.conf,
        'verbose': args.verbose,
    }

def main():

    kwargs = _get_cmdline_args()

    shepherd = ShepardOperator(**kwargs)
    shepherd.activate()

if __name__ == '__main__':
    main()