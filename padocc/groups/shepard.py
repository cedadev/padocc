__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

"""
SHEPARD:
Serialised Handler for Enabling Padocc Aggregations via Recurrent Deployment
"""

import os
import yaml
import argparse
from typing import Union
import glob
import json
import time
from datetime import datetime

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
        if self.flock_dir is None:
            raise ValueError(
                'Missing "flock_dir" from config.'
            )

        # Shepard Files
        # - workdir for operations.
        # - path to a group file.

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
                self.run_batch(cycle=cycle)
                time.sleep(self.cycle_delay)

        self.logger.info('Operation complete')

    def run_batch(
            self, 
            batch_limit: int = 100, 
            cycle: int = 1) -> None:
        """
        Run a batch of processes.
        """

        batch_limit = self.conf.get('batch_limit',None) or batch_limit

        # Initialise all groups if needed (outside of batch limit)

        flocks = self._init_all_flocks()
        self.logger.info("All flocks initialised")

        task_list, total_processes = self._assemble_task_list(flocks, batch_limit)

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

        del flocks
        del task_list
        #clear_loggers(ignore=[self.log_label])

    def _process_task(
            self, 
            task: ShepardTask, 
            flock: GroupOperation):
        """
        Process Individual Tasks
        """

        # Create Repeat ID for the given task
        # Execute the correct phase with that given repeat ID

        new_repeat_id = f'progression_{task.new_phase}'

        flock.repeat_by_status(
            'Success',
            new_repeat_id,
            task.old_phase
        )

        flock.run(
            task.new_phase,
            repeat_id=new_repeat_id,
            bypass=self.bypass
        )

    def _assemble_task_list(
            self, 
            flocks: list[GroupOperation], 
            batch_limit: int) -> tuple:
        """
        Assemble the task list for the retrieved flocks
        """

        task_list = []
        proj_count = 0
        for fid, flock in enumerate(flocks):
            status_dict = flock.get_codes_by_status()

            for phase in ['init','scan','compute']:
                if 'Success' not in status_dict[phase]:
                    continue

                num_codes = len(status_dict[phase]['Success'])

                if num_codes == 0:
                    continue

                task_list.append(
                        ShepardTask(fid, flock.groupID, phase, num_codes)
                    )

                proj_count += num_codes

                if proj_count > batch_limit:
                    break

        return task_list, proj_count

    def _init_all_flocks(self):
        """
        Initialise and find all flocks
        """
        shepard_files = self.find_flocks()
        missed_flocks = []
        shp_flock = []
        self.logger.info(f'Discovering {len(shepard_files)} flocks')
        for idx, flock_path in enumerate(shepard_files):
            flock_file = flock_path.split('/')[-1]
            try:
                fconf = self.open_flock(flock_path)
                self.logger.info(f' > Accessed flock {idx+1}')
            except ValueError as err:
                missed_flocks.append((flock_path, err))
                continue

            flock = GroupOperation(
                fconf['groupID'],
                fconf['workdir'],
                label=f'shepard->{fconf["groupID"]}',
                verbose=self._verbose,
            )

            if not flock.datasets.get():
                self.logger.info(f' > Creating flock {idx+1}: {flock_file}')
                flock.init_from_file(fconf['group_file'], substitutions=fconf['substitutions'])
            else:
                self.logger.debug(f' > Skipped creating existing flock: {fconf["groupID"]}')

            shp_flock.append(flock)

        # Handle missed flocks here.

        return shp_flock

    def open_flock(self, file: str):

        if not os.path.isfile(file):
            raise ValueError(f'Unable to open {file}')
        
        with open(file) as f:
            return json.load(f)

    def find_flocks(self):
        
        if not os.path.isdir(self.flock_dir):
            raise ValueError(
                f'Flock Directory: {self.flock_dir} - inaccessible.'
            )
        
        return glob.glob(f'{self.flock_dir}/*.shp', recursive=True)

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