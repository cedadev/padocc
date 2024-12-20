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

from padocc.core.logs import LoggedOperation
from padocc.operations.group import GroupOperation

shepard_template = {
    'workdir': '/my/workdir',
    'group_file': '/my/group/file.csv',
    'groupID': 'my-group1',
    'substitutions':['a','b']
}

class ShepardOperator(LoggedOperation):

    def __init__(self, conf: Union[dict,None] = None, verbose: int = 0) -> None:

        self.conf = self._load_config(conf)

        if self.conf is None:
            raise NotImplementedError(
                'Shepard use without a config file is not enabled.'
            )
        
        self.flock_dir = self.conf.get('flock_dir',None)
        if self.flock_dir is None:
            raise ValueError(
                'Missing "flock_dir" from config.'
            )
        
        super().__init__(label='shepard-deploy',verbose=verbose)

        # Shepard Files
        # - workdir for operations.
        # - path to a group file.

    def run_batch(self, batch_limit: int = 100):

        batch_limit = self.conf.get('batch_limit',None) or batch_limit

        # Initialise all groups if needed (outside of batch limit)

        flock = self._init_all_flocks()

        self.logger.info("All flocks initialised")

    def _init_all_flocks(self):
        shepard_files = self.find_flocks()
        missed_flocks = []
        shp_flock = []
        for idx, shp in enumerate(shepard_files):
            self.logger.info(f'Instantiating flock {idx+1}: {shp}')
            try:
                fconf = self.open_flock(shp)
            except ValueError as err:
                missed_flocks.append((shp, err))
                continue

            flock = GroupOperation(
                fconf['groupID'],
                fconf['workdir'],
                label=f'shepard->{fconf["groupID"]}',
                verbose=self._verbose,
            )

            if not flock.datasets.get():
                flock.init_from_file(fconf['group_file'], substitutions=fconf['substitutions'])
            else:
                self.logger.info(f'Skipped existing flock: {fconf["groupID"]}')

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
        
        return glob.glob(f'{self.flock_dir}/**/*.shp', recursive=True)

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
            self.logger.error(f'Config file {conf} unreachable')
            return None

def _get_cmdline_args():
    """
    Get command line arguments passed to shepard
    """

    parser = argparse.ArgumentParser(description='Entrypoint for SHEPARD module')
    parser.add_argument('--conf',type=str, help='Config file as part of deployment')
    parser.add_argument('-v','--verbose', action='count', default=0, help='Set level of verbosity for logs')

    args = parser.parse_args()

    return {
        'conf': args.conf,
        'verbose': args.verbose}

def main():

    kwargs = _get_cmdline_args()

    shepherd = ShepardOperator(**kwargs)
    shepherd.run_batch()

if __name__ == '__main__':
    main()