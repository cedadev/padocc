
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import sys
import argparse
import os

def run_init(args):
    from scripts.init import init_config
    init_config(args)

def run_scan():
    pass

drivers = {
    'init':run_init,
    'scan':run_scan
}

def main(args):
    if os.getenv('WORKDIR'):
        args.workdir = os.getenv('WORKDIR')

    if not args.workdir:
        print('Error: No working directory given as input or from environment')
        return None

    # Run the specified phase
    if args.phase in drivers:
        drivers[args.phase](args)
    else:
        print(f'Error: "{args.phase}" not recognised, please select from {list(drivers.keys())}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('proj_code',type=str, help='Project identifier code')
    parser.add_argument('-w',dest='workdir', help='Working directory for pipeline')
    parser.add_argument('-g',dest='groupID', help='Group identifier label')
    args = parser.parse_args()
    main(args)

    