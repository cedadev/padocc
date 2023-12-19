# Go through the set of outputs under a specific directory
# Determine which project codes to rerun for a given phase
# Output how many are in each section

# Take workdir and groupID as inputs, also phase
# Save proj_codes_<vn>.txt as list of incomplete projects

__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import argparse
import os
import glob
import logging

levels = [
    logging.WARN,
    logging.INFO,
    logging.DEBUG
]

def init_logger(verbose, mode, name):
    """Logger object init and configure with formatting"""
    verbose = min(verbose, len(levels)-1)

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

phases = ['scan', 'compute', 'validate']
checks = ['/detail-cfg.json','/*kerchunk*','*.json']

def get_attribute(env, args, var):
    if os.getenv(env):
        return os.getenv(env)
    elif hasattr(args, var):
        return getattr(args, var)
    else:
        print(f'Error: Missing attribute {var}')
        return None
    
def find_redos(phase, workdir, groupID, check, ignore=[]):
    checkdir = f'{workdir}/in_progress/{groupID}/'
    proj_codes = os.listdir(checkdir)

    if phase == 'validate':
        checkdir = f'{args.workdir}/complete/{args.groupID}/'
    redo_pcodes = []
    complete = []
    for pcode in proj_codes:
        check_file = checkdir + pcode + check
        if pcode not in ignore:
            if glob.glob(check_file):
                if phase == 'validate':
                    complete.append(pcode)
                else:
                    pass
            else:
                redo_pcodes.append(pcode)
    return redo_pcodes, complete

def main(args):
    # Assemble directory
    # Check each project for correct output file
    
    logger = init_logger(args.verbose, 0, 'identify')

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')


    if args.phase not in phases:
        logger.error(f'Phase not accepted here - {args.phase}')
        return None
    else:
        logger.info(f'Discovering dataset progress within group {args.groupID}')
        redo_pcodes = []
        for index, phase in enumerate(phases):
            redo_pcodes, completes = find_redos(phase, args.workdir, args.groupID, checks[index], ignore=redo_pcodes)
            logger.info(f'{phase}: {len(redo_pcodes)} datasets')
            if completes:
                logger.info(f'Complete: {len(completes)} datasets')
            if phase == args.phase:
                break
    
    # Write pcodes
    id = 1
    new_projcode_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{args.phase}_{id}.txt'
    while os.path.isfile(new_projcode_file):
        id += 1
        new_projcode_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{args.phase}_{id}.txt'

    if not args.soft:
        with open(new_projcode_file,'w') as f:
            f.write('\n'.join(redo_pcodes))

        # Written new pcodes
        print(f'Written {len(redo_pcodes)} pcodes to {new_projcode_file}')
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('phase', type=str,    help='Phase of the pipeline to initiate')
    parser.add_argument('groupID', type=str,  help='Group identifier label')
    parser.add_argument('-w',dest='workdir',  help='Working directory for pipeline')
    parser.add_argument('-s',dest='soft',action='store_true', help='View mode only (soft check)')
    parser.add_argument('-v','--verbose',dest='verbose', action='count', default=0, help='Print helpful statements while running')

    args = parser.parse_args()

    main(args)