__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import sys
import json
import os
import argparse
import subprocess

from pipeline.logs import init_logger
from pipeline.utils import BypassSwitch, get_attribute, get_codes, times
from pipeline.allocate import assemble_allocations

phases = list(times.keys())

def get_group_len(workdir, group, repeat_id='main') -> int:
    """
    Implement parallel reads from single 'group' file

    :param workdir:     (str) The path of the current pipeline working directory.

    :param group:       (str) The name of the dataset group within the pipeline.

    :param repeat_id:   (int) Repeat-id subset within the group, default is main.

    :returns:           (int) The number of projects within the specified subset 
                        of a group of datasets.
    """

    # Semi-superfluous function, left in only as a doc-string reference.
    codes = get_codes(group, workdir, f'proj_codes/{repeat_id}')
    if codes:
        return len(codes)
    else:
        return 0

def deploy_array_job(args, logger, time=None, label=None, group_len=None):

    # Establish some group parameters
    if not group_len:
        group_len          = get_group_len(args.workdir, args.groupID, repeat_id = args.repeat_id)

    if not label:
        group_phase_sbatch = f'{args.groupdir}/sbatch/{args.phase}.sbatch'
        repeat_id = args.repeat_id
    else:
        group_phase_sbatch = f'{args.groupdir}/sbatch/{args.phase}_{label}.sbatch'
        if label != 'allocations':
            repeat_id = f'{args.repeat_id}/{label}.txt'
        else:
            repeat_id = f'{args.repeat_id}/{label}'

    master_script      = f'{args.source}/single_run.py'
    template           = 'extensions/templates/phase.sbatch.template'

    # Open sbatch template from file.
    with open(template) as f:
        sbatch = '\n'.join([r.strip() for r in f.readlines()])

    # Setup time and memory defaults
    if not time:
        time = times[args.phase]
        if args.time_allowed:
            time = args.time_allowed

    mem = '2G'
    if args.memory:
        mem = args.memory

    jobname = f'{args.groupID}_{args.phase}'
    if label:
        jobname = f'{label}_{args.phase}_{args.groupID}'

    sb = sbatch.format(
        jobname,                              # Job name
        time,                                 # Time
        mem,                                  # Memory
        args.venvpath,
        args.workdir,
        args.groupdir,
        master_script, args.phase, args.groupID, time, mem, repeat_id
    )

    # Additional carry-through flags
    sb += f' -b {args.bypass}'
    if args.forceful:
        sb += ' -f'
    if args.verbose:
        sb += ' -v'
    if args.quality:
        sb += ' -Q'
    if args.backtrack:
        sb += ' -B'
    if args.dryrun:
        sb += ' -d'
    if args.binpack:
        sb += ' -A'

    with open(group_phase_sbatch,'w') as f:
        f.write(sb)

    # Submit job array for this group in this phase
    if args.dryrun:
        logger.info('DRYRUN: sbatch command: ')
        print(f'sbatch --array=0-{group_len-1} {group_phase_sbatch}')
    else:
        os.system(f'sbatch --array=0-{group_len-1} {group_phase_sbatch}')

def main(args) -> None:
    """
    Assemble sbatch script for parallel running jobs and execute. May include
    allocation of multiple tasks to each job if enabled.

    :param args:    (Object) ArgParse object containing all required parameters
                    from default values or specific inputs from command-line.
    
    :returns: None
    """

    logger = init_logger(args.verbose, 0, 'main-group')

    allocations = None

    # Set up main parameters
    phase   = args.phase
    group   = args.groupID

    if phase not in phases:
        logger.error(f'"{phase}" not recognised, please select from {phases}')
        return None

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')
    args.source   = get_attribute('SRCDIR', args, 'source')
    args.venvpath = get_attribute('KVENV', args, 'kvenv')
    args.groupdir = f'{args.workdir}/groups/{group}'

    logger.info(f'Starting group execution for {group}')
    logger.debug('Pipeline variables:')
    logger.debug(f'WORKDIR : {args.workdir}')
    logger.debug(f'GROUPDIR: {args.groupdir}')
    logger.debug(f'SRCDIR  : {args.source}')
    logger.debug(f'VENVDIR : {args.venvpath}')

    # Init not parallelised - run for whole group here
    if phase == 'init':
        from pipeline.init import init_config
        logger.info(f'Running init steps as a serial process for {group}')
        init_config(args)
        return None
    
    # Make Directories - with allocations
    for dirx in ['sbatch','allocations']: # Add allocations
        if not os.path.isdir(f'{args.groupdir}/{dirx}'):
            os.makedirs(f'{args.groupdir}/{dirx}')

    # Experimental bin-packing: Not fully implemented 25/03
    if args.binpack:
        allocations = assemble_allocations(args)
        for alloc in allocations:
            deploy_array_job(args, logger, label=alloc[0], time=alloc[1], group_len=alloc[2])
    else:
        deploy_array_job(args, logger)    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a group of datasets')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('groupID',type=str, help='Group identifier code')

    # Group-run specific
    parser.add_argument('-S','--source', dest='source', help='Path to directory containing master scripts (this one)')
    parser.add_argument('-e','--environ',dest='venvpath', help='Path to virtual (e)nvironment (excludes /bin/activate)')
    parser.add_argument('-i', '--input', dest='input', help='input file (for init phase)')
    parser.add_argument('-A', '--alloc-bins', dest='binpack',action='store_true', help='input file (for init phase)')

    # Action-based - standard flags
    parser.add_argument('-f','--forceful',dest='forceful',action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',  action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )
    parser.add_argument('-Q','--quality', dest='quality', action='store_true', help='Quality assured checks - thorough run')
    parser.add_argument('-b','--bypass-errs', dest='bypass', default='DBSCM', help=BypassSwitch().help())
    parser.add_argument('-B','--backtrack', dest='backtrack', action='store_true', help='Backtrack to previous position, remove files that would be created in this job.')

    # Environment variables
    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-p','--proj_dir',    dest='proj_dir',      help='Project directory for pipeline')

    # Single-job within group
    parser.add_argument('-G','--groupID',   dest='groupID', default=None, help='Group identifier label')
    parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job')
    parser.add_argument('-M','--memory', dest='memory', default='2G', help='Memory allocation for this job (i.e "2G" for 2GB)')
    parser.add_argument('-s','--subset',    dest='subset',    default=1,   type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='main', help='Repeat id (main if first time running, <phase>_<repeat> otherwise)')

    # Specialised
    parser.add_argument('-n','--new_version', dest='new_version',   help='If present, create a new version')
    parser.add_argument('-m','--mode',        dest='mode', default=None, help='Print or record information (log or std)')

    args = parser.parse_args()

    main(args)

    