__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import sys
import json
import os
import argparse
import subprocess

from pipeline.logs import init_logger
from pipeline.utils import BypassSwitch, get_attribute, get_codes
from pipeline.allocate import create_allocation

times = {
    'scan'    :'10:00', # No prediction possible prior to scanning
    'compute' :'60:00',
    'validate':'30:00' # From CMIP experiments - no reliable prediction mechanism possible
}

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

def main(args) -> None:
    """
    Assemble sbatch script for parallel running jobs and execute. May include
    allocation of multiple tasks to each job if enabled.

    :param args:    (Object) ArgParse object containing all required parameters
                    from default values or specific inputs from command-line.
    
    :returns: None
    """

    logger = init_logger(args.verbose, 0, 'main-group')

    # Set up main parameters
    phase   = args.phase
    group   = args.groupID

    if phase not in phases:
        logger.error(f'"{phase}" not recognised, please select from {phases}')
        return None

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')
    SRCDIR        = get_attribute('SRCDIR', args, 'source')
    VENV          = get_attribute('KVENV', args, 'kvenv')

    args.groupdir = f'{args.workdir}/groups/{group}'

    logger.info(f'Starting group execution for {group}')
    logger.debug('Pipeline variables:')
    logger.debug(f'WORKDIR : {args.workdir}')
    logger.debug(f'GROUPDIR: {args.groupdir}')
    logger.debug(f'SRCDIR  : {SRCDIR}')
    logger.debug(f'VENVDIR : {VENV}')

    # Experimental bin-packing: Not fully implemented 25/03
    if args.binpack:
        create_allocation(args)

    # Init not parallelised - run for whole group here
    if phase == 'init':
        from pipeline.init import init_config
        logger.info(f'Running init steps as a serial process for {group}')
        args.source   = SRCDIR
        args.venvpath = VENV
        init_config(args)
        return None

    # Establish some group parameters
    group_len          = get_group_len(args.workdir, group, repeat_id = args.repeat_id)
    group_phase_sbatch = f'{args.groupdir}/sbatch/{phase}.sbatch'
    master_script      = f'{SRCDIR}/single_run.py'
    template           = 'extensions/templates/phase.sbatch.template'

    # Make Directories
    for dirx in ['sbatch','outs','errs']: # Add allocations
        if not os.path.isdir(f'{args.groupdir}/{dirx}'):
            os.makedirs(f'{args.groupdir}/{dirx}')

    # Open sbatch template from file.
    with open(template) as f:
        sbatch = '\n'.join([r.strip() for r in f.readlines()])

    # Setup time and memory defaults
    time = times[phase]
    if args.time_allowed:
        time = args.time_allowed
    mem = '2G'
    if args.memory:
        mem = args.memory

    # Suppressed since now manually logging with changing filehandler.
    #outdir = f'{args.workdir}/groups/args.groupID/outs/raw/%A_%a.out'
    #errdir = f'{args.workdir}/groups/{args.groupID}/errs/raw/%A_%a.out'

    #os.system(f'rm -rf {outdir}/*')
    #os.system(f'rm -rf {errdir}/*')

    sb = sbatch.format(
        f'{group}_{phase}_array',             # Job name
        time,                                 # Time
        mem,                                  # Memory
        #outdir,
        #errdir,
        VENV,
        args.workdir,
        args.groupdir,
        master_script, phase, group, time, mem, args.repeat_id
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

    if args.repeat_id:
        sb += f' -r {args.repeat_id}'

    with open(group_phase_sbatch,'w') as f:
        f.write(sb)

    # Submit job array for this group in this phase
    if args.dryrun:
        logger.info('DRYRUN: sbatch command: ')
        print(f'sbatch --array=0-{group_len-1} {group_phase_sbatch}')
    else:
        os.system(f'sbatch --array=0-{group_len-1} {group_phase_sbatch}')

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

    