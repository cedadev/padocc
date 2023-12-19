import sys
import json
import os
import argparse
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

def get_group_len(workdir, group, repeat_id=1):
    """Implement parallel reads from single 'group' file"""
    with open(f'{workdir}/groups/{group}/proj_codes_{repeat_id}.txt') as f:
        group_len = len(list(f.readlines()))
    return group_len

times = {
    'scan':'5:00',
    'compute':'30:00',
    'validate':'15:00'
}

phases = list(times.keys())

def get_attribute(env, args, var):
    """Assemble environment variable or take from passed argument."""
    if os.getenv(env):
        return os.getenv(env)
    elif hasattr(args, var):
        return getattr(args, var)
    else:
        print(f'Error: Missing attribute {var}')
        return None

def main(args):
    """Assemble sbatch script for parallel running jobs"""

    logger = init_logger(args.verbose, 0, 'main-group')

    # Set up main parameters
    phase   = args.phase
    group   = args.groupID

    WORKDIR  = get_attribute('WORKDIR', args, 'workdir')
    if not WORKDIR:
        logger.error('WORKDIR missing or undefined')
        return None
    SRCDIR   = get_attribute('SRCDIR', args, 'source')
    if not SRCDIR:
        logger.error('SRCDIR missing or undefined')
        return None
    VENV     = get_attribute('KVENV', args, 'kvenv')
    if not VENV:
        logger.error('VENV missing or undefined')
        return None
    GROUPDIR = f'{WORKDIR}/groups/{group}'

    # init not parallelised
    if phase == 'init':
        from pipeline.init import init_config
        logger.info('Running init steps as serial process')
        args.groupdir = GROUPDIR
        args.workdir  = WORKDIR
        args.source   = SRCDIR
        args.venvpath = VENV
        init_config(args)
        return None

    # Establish some group parameters
    group_len          = get_group_len(WORKDIR, group, repeat_id = args.repeat_id)
    group_phase_sbatch = f'{GROUPDIR}/sbatch/{phase}.sbatch'
    master_script      = f'{SRCDIR}/single_run.py'
    template           = 'templates/phase.sbatch.template'


    # Make Directories
    for dirx in ['sbatch','outs','errs']:
        if not os.path.isdir(f'{GROUPDIR}/{dirx}'):
            os.makedirs(f'{GROUPDIR}/{dirx}')

    if phase not in phases:
        logger.error(f'"{phase}" not recognised, please select from {phases}')
        return None

    with open(template) as f:
        sbatch = '\n'.join([r.strip() for r in f.readlines()])

    sb = sbatch.format(
        f'{group}_{phase}_array',             # Job name
        times[phase],                         # Time
        f'{GROUPDIR}/outs/%A/%a.out', # Outs
        f'{GROUPDIR}/errs/%A/%a.err', # Errs
        VENV,
        WORKDIR,
        GROUPDIR,
        master_script, phase, group, times[phase]
    )
    if args.forceful:
        sb += ' -f'
    if args.verbose:
        sb += ' -v'
    if args.bypass:
        sb += ' -b'

    if args.repeat_id:
        sb += f' -r {args.repeat_id}'

    with open(group_phase_sbatch,'w') as f:
        f.write(sb)

    # Submit job array for this group in this phase
    if args.dryrun:
        logger.info('DRYRUN: sbatch command: ')
        print(f'sbatch --array=0-{group_len} {group_phase_sbatch}')
    else:
        os.system(f'sbatch --array=0-{group_len} {group_phase_sbatch}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a group of datasets')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('groupID',type=str, help='Group identifier code')

    parser.add_argument('-s',dest='source', help='Path to directory containing master scripts (this one)')
    parser.add_argument('-e',dest='venvpath', help='Path to virtual (e)nvironment (excludes /bin/activate)')

    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-p','--proj_dir',    dest='proj_dir',      help='Project directory for pipeline')
    parser.add_argument('-n','--new_version', dest='new_version',   help='If present, create a new version')
    parser.add_argument('-m','--mode',        dest='mode', default=None, help='Print or record information (log or std)')
    parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job')
    parser.add_argument('-b','--bypass-errs', dest='bypass', action='store_true', help='Bypass all error messages - skip failed jobs')
    
    parser.add_argument('-i', '--input', dest='input', help='input file (for init phase)')

    parser.add_argument('-S','--subset',    dest='subset',    default=1,   type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='1', help='Repeat id (1 if first time running, <phase>_<repeat> otherwise)')

    parser.add_argument('-f',dest='forceful', action='store_true', help='Force overwrite of steps if previously done')

    parser.add_argument('-v','--verbose',dest='verbose' , action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',  action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )

    args = parser.parse_args()

    main(args)

    