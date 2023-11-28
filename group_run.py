import sys
import json
import os
import argparse

def output(msg,verb=True, mode=None, log=None, pref=0):
    prefixes = ['INFO','ERR']
    prefix = prefixes[pref]
    if verb:
        if mode == 'log':
            log += f'{prefix}: {msg}\n'
        else:
            print(f'> {prefix}: {msg}')
    return log

def get_group_len(workdir, group):
    # Implement parallel reads from single 'group' file
    with open(f'{workdir}/groups/filelists/{group}.txt') as f:
        group_len = len(list(f.readlines()))
    return group_len

times = {
    'scan':'5:00',
    'compute':'20:00',
    'test':''
}

phases = list(times.keys())

def get_attribute(env, args, var):
    if os.getenv(env):
        return os.getenv(env)
    elif hasattr(args, var):
        return getattr(args, var)
    else:
        print(f'Error: Missing attribute {var}')
        return None

def main(args):

    # Set up main parameters
    phase   = args.phase
    group   = args.group

    WORKDIR  = get_attribute('WORKDIR', args, 'workdir')
    if not WORKDIR:
        output('WORKDIR missing or undefined', verb=args.verbose)
        return None
    SRCDIR   = get_attribute('SRCDIR', args, 'source')
    if not SRCDIR:
        output('SRCDIR missing or undefined', verb=args.verbose)
        return None
    VENV     = get_attribute('KVENV', args, 'kvenv')
    if not VENV:
        output('VENV missing or undefined', verb=args.verbose)
        return None
    GROUPDIR = f'{WORKDIR}/groups/{group}'

    # Establish some group parameters
    group_len          = get_group_len(WORKDIR, group)
    group_phase_sbatch = f'{GROUPDIR}/sbatch/{phase}.sbatch'
    master_script      = f'{SRCDIR}/single_run.py'
    template           = 'templates/phase.sbatch.template'


    # Make Directories
    for dirx in ['sbatch','outs','errs']:
        if not os.path.isdir(f'{GROUPDIR}/{dirx}'):
            os.makedirs(f'{GROUPDIR}/{dirx}')

    if phase not in phases:
        print(f'Error: "{phase}" not recognised, please select from {phases}')
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
        master_script, phase, group
    )
    if args.forceful:
        sb += ' -f'
    if args.verbose:
        sb += ' -v'

    with open(group_phase_sbatch,'w') as f:
        f.write(sb)

    # Submit job array for this group in this phase
    print(f'sbatch --array=0-{group_len} {group_phase_sbatch}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a group of datasets')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('group',type=str, help='Group identifier code')
    parser.add_argument('-w',dest='workdir', help='Working directory for pipeline')
    parser.add_argument('-g',dest='groupdir', help='Group directory for pipeline')
    parser.add_argument('-s',dest='source', help='Path to directory containing master scripts (this one)')
    parser.add_argument('-e',dest='venvpath', help='Path to virtual (e)nvironment (excludes /bin/activate)')
    parser.add_argument('-f',dest='forceful', action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-v',dest='verbose' , action='store_true', help='Print helpful statements while running')
    args = parser.parse_args()

    main(args)

    