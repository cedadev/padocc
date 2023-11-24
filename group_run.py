import sys
import json
import os
import argparse

def get_group_len(groupdir, group):
    # Implement parallel reads from single 'group' file
    with open(f'{groupdir}/filelists/{group}.txt') as f:
        group_len = len(list(f.readlines()))
    return group_len

times = {
    'scan':'5:00',
    'compute':'',
    'test':''
}

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
    SRCDIR   = get_attribute('SRCDIR', args, 'source')
    VENV     = get_attribute('KVENV', args, 'kvenv')
    GROUPDIR = f'{WORKDIR}/groups/{group}'

    # Establish some group parameters
    group_len          = get_group_len(GROUPDIR, group)
    group_phase_sbatch = f'{GROUPDIR}/{group}/sbatch/{phase}.sbatch'
    master_script      = f'{SRCDIR}/single_run.py'
    template           = 'templates/phase.sbatch.template'


    # Make Directories
    for dirx in ['sbatch','outs','errs']:
        if not os.path.isdir(f'{GROUPDIR}/{group}/{dirx}'):
            os.makedirs(f'{GROUPDIR}/{group}/{dirx}')

    if phase not in phases:
        print(f'Error: "{phase}" not recognised, please select from {phases}')
        return None

    with open(template) as f:
        sbatch = '\n'.join([r.strip() for r in f.readlines()])
    sb = sbatch.format(
        f'{group}_{phase}_array',
        times[phase],
        f'{GROUPDIR}/{group}/outs/%A_%a.out',
        f'{GROUPDIR}/{group}/errs/%A_%a.err',
        VENV,
        master_script,
        group
    )
    with open(group_phase_sbatch,'w') as f:
        f.write(sb)

    # Submit job array for this group in this phase
    print(f'sbatch --array=0-{group_len} {group_phase_sbatch}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a group of datasets')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('group',type=str, help='Group identifier code')
    parser.add_argument('-w',dest='workdir', help='Working directory for pipeline')
    parser.add_argument('-s',dest='source', help='Path to directory containing master scripts (this one)')
    parser.add_argument('-e',dest='venvpath', help='Path to virtual (e)nvironment (excludes /bin/activate)')
    args = parser.parse_args()

    main(args)

    