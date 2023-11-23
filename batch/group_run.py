import sys
import json
import os

def get_group_len(groupdir, group):
    # Implement parallel reads from single 'group' file
    with open(f'{groupdir}/filelists/{group}.txt') as f:
        group_len = len(list(f.readlines()))
    return group_len

scripts = {
    'scan':'pre_process/CFG_dataset_scan.py',
    'compute':'process/CFG_compute.py',
    'test':'post_process/CFG_generic_test.py'
}

times = {
    'scan':'5:00',
    'compute':'',
    'test':''
}

if __name__ == '__main__':
    phase = sys.argv[1]
    group = sys.argv[2]

    WORKDIR = os.environ['WORKDIR']
    GROUPDIR = os.environ['GROUPDIR']
    VENV='/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/build_venv'
    SCRIPTDIR = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder'

    group_len = get_group_len(GROUPDIR, group)
    group_phase_sbatch = f'{GROUPDIR}/{group}/sbatch/{phase}.sbatch'

    # Make Directories
    for dirx in ['sbatch','outs','errs']:
        if not os.path.isdir(f'{GROUPDIR}/{group}/{dirx}'):
            os.makedirs(f'{GROUPDIR}/{group}/{dirx}')

    script = f'{SCRIPTDIR}/{scripts[phase]}'

    template = f'phase.sbatch.template'
    if not os.path.isfile(template):
        print(f'Error: {phase} is not a known phase')
    else:
        with open(template) as f:
            sbatch = '\n'.join([r.strip() for r in f.readlines()])
        sb = sbatch.format(
            f'{group}_{phase}_array',
            times[phase],
            f'{GROUPDIR}/{group}/outs/%A_%a.out',
            f'{GROUPDIR}/{group}/errs/%A_%a.err',
            VENV,
            script,
            group,
            GROUPDIR
        )
        with open(group_phase_sbatch,'w') as f:
            f.write(sb)

        # Submit job array for this group in this phase
        print(f'sbatch --array=0-{group_len} {group_phase_sbatch}')



    

