import sys
import json
import os

def get_group_len(groupdir, group):
    # Implement parallel reads from single 'group' file
    with open(f'{groupdir}/{group}/proj_codes.txt'):
        group_len = len(list(f.readlines()))
    return group_len

scripts = {
    'scan':'pre_process/CFG_dataset_scan.py',
    'compute':'process/CFG_compute.py',
    'test':'post_process/CFG_generic_test.py'
}

times = {
    'scan':'',
    'compute':'',
    'test':''
}

if __name__ == '__main__':
    phase = sys.argv[1]
    group = sys.argv[2]

    WORKDIR = None
    GROUPDIR = os.environ['GROUPDIR']
    VENV=None

    group_len = get_group_len(group)
    group_phase_sbatch = f'{GROUPDIR}/{group}/sbatch/{phase}.sbatch'

    # Make Directories
    os.makedirs(f'{GROUPDIR}/{group}/sbatch')
    os.makedirs(f'{GROUPDIR}/{group}/outs')
    os.makedirs(f'{GROUPDIR}/{group}/errs')

    script = scripts[phase]

    template = f'phase.sbatch.template'
    if not os.path.isfile(template):
        print(f'Error: {phase} is not a known phase')
    else:
        with open(template) as f:
            sbatch = [r.strip() for r in f.readlines()].join('\n')
        sb = sbatch.format(
            f'{group}_{phase}_array',
            times[phase],
            f'{GROUPDIR}/{group}/outs/%A.out',
            f'{GROUPDIR}/{group}/errs/%A.err',
            VENV,
            script,
            group,
            GROUPDIR
        )
        with open(group_phase_sbatch,'w') as f:
            f.write(sbatch)

        # Submit job array for this group in this phase
        os.system(f'sbatch --array=0-{group_len} {group_phase_sbatch}')



    

