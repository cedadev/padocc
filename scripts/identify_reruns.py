# Go through the set of outputs under a specific directory
# Determine which project codes to rerun for a given phase
# Output how many are in each section

# Take workdir and groupID as inputs, also phase
# Save proj_codes_<vn>.txt as list of incomplete projects

import argparse
import os


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

    if check == 'validate':
        checkdir = f'{args.workdir}/complete/{args.groupID}/'
    redo_pcodes = []
    for pcode in proj_codes:
        check_file = checkdir + pcode + check
        if (not os.path.isfile(check_file)) and (pcode not in ignore):
            redo_pcodes.append(pcode)
    return redo_pcodes

def main(args):
    # Assemble directory
    # Check each project for correct output file

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')

    if args.phase not in phases:
        print('Phase not accepted here - ',args.phase)
        return None
    else:
        redo_pcodes = []
        x=-1
        while phases[x] != args.phase:
            x += 1
            redo_pcodes = find_redos(phases[x], args.workdir, args.groupID, checks[x], ignore=redo_pcodes)
            print(phases[x], len(redo_pcodes))
    
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

    args = parser.parse_args()

    main(args)