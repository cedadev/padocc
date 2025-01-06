__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

## PADOCC CLI for entrypoint scripts

import argparse

from padocc.core.utils import BypassSwitch
from padocc.operations import GroupOperation
from padocc import phase_map

def get_args():
    parser = argparse.ArgumentParser(description='Run a pipeline step for a group of datasets')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate')

    # Action-based - standard flags
    parser.add_argument('-f','--forceful',dest='forceful',action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',  action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )
    parser.add_argument('-T','--thorough', dest='thorough', action='store_true', help='Thorough processing - start from scratch')
    parser.add_argument('-b','--bypass-errs', dest='bypass', default='DBSCL', help=BypassSwitch().help())

    # Environment variables
    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')

    # Single-job within group
    parser.add_argument('-G','--groupID',   dest='groupID', default=None, help='Group identifier label')
    parser.add_argument('-s','--subset',    dest='subset',    default=1,   type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='main', help='Repeat id (main if first time running, <phase>_<repeat> otherwise)')

    # Specialised
    parser.add_argument('-C','--cloud-format', dest='mode', default='kerchunk', help='Output format required.')
    parser.add_argument('-i', '--input', dest='input', help='input file (for init phase)')

    # Unused v1.3
    parser.add_argument('-n','--new_version', dest='new_version',   help='If present, create a new version')
    parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job')
    parser.add_argument('-M','--memory', dest='memory', default='2G', help='Memory allocation for this job (i.e "2G" for 2GB)')
    parser.add_argument('-B','--backtrack', dest='backtrack', action='store_true', help='Backtrack to previous position, remove files that would be created in this job.')
    parser.add_argument('-e','--environ',dest='venvpath', help='Path to virtual (e)nvironment (excludes /bin/activate)')
    parser.add_argument('-A', '--alloc-bins', dest='binpack',action='store_true', help='input file (for init phase)')
    parser.add_argument('--allow-band-increase', dest='band_increase',action='store_true', help='Allow automatic banding increase relative to previous runs.')

    args = parser.parse_args()
    return args

def main():
    """
    Run Command Line functions for PADOCC serial
    processing. Parallel process deployment will 
    be re-added in the full version."""
    args = get_args()

    if args.groupID is not None:
        group = GroupOperation(
            args.groupID,
            workdir=args.workdir,
            forceful=args.forceful,
            dryrun=args.dryrun,
            thorough=args.thorough,
            label=f'PADOCC-CLI-{args.phase}',
            verbose=args.verbose,
            bypass=args.bypass
        )

        if args.phase == 'init':
            group.init_from_file(args.input_file)
            return

        group.run(
            args.phase,
            mode=args.mode,
            repeat_id=args.repeat_id,
            proj_code=args.proj_code,
            subset=args.subset,
        )

    else:

        if args.phase not in phase_map:
            print(f'Error: Unrecognised phase "{args.phase}" - must be one of {list(phase_map.keys())}')
            return
        
        operation = phase_map[args.phase]
        if isinstance(operation, dict):
            # Multiple choice
            if args.mode not in operation:
                print(f'Error: Unrecognised cloud format "{args.mode}" - must be one of {list(operation.keys())}')
                return
            
            operation = operation[args.mode]

        proj = operation(
            args.proj_code,
            args.workdir,
            bypass=args.bypass,
            label=f'PADOCC-CLI-{args.proj_code}',
            verbose=args.verbose,
            forceful=args.forceful,
            dryrun=args.dryrun,
            thorough=args.thorough
        )

        proj.run(mode=args.mode)

if __name__ == '__main__':
    main()