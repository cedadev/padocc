__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

## PADOCC CLI for entrypoint scripts

import argparse

from padocc import GroupOperation, phase_map
from padocc.core.utils import BypassSwitch, get_attribute, list_groups

def check_specials(args: dict) -> bool:
    """
    Check and perform any special features requested
    """

    if args.phase == 'list':
        list_groups(args.workdir)
        return True
    
    group = GroupOperation(
            args.groupID,
            args.workdir,
            verbose=args.verbose,
            dryrun=args.dryrun,
            forceful=args.forceful
        )
    
    if args.phase == 'add':
        moles_tags = (args.special == 'moles')
        group.add_project(args.input, moles_tags=moles_tags)
        return True
    
    if args.phase == 'status':
        group.summarise_status()
        return True
    
    if args.phase == 'check':
        group.check_attribute(args.special)
        return True

    if args.phase == 'complete':
        group.complete_group(
            args.special,
            repeat_id=args.repeat_id)
        return True


def get_args():
    parser = argparse.ArgumentParser(description='Run a pipeline step for a group of datasets')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate')

    parser.add_argument('--special', dest='special', help='See documentation for use cases.')

    # Action-based - standard flags
    parser.add_argument('-f','--forceful',dest='forceful',action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',  action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )
    parser.add_argument('-T','--thorough', dest='thorough', action='store_true', help='Thorough processing - start from scratch')
    parser.add_argument('-b','--bypass-errs', dest='bypass', default='D', help=BypassSwitch().help())

    # Environment variables
    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')

    # Single-job within group
    parser.add_argument('-G','--groupID',   dest='groupID', default=None, help='Group identifier label')
    parser.add_argument('-s','--subset',    dest='subset',    default=None,   type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='main', help='Repeat id (main if first time running, <phase>_<repeat> otherwise)')
    parser.add_argument('-p','--proj_code',dest='proj_code',help='Run for a specific project code, within a group or otherwise')

    # Specialised
    parser.add_argument('-C','--cloud-format', dest='mode', default='kerchunk', help='Output format required.')
    parser.add_argument('-i', '--input', dest='input', help='input file (for init phase)')

    # Parallel deployment
    parser.add_argument('--parallel', dest='parallel', action='store_true',help='Add for parallel deployment with SLURM')
    parser.add_argument('-n','--new_version', dest='new_version',   help='If present, create a new version')
    parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job')
    parser.add_argument('--mem-allowed', dest='mem_allowed', default='100MB', help='Memory allowed for Zarr rechunking')
    parser.add_argument('-M','--memory', dest='memory', default='2G', help='Memory allocation for this job (i.e "2G" for 2GB)')
    parser.add_argument('-B','--backtrack', dest='backtrack', action='store_true', help='Backtrack to previous position, remove files that would be created in this job.')
    parser.add_argument('-e','--environ',dest='venvpath', help='Path to virtual (e)nvironment (excludes /bin/activate)')
    parser.add_argument('-A', '--alloc-bins', dest='binpack',action='store_true', help='input file (for init phase)')
    parser.add_argument('--allow-band-increase', dest='band_increase',action='store_true', help='Allow automatic banding increase relative to previous runs.')

    args = parser.parse_args()

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')

    return args

def main():
    """
    Run Command Line functions for PADOCC serial
    processing. Parallel process deployment will 
    be re-added in the full version."""
    args = get_args()

    if args.phase == 'init' and args.groupID is None:
        print('Error: GroupID must be provided on initialisation')
        return
    
    bypass=BypassSwitch(args.bypass)

    # Generic special features
    if check_specials(args):
        return

    if args.groupID is not None:
        group = GroupOperation(
            args.groupID,
            workdir=args.workdir,
            forceful=args.forceful,
            dryrun=args.dryrun,
            thorough=args.thorough,
            label=f'PADOCC-CLI-{args.phase}',
            verbose=args.verbose,
            bypass=bypass
        )

        if args.phase == 'new':
            group.save_files()
            return

        if args.phase == 'init':
            group.init_from_file(args.input)
            return
        
        if args.parallel:
            group.deploy_parallel(
                args.phase,
                source=args.venvpath,
                band_increase=args.band_increase,
                binpack=args.binpack,
                time_allowed=args.time_allowed,
                memory=args.memory,
                subset=args.subset,
                repeat_id=args.repeat_id,
                bypass=args.bypass,
                mode=args.mode,
                new_version=args.new_version
            )
            return

        group.run(
            args.phase,
            mode=args.mode,
            repeat_id=args.repeat_id,
            proj_code=args.proj_code,
            subset=args.subset,
            mem_allowed=args.mem_allowed
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