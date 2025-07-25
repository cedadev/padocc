__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

## PADOCC CLI for entrypoint scripts

import argparse
import yaml

from padocc import GroupOperation, phase_map
from padocc.core.utils import BypassSwitch, get_attribute, list_groups, group_exists

# Ensure all directories are created with 775 permissions
import os
os.umask(0o002)

SHORTCUTS = [
    'list',
    'add',
    'delete',
    'get_log',
    'status',
    'summarise',
    'check_attr',
    'set_attr',
    'complete',
    'pfunc',
    'report',
]

def check_shortcuts(args: dict) -> bool:
    """
    Check and perform any special features requested
    """

    if args.phase == 'list':
        list_groups(args.workdir)
        return True
    
    if not group_exists(args.groupID, args.workdir):
        raise ValueError(
            'Shortcuts can only be performed on existing groups - ' \
            'use `padocc new -G group` to create a new group.'
        )
    
    group = GroupOperation(
        args.groupID,
        args.workdir,
        verbose=args.verbose,
        dryrun=args.dryrun,
        forceful=args.forceful
    )

    if args.phase == 'delete':
        if args.proj_code is not None:
            group.remove_project(args.proj_code, ask=True)
            return True
        group.delete_group()
    
    elif args.phase == 'get_log':
        if args.proj_code is not None:
            proj = group[args.proj_code]
            proj.show_log_contents(
                args.shortcut,
                halt=False)
            return True
        
        for project in group:
            project.show_log_contents(
                args.shortcut,
                halt=True)
    
    elif args.phase == 'add':
        moles_tags = (args.shortcut == 'moles')
        group.add_project(args.input, moles_tags=moles_tags)
    
    elif args.phase == 'status':
        group.summarise_status(repeat_id=args.repeat_id)
    
    elif args.phase == 'summarise':
        group.summarise_data(repeat_id=args.repeat_id)
    
    elif args.phase == 'check_attr':
        group.check_attribute(args.shortcut)
    
    elif args.phase == 'set_attr':
        attr, value = args.shortcut.split(':')
        group.set_all_values(
            attr,
            value,
            repeat_id=args.repeat_id
        )

    elif args.phase == 'complete':
        group.complete_group(
            args.shortcut,
            repeat_id=args.repeat_id,
            thorough=args.thorough)

    elif args.phase == 'pfunc':
        try:
            module = __import__(args.shortcut)
        except ImportError as err:
            print(f'ERROR: Custom module {args.shortcut} could not be imported')

        group.apply_pfunc(
            module, 
            repeat_id=args.repeat_id)

    elif args.phase == 'report':
        if args.proj_code is None:
            # Combine reports for multiple projects.
            report = group.combine_reports(repeat_id=args.repeat_id)
            print(yaml.dump(report))
            return True
        
        proj = group[args.proj_code]
        report = proj.get_report()
        print(report)
        print(yaml.dump(report))
    else:
        pass

    return True

def get_args():
    parser = argparse.ArgumentParser(description='Run a pipeline step for a group of datasets')
    parser.add_argument('phase', type=str, help='Phase of the pipeline to initiate', metavar="See 'All Operations' section of documentation")

    parser.add_argument('--shortcut', dest='shortcut', help='See documentation for use cases.')
    parser.add_argument('--xarray_kwargs', dest='xarray_kwargs', help='Supply kwargs for xarray, comma separated')

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
    parser.add_argument('-s','--subset',    dest='subset',    default=None, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='main', help='Repeat id (main if first time running, <phase>_<repeat> otherwise)')
    parser.add_argument('-p','--proj_code',dest='proj_code',help='Run for a specific project code, within a group or otherwise')

    # Specialised
    parser.add_argument('-C','--cloud-format', dest='mode', default=None, help='Output format required.')
    parser.add_argument('-i','--input', dest='input', help='input file (for init phase)')
    parser.add_argument('-o','--output', dest='output', help='output file for specific shortcut operations.')
    parser.add_argument('--valid', dest='error_bypass',default=None,help='Location of validation error bypass file.')

    # Parallel deployment
    parser.add_argument('--parallel', dest='parallel',action='store_true',help='Add for parallel deployment with SLURM')
    parser.add_argument('--parallel_project', dest='parallel_project',default=None, help='Add for parallel deployment with SLURM for internal project conversion.')
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

    xarray_kwargs = None
    if args.xarray_kwargs is not None:
        xarray_kwargs = {}
        for i in args.xarray_kwargs.split(','):
            attr, val = i.split(':')
            if val == 'pyTrue':
                val = True
            elif val == 'pyFalse':
                val = False

            xarray_kwargs[attr] = val


    if args.phase == 'init' and args.groupID is None:
        print('Error: GroupID must be provided on initialisation')
        return
    
    bypass=BypassSwitch(args.bypass)

    # Generic special features
    if args.phase in SHORTCUTS:
        if check_shortcuts(args):
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
            bypass=bypass,
            xarray_kwargs=xarray_kwargs
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
                new_version=args.new_version,
                xarray_kwargs=args.xarray_kwargs # Unprocessed
            )
            return
        
        run_kwargs = {}
        if args.parallel_project is not None:
            run_kwargs = {
                'compute_subset':args.parallel_project.split('/')[0],
                'compute_total':args.parallel_project.split('/')[1]
            }

        run_kwargs['error_bypass'] = args.error_bypass

        group.run(
            args.phase,
            mode=args.mode,
            repeat_id=args.repeat_id,
            proj_code=args.proj_code,
            subset=args.subset,
            mem_allowed=args.mem_allowed,
            run_kwargs=run_kwargs,
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
            thorough=args.thorough,
            xarray_kwargs=xarray_kwargs
        )

        proj.run(mode=args.mode)

if __name__ == '__main__':
    main()