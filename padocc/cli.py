__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

## PADOCC CLI for entrypoint scripts

import argparse
import yaml
from typing import Union

from padocc import GroupOperation
from padocc.core.logs import set_verbose
from padocc.core.utils import (
    BypassSwitch, get_attribute, list_groups, 
    group_exists, BASE_CFG, DETAIL_CFG)

# Ensure all directories are created with 775 permissions
import os
os.umask(0o002)

def listgroups(workdir: str, **kwargs):
    """
    List groups in a working directory"""
    list_groups(workdir)

def add_to_group(group, moles_tags: bool = False, input: Union[str,None] = None, **kwargs):
    """Add projects to a group"""

    if input is None:
        raise ValueError('Missing input file `-i`')

    group.add_project(input, moles_tags=moles_tags)
    group.save_files()

def delete_group(
        group: Union[GroupOperation,None],
        proj_code: Union[str,None] = None,
        ask: bool = True,
        **kwargs
    ):
    """Delete projects or whole groups"""
    
    if proj_code is not None:
        group.remove_projects(proj_code, ask=ask)
        return
    
    group.delete_group(ask=ask)

def get_logs(group, proj_code: Union[str,None] = None, log_phase: Union[str,None] = None, **kwargs):
    """
    Get the logs for a specific phase from a project in a group.
    """
    if proj_code is not None:
        proj = group[proj_code]
        proj.show_log_contents(
            log_phase,
            halt=False)
        return True
    
    for project in group:
        project.show_log_contents(
            log_phase,
            halt=True)
        
def get_aggregations(group, repeat_id: str = 'main', **kwargs):
    """Get status of a group, with display parameters"""
    group.summarise_aggregations(repeat_id=repeat_id)

def get_status(group, repeat_id: str = 'main', 
               long_display: bool = False, display_upto: str = '5', 
               separate_errors: bool = False, **kwargs):
    """Get status of a group, with display parameters"""
    display_upto = int(display_upto)

    group.summarise_status(repeat_id=repeat_id, 
                           long_display=long_display, display_upto=display_upto,
                           separate_errors=separate_errors)

def get_summary(group, repeat_id: str = 'main', **kwargs):
    """Get data summary for group"""
    group.summarise_data(repeat_id=repeat_id)

def check_attribute(group, attr: str, repeat_id: str = 'main', **kwargs):
    """Check attribute across whole group"""

    file_data_source = None
    attr_parts = attr.split('.')
    if attr_parts[0] in BASE_CFG:
        file_data_source = 'base_cfg'
    elif attr_parts[0] in DETAIL_CFG:
        file_data_source = 'detail_cfg'
    elif getattr(group[0], attr):
        pass
    else:
        raise ValueError(
            f'Unable to check attribute "{attr}", not found in project'
        )
    
    for project in group.proj_codes[repeat_id]:
        proj = group[project]
        if file_data_source is None:
            value = getattr(proj, attr)
        else:
            value = getattr(proj, file_data_source)
            for ap in attr_parts:
                value = value[ap]
            
        print(f'{proj.proj_code} - {attr}: {value}')

def set_attribute(group, attr: str, value: str, repeat_id: str = 'main', proj_code: str = None,**kwargs):
    """
    Check attribute name against known attributes and set value"""
    file_data_source = None
    attr_parts = attr.split('.')
    if attr_parts[0] in BASE_CFG:
        file_data_source = 'base_cfg'
    elif attr_parts[0] in DETAIL_CFG:
        file_data_source = 'detail_cfg'
    elif getattr(group[0], attr):
        pass
    else:
        raise ValueError(
            f'Unable to set attribute "{attr}", not found in project'
        )
    
    if value == 'True':
        value = True
    elif value == 'False':
        value = False
    elif value == 'None':
        value = None
    
    def recursive_reset(src, attrset, value):
        if len(attrset) > 1:
            src[attrset[0]] = recursive_reset(src[attrset[0]], attrset[1:], value)
        else:
            src[attrset[0]] = value
        return src
    
    codes = group.proj_codes[repeat_id]
    if proj_code is not None:
        codes = proj_code.split(',')
    
    for project in codes:
        proj = group[project]
        if file_data_source is None:
            setattr(proj, attr, value)
        else:
            fh = getattr(proj, file_data_source)
            fh.set(recursive_reset(fh.get(), attr_parts, value))

            # Ensure filehandler is preserved above so it doesn't get reset as a non-dict here
            setattr(proj, file_data_source, fh)
            proj.save_files()

def complete_group(
        group, 
        completion_dir: str, 
        proj_code: Union[str,None] = None, 
        repeat_id: str = 'main', 
        thorough: bool = True,
        remote_sub: Union[str,None] = None,
        remote_replace: Union[str,None] = None,
        **kwargs
    ):
    """Complete projects in a group"""
    complete_kwargs = {}

    if remote_sub is not None:
        complete_kwargs['sub'] = remote_sub
    if remote_replace is not None:
        complete_kwargs['replace'] = remote_replace

    if proj_code is None:
        group.complete_group(
            completion_dir,
            repeat_id=repeat_id,
            thorough=thorough,
            **complete_kwargs)
    else:
        try:
            project = group[proj_code]
            project.complete_project(move_to=completion_dir, thorough=thorough,**complete_kwargs)
        except:
            print(f'ERROR: Unable to instantiate project {proj_code} from {group.groupID}')

def apply_pfunc(group, **kwargs):
    raise NotImplementedError

def report_group(group, proj_code: Union[str,None] = None, repeat_id: str = 'main', **kwargs):
    """Obtain report for a project or whole group"""
    if proj_code is None:
        # Combine reports for multiple projects.
        report = group.combine_reports(repeat_id=repeat_id)
        print(yaml.dump(report))
        return
    
    if proj_code.isnumeric():
        proj_code = group.proj_codes[repeat_id][int(proj_code)]
    
    proj = group[proj_code]
    report = proj.get_report()
    print(yaml.dump(report))
    print(proj.dataset.filepath)
    
def repeat_subset(group, old_phase: str, old_status: str, new_repeat_id: str, old_repeat_id: str = 'main', **kwargs):
    """Create group subset for repeat process."""
    group.repeat_by_status(
        status=old_status,
        new_repeat_id=new_repeat_id,
        phase=old_phase,
        old_repeat_id=old_repeat_id,
    )
    group.save_files()

def update_project_status(group, old_phase: str, old_status: str, repeat_id: str = 'main', new_status: str = 'Redo', new_phase: Union[str,None] = None, **kwargs):
    """Manually update project statuses"""

    if new_phase is None:
        new_phase = old_phase

    status_set = group.determine_status_sets(
        old_status, # Status
        old_phase, # Phase
        repeat_id)
    
    for pc in status_set:
        proj = group[pc]

        proj.update_status(
            new_phase,
            new_status
        )
        proj.save_files()
    group.save_files()

def transfer_projects(group, proj_code: Union[str,None] = None, repeat_id: str = 'main', new_group: str = '', **kwargs):
    """Transfer projects between groups"""

    transfer_group = GroupOperation(
        new_group,
        workdir=group.workdir
    )

    group.transfer_projects(proj_code, transfer_group, repeat_id)
    group.save_files()

def new_group(group, **kwargs):
    """Save newly created group"""
    group.save_files()

def init_group(group, run_kwargs: Union[dict,None] = None, **kwargs):
    """Initialise group from input file"""

    if run_kwargs.get('input_file',None) is None:
        raise ValueError('Missing input file `-i`')

    group.init_from_file(run_kwargs.get('input_file'))
    group.save_files()

def scan_group(group, **kwargs):
    group.run('scan',**kwargs)

def compute_group(
        group, 
        mem_allowed: Union[str,None] = None, 
        **kwargs):
    group.run('compute', mem_allowed=mem_allowed, **kwargs)

def validate_group(
        group, 
        run_kwargs: Union[dict,None] = None,
        **kwargs):
    """Perform `scan`, `compute` or `validate` from this function."""

    group.run('validate', run_kwargs=run_kwargs,**kwargs)

OPERATIONS = {
    'add':add_to_group,
    'delete': delete_group,
    'logs': get_logs,
    'status': get_status,
    'summarise': get_summary,
    'check_attr': check_attribute,
    'set_attr': set_attribute,
    'complete': complete_group,
    'pfunc': apply_pfunc,
    'report': report_group,
    'repeat': repeat_subset,
    'update_status': update_project_status,
    'transfer': transfer_projects,
    'new': new_group,
    'init': init_group,
    'scan': scan_group,
    'compute': compute_group,
    'validate': validate_group,
    'aggregations': get_aggregations,
}

# Only actions allowed for groups that don't yet exist.
NEW_GROUP_ACTIONS = [
    'add',
    'init',
    'new',
    'transfer'
]

def parse_group(
        operation: str,
        groupID  : str,
        workdir  : str,
        verbose  : int = 0,
        bypass   : str = '',
        repeat_id : str = 'main',
        parallel : bool = False,
        forceful : bool = False,
        dryrun   : bool = False,
        thorough : bool = False,
        binpack  : bool = False,
        band_increase: bool = False,
        proj_code: Union[str,None] = None,
        venvpath : Union[str,None] = None,
        aggregator : Union[str,None] = None,
        subset : Union[str,None] = None,
        time_allowed: Union[str,None] = None,
        memory: Union[str,None] = None,
        input_file: Union[str,None] = None,
        xarray_kwargs_raw: Union[str,None] = None,
        parallel_project: Union[str,None] = None,
        identical_dims: Union[str,None] = None,
        concat_dims: Union[str,None] = None,
        wait_sbatch: bool = False,
        new_version: bool = False,
        b64vars: Union[list,None] = None,
        toggle_CFA: Union[str,None] = None,
        func: callable = print,
        **kwargs
):
    """
    Parse all group-based arguments to group instantiation"""

    ## 1. Filter arguments
    bypass=BypassSwitch(bypass)

    set_verbose(0, 'padocc')

    xarray_kwargs = None
    if xarray_kwargs_raw is not None:
        xarray_kwargs = {}
        for i in xarray_kwargs.split(','):
            attr, val = i.split(':')
            if val == 'pyTrue':
                val = True
            elif val == 'pyFalse':
                val = False

            xarray_kwargs[attr] = val

    if not group_exists(groupID, workdir):
        if operation not in NEW_GROUP_ACTIONS:
            raise ValueError(f'Unsupported action for new group {groupID}')

    ## 2. Create group
    op_group = GroupOperation(
        groupID,
        workdir=workdir,
        forceful=forceful,
        dryrun=dryrun,
        thorough=thorough,
        new_version=new_version,
        label=f'PADOCC-CLI-{operation}',
        verbose=verbose,
        bypass=bypass,
        xarray_kwargs=xarray_kwargs
    )

    ## 3. Toggle CFA on/off
    if toggle_CFA == 'y':
        for proj in op_group.assemble_codeset(proj_code, subset, repeat_id=repeat_id):
            project = op_group[proj]
            project.cfa_enabled = True
            project.save_files()
    elif toggle_CFA == 'n':
        for proj in op_group.assemble_codeset(proj_code, subset, repeat_id=repeat_id):
            project = op_group[proj]
            project.cfa_enabled = False
            project.save_files()

    ## 4. Assemble 'run_kwargs'
    run_kwargs = {}
    if parallel_project is not None:
        run_kwargs = {
            'compute_subset':parallel_project.split('/')[0],
            'compute_total':parallel_project.split('/')[1]
        }

    if identical_dims is not None:
        run_kwargs['identical_dims'] = identical_dims.split(',')

    if concat_dims is not None:
        run_kwargs['concat_dims'] = concat_dims.split(',')

    run_kwargs['input_file'] = input_file
    run_kwargs['aggregator'] = aggregator
    run_kwargs['b64vars'] = b64vars

    ## 5a. Run Parallel
    if parallel:

        mode = kwargs.pop('mode')

        op_group.deploy_parallel(
            operation,
            proj_code=proj_code,
            bypass=bypass,
            source=venvpath,
            band_increase=band_increase,
            binpack=binpack,
            time_allowed=time_allowed,
            memory=memory,
            subset=subset,
            repeat_id=repeat_id,
            xarray_kwargs=xarray_kwargs_raw, # Unprocessed raw CLI value
            run_kwargs=run_kwargs,
            wait=wait_sbatch,
            mode=mode
        )
        return
    
    ## 5b. Run Serial
    OPERATIONS[operation](op_group, run_kwargs=run_kwargs,repeat_id=repeat_id, proj_code=proj_code,**kwargs)

def get_args():

    universal_parser = argparse.ArgumentParser(add_help=False) # Applies to all
    phased_parser  = argparse.ArgumentParser(add_help=False) # Applies to `scan`, `compute`, `validate`
    input_parser   = argparse.ArgumentParser(add_help=False) # Applies to `validate`, `add` and `init`

    group_parser   = argparse.ArgumentParser(add_help=False) # Applies to all EXCEPT `list`
    group_parser.set_defaults(func=parse_group)

    # Flags that apply universally to all operations
    universal_parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')

    # Flags that apply to all group-based operations
    group_parser.add_argument('-G','--groupID',   dest='groupID', default=None, help='Group identifier label', required=True)
    group_parser.add_argument('-s','--subset',    dest='subset',    default=None, help='Size of subset within group')
    group_parser.add_argument('-r','--repeat_id', dest='repeat_id', default='main', help='Repeat id (main if first time running, <phase>_<repeat> otherwise)')
    group_parser.add_argument('-p','--proj_code',dest='proj_code',help='Run for a specific project code, within a group or otherwise')
    group_parser.add_argument('-v','--verbose', dest='verbose', action='count', default=0, help='Print helpful statements while running')
    group_parser.add_argument('-T','--thorough', dest='thorough', action='store_true', help='Thorough processing - start from scratch')


    # Operations involving an input file
    input_parser.add_argument('-i','--input_file', dest='input_file', help='Input file for `init`, `add` or `validate` operations.')

    # Generic arguments for all phased operations
    phased_parser.add_argument('-f','--forceful',dest='forceful',action='store_true', help='Force overwrite of steps if previously done')
    phased_parser.add_argument('-d','--dryrun',  dest='dryrun',  action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )
    phased_parser.add_argument('-b','--bypass-errs', dest='bypass', default='D', help=BypassSwitch().help())
    phased_parser.add_argument('-C','--cloud-format', dest='mode', default=None, help='Output format to be used.')
    phased_parser.add_argument('--CFA', dest='toggle_CFA', help='Output format to be used.')
    phased_parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job (parallel only)')
    phased_parser.add_argument('-M','--memory', dest='memory', default='2G', help='Memory allocation for this job (parallel only)(i.e "2G" for 2GB)')

    phased_parser.add_argument('-e','--environ',dest='venvpath', help='Path to virtual (e)nvironment (excludes /bin/activate)')
    phased_parser.add_argument('-A', '--alloc-bins', dest='binpack',action='store_true', help='input file (for init phase)')
    phased_parser.add_argument('--xarray_kwargs', dest='xarray_kwargs_raw', help='Supply kwargs for xarray, comma separated') # All operations
    phased_parser.add_argument('--parallel', dest='parallel',action='store_true',help='Add for parallel deployment with SLURM') # All
    phased_parser.add_argument('--parallel_project', dest='parallel_project',default=None, help='Add for parallel deployment with SLURM for internal project conversion.') # All
    phased_parser.add_argument('--allow-band-increase', dest='band_increase',action='store_true', help='Allow automatic banding increase relative to previous runs.')
    phased_parser.add_argument('-n','--new_version', dest='new_version', action='store_true', help='If present, allow a new version to be created')
    phased_parser.add_argument('--diagnostic', dest='diagnostic',action='store_true',help='Enter diagnostic mode.')
    phased_parser.add_argument('--wait_sbatch', dest='wait_sbatch', action='store_true', help='Halt on sbatch (SLURM) submissions')

    parser = argparse.ArgumentParser(description='Run PADOCC commands or pipeline phases')

    subparsers = parser.add_subparsers(dest='operation')

    # Subparsers with Bespoke Flags
    ## Add
    add = subparsers.add_parser('add', help='Add projects to an existing group.',
                                parents=[universal_parser, group_parser, input_parser])
    add.add_argument('--moles', dest='moles', help='Flag for input files being processed from moles')
    ## Complete
    complete = subparsers.add_parser('complete', help='Complete projects from a group or the entire group - transfer reports and data files.', 
                                parents=[universal_parser, group_parser])
    complete.add_argument('--completion_dir',help='Directory for completion - data and reports will be transferred.', required=True)
    complete.add_argument('--sub', dest='remote_sub',help='Substitution if not the default')
    complete.add_argument('--replace', dest='remote_replace',help='Replacement to substitution if not the default')
    ## Compute
    compute = subparsers.add_parser('compute',help='Compute data aggregations for a project, group or subset of projects. (Pipeline phase 2)', 
                                parents=[universal_parser, group_parser, phased_parser])
    compute.add_argument('--mem-allowed', dest='mem_allowed', default='100MB', help='Memory allowed for Zarr rechunking') # Compute only
    compute.add_argument('--aggregator', dest='aggregator',default=None, help='Specific aggregation method to use for Kerchunk references') # Compute only
    compute.add_argument('--identical_dims', dest='identical_dims', default=None, help='Manually supply new aggregation parameters: Identical dims')
    compute.add_argument('--concat_dims', dest='concat_dims', default=None, help='Manually supply new aggregation parameters: Concat dims')
    compute.add_argument('--b64vars', dest='b64vars', help='Manually supply variables for b64 encoding (Kerchunk)' )
    ## Logs
    logs = subparsers.add_parser('logs',help='Obtain logs from a given project or group.', 
                                parents=[universal_parser, group_parser])
    logs.add_argument('--log_phase', help='Phase from which to retrieve the logs', required=True)
    # Filter flag

    ## Repeat
    repeat = subparsers.add_parser('repeat', help='Subset projects from a group for reanalysis or repeating a phase of the pipeline.', 
                                parents=[universal_parser, group_parser])
    repeat.add_argument('--status', dest='old_status',help='Current status of projects to be repeated.')
    repeat.add_argument('--phase', dest='old_phase',help='Current phase for projects being repeated')
    repeat.add_argument('--new_repeat', dest='new_repeat_id',help='Label to apply to newly created group subset.', required=True)
    ## Status
    status = subparsers.add_parser('status', help='Get a general status display for a given group.', 
                                parents=[universal_parser, group_parser])
    status.add_argument('-D','--display_upto', default=10,help="Display a number of project IDs for each status type")
    status.add_argument('-L','--long_display', default=False, action='store_true', help='Display full status messages without truncation')
    status.add_argument('-S','--separate_errors', default=False, action='store_true', help='Display full status messages without truncation')
    ## Transfer
    transfer = subparsers.add_parser('transfer', help='Transfer projects between groups.', 
                                parents=[universal_parser, group_parser])
    transfer.add_argument('--new_group',dest='new_group',help='Transfer projects to new group.', required=True)
    ## Update Status
    update_status = subparsers.add_parser('update_status', help='Manually update the status of one or more projects matching conditions.', 
                                parents=[universal_parser, group_parser])
    update_status.add_argument('--old_phase', dest='old_phase',help='Current phase of projects being updated.', required=True)
    update_status.add_argument('--new_phase', dest='new_phase',default=None, help='Update phase of project.')
    update_status.add_argument('--old_status', dest='old_status',help='Current status of projects being updated.', required=True)
    update_status.add_argument('--new_status', dest='new_status',help='New status message for projects.', required=True)

    # Subparsers without Bespoke Flags
    ## Aggregations
    agg = subparsers.add_parser('aggregations',help='Get summary of aggregations used',
                                parents=[universal_parser, group_parser])
    ## Check Attr
    check_attr = subparsers.add_parser('check_attr', help='Check the value of an attribute across all group projects.', 
                                parents=[universal_parser, group_parser])
    check_attr.add_argument('--attr', dest='attr', help='Attribute name to check')
    ## Delete
    delete = subparsers.add_parser('delete', help='Delete projects from a group or the entire group.',
                                parents=[universal_parser, group_parser])
    ## Init
    init = subparsers.add_parser('init',help='Initialise a new/existing empty group from an input file.', 
                                parents=[universal_parser, group_parser, phased_parser, input_parser])
    

    ## List
    list_groups = subparsers.add_parser('list', help='List groups in the current working directory. (WORKDIR)',
                                parents=[universal_parser])
    list_groups.set_defaults(func=listgroups)
    ## New
    new = subparsers.add_parser('new',help='Create a new empty group (to be filled with projects).', 
                                parents=[universal_parser, group_parser, phased_parser])
    
    ## Pfunc
    pfunc = subparsers.add_parser('pfunc', help='Perform a custom function across a group', 
                                parents=[universal_parser, group_parser])
    ## Report
    report = subparsers.add_parser('report', help='Obtain the validation report for a given project, or a combined report for a group.', 
                                parents=[universal_parser, group_parser])
    ## Scan
    scan = subparsers.add_parser('scan',help='Scan a project, group or subset of projects. (Pipeline phase 1)', 
                                parents=[universal_parser, group_parser, phased_parser])
    ## Set Attr
    set_attr = subparsers.add_parser('set_attr', help='Set the value of an attribute across all group projects.',
                                parents=[universal_parser, group_parser])
    set_attr.add_argument('--attr', dest='attr', help='Attribute name to set')
    set_attr.add_argument('--value', dest='value', help='New value for attribute')
    ## Summarise
    summarise = subparsers.add_parser('summarise', help='Obtain a data summary for a given group.', 
                                parents=[universal_parser, group_parser])
    ## Validate
    validate = subparsers.add_parser('validate',help='Validate data aggregations for a project, group or subset of projects. (Pipeline phase 3)', 
                                parents=[universal_parser, group_parser, phased_parser, input_parser])

    args = parser.parse_args()

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')

    return args

def main():
    """
    Run Command Line functions for PADOCC serial
    processing. Parallel process deployment will 
    be re-added in the full version."""
    args = get_args()
    args.func(**vars(args))

if __name__ == '__main__':
    main()