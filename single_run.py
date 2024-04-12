
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import sys
import argparse
import os
import json
import logging
from datetime import datetime
import traceback
import re

# Pipeline Modules
from pipeline.logs import init_logger, reset_file_handler, log_status
from pipeline.utils import get_attribute, BypassSwitch, get_codes, set_last_run, get_proj_dir
from pipeline.errors import ProjectCodeError, MissingVariableError, BlacklistProjectCode, SourceNotFoundError

def run_init(args, logger, fh=None, **kwargs) -> None:
    """
    Start initialisation for single dataset

    :param args:    (obj) Set of command line arguments supplied by argparse.

    :param logger:  (obj) Logging object for info/debug/error messages.

    :param fh:      (str) Path to file for logger I/O when defining new logger.

    :returns: None
    """
    from pipeline.init import init_config
    logger.info('Starting init process')
    init_config(args, logger, fh=fh, **kwargs)

def run_scan(args, logger, fh=None,**kwargs) -> None:
    """
    Start scanning process for individual dataset

    :param args:    (obj) Set of command line arguments supplied by argparse.

    :param logger:  (obj) Logging object for info/debug/error messages.

    :param fh:      (str) Path to file for logger I/O when defining new logger.

    :returns: None
    """
    from pipeline.scan import scan_config
    logger.info('Starting Dataset Scan')
    scan_config(args,logger, fh=fh, **kwargs)

def run_compute(args, logger, fh=None, logid=None, **kwargs) -> None:
    """
    Setup computation parameters for individual dataset

    :params args:   (obj) Set of command line arguments supplied by argparse.

    :params logger: (obj) Logging object for info/debug/error messages.

    :params fh:     (str) Path to file for logger I/O when defining new logger.

    :params logid:  (str) Passed to KerchunkDSProcessor for specifying a logger component.

    :returns: None
    """
    from pipeline.compute import compute_config
    logger.info('Starting compute process')

    if args.bypass.skip_scan:
        from pipeline.scan import write_skip
        write_skip(args.proj_dir, args.proj_code, logger)

    compute_config(args, logger, fh=fh, logid=logid, **kwargs)

def run_validation(args, logger, fh=None, **kwargs) -> None:
    """
    Start validation of single dataset.

    :param args:    (obj) Set of command line arguments supplied by argparse.

    :param logger:  (obj) Logging object for info/debug/error messages.

    :param fh:      (str) Path to file for logger I/O when defining new logger.

    :returns: None
    
    """
    from pipeline.validate import validate_dataset
    logger.info('Starting validation process')
    validate_dataset(args, logger, fh=fh, **kwargs)

    # Note: Validation proved to be unpredictable for timings - not suitable for job allocation.

# Driver functions map to command line input of 'phase'
drivers = {
    'init':run_init,
    'scan':run_scan,
    'compute': run_compute,
    'validate': run_validation
}

def get_proj_code(workdir: str, group: str, pid, repeat_id, subset=0, id=0) -> str:
    """
    Get the correct code given a slurm id from a group of project codes
    
    :param workdir:     (str) The current pipeline working directory.

    :param group:       (str) The name of the group which this project code belongs to.

    :param pid:         (str) The project code for which to get the index.

    :param repeat_id:   (str) The subset within the group (default is main)

    :param subset:      (int) The size of the subset within this repeat group.

    :param id:          (int) The specific index of this subset within a group.
                        i.e subset size of 100, total codes is 1000 so 10 codes per subset.
                        an id value of 2 would mean the third group of 10 codes.

    :returns: The project code (DOI) in string format not index format.
    """
    try:
        proj_codes = get_codes(group, workdir, f'proj_codes/{repeat_id}')
        if not proj_codes:
            raise SourceNotFoundError(sfile=f'proj_codes/{repeat_id}')
        proj_code = proj_codes[int(id)*subset + pid]
    except:
        raise ProjectCodeError
    return proj_code

def blacklisted(proj_code: str, groupdir: str, logger) -> bool:
    """
    Determine if the current project code is blacklisted
    
    :param groupdir:    (str) The path to a group directory within the pipeline

    :param proj_code:   (str) The project code in string format (DOI)

    :param logger:      (obj) Logging object for info/debug/error messages.

    :returns: True if the project code is in the blacklist, false otherwise.
    """
    blackcodes = get_codes(groupdir, None, 'blacklist_codes')
    if blackcodes:
        return bool(re.match(f'.*{proj_code}.*',''.join(map(str,blackcodes))))
    else:
        logger.debug('No blacklist file preset for this group')
        return False
    
def assemble_single_process(args, logger=None, jobid='', fh=None, logid=None) -> None:
    """
    Process a single task and assemble required parameters. This task may sit within a subset,
    repeat id or larger group, but everything from here is concerned with the processing of 
    a single dataset (task).

    :param args:        (obj) Set of command line arguments supplied by argparse.

    :param logger:      (obj) Logging object for info/debug/error messages. Will create a new 
                        logger object if not given one.

    :param jobid:       (str) From SLURM_ARRAY_JOB_ID - matters for which log files are created.

    :param fh:          (str) Path to file for logger I/O when defining new logger.

    :param logid:       (str) If creating a new logger, will need an id to distinguish this logger
                        from other single processes (typically n of N total processes.)

    :returns: None
    """
    if not logger:
        logger = init_logger(args.verbose, args.mode, f'{args.phase}', fh=fh, logid=logid)

    if not args.phase in drivers:
        logger.error(f'"{args.phase}" not recognised, please select from {list(drivers.keys())}') 
        return None
    logger.info(f"Initialised single process - {datetime.now().strftime('%H:%M:%S %d/%m/%y')}")

    logger.debug('Using attributes:')
    logger.debug(f'proj_code: {args.proj_code}')
    logger.debug(f'proj_dir : {args.proj_dir}')

    if not args.bypass.skip_report:
        log_status(args.phase, args.proj_dir, 'pending', logger, jobid=jobid, dryrun=args.dryrun)
        if 'allocations' not in args.repeat_id:
            set_last_run(args.proj_dir, args.phase, args.time_allowed)

    try:
        drivers[args.phase](args, logger)
    except Exception as err:
        logger.error('Exception caught for single process')
        if jobid != '':
            tb = traceback.format_exc()
            logger.info(tb)
        raise err

    if not args.bypass.skip_report:
        log_status(args.phase, args.proj_dir, 'complete', logger, jobid=jobid, dryrun=args.dryrun)

def main(args) -> None:
    """
    Main function for processing a single job. This could be multiple tasks/datasets within 
    a single job, but everything from here is serialised, i.e run one after another.

    :param args:        (obj) Set of command line arguments supplied by argparse.

    :returns: None
    """

    jobid = ''
    fh    = ''

    if os.getenv('SLURM_ARRAY_JOB_ID'):
        jobid = os.getenv('SLURM_ARRAY_JOB_ID')
        taskid = os.getenv('SLURM_ARRAY_TASK_ID')

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')
    if args.groupID:
        args.groupdir = f'{args.workdir}/groups/{args.groupID}'
        if jobid != '':
            # Update to expected path.
            fh = f'{args.groupdir}/errs/{args.phase}_{args.repeat_id.replace("/","_")}/{jobid}_{taskid}.log'

    print(fh, jobid)
    logger      = init_logger(args.verbose, args.mode, 'main', fh=fh)
    args.bypass = BypassSwitch(switch=args.bypass)

    if not args.workdir:
        logger.error('No working directory given as input or from environment')
        raise MissingVariableError(type='$WORKDIR')
    
    if not os.access(args.workdir, os.W_OK):
        logger.error('Workdir provided is not writable')
        raise IOError('Workdir not read/writable')

    logger.debug('Passed initial writability checks')

    logger.debug('Pipeline variables (reconfigured):')
    logger.debug(f'WORKDIR : {args.workdir}')
    logger.debug(f'GROUPDIR: {args.groupdir}')

    passes, fails = 0, 0
    codes = []
    if args.binpack:
        logger.debug('Getting codes from allocations or bands')
        # Using Allocations or Bands
        if 'allocations' in args.repeat_id:
            codes = get_codes(
                args.groupID, args.workdir, 
                f'proj_codes/{args.repeat_id}/{args.proj_code}'
            )
        else:
            # Bands just point to a different repeat_id. 
            codes = [
                get_proj_code(
                    args.workdir, args.groupID, int(args.proj_code), args.repeat_id
            )]
        
    elif args.subset > 1:
        # Using unallocated subsets.
        for pid in range(args.subset):
            codes.append(
                get_proj_code(
                    args.workdir, args.groupID, pid, args.repeat_id, 
                    subset=args.subset, id=int(args.proj_code)
            ))
    else:
        if not re.match('.*.[a-zA-Z].*',args.proj_code):
            # Project code is an index - quick convert to actual project code.
            codes = [
                get_proj_code(
                    args.workdir, args.groupID, int(args.proj_code), args.repeat_id
            )]
        else:
            codes = [args.proj_code]

    logger.info(f'Identified {len(codes)} dataset(s) to process')
    quality = bool(args.quality)
    for id, proj_code in enumerate(codes):
        # Ensures no reset within any child processes.
        args.quality = quality
        
        if len(codes) > 1:
            logger.info(f'Starting process for {id+1}/{len(codes)}')
        if id > 0:
            logger.info(f'Success (so far): {passes}, Errors (so far): {fails}')
        args.proj_code = proj_code
        args.proj_dir  = get_proj_dir(args.proj_code, args.workdir, args.groupID)

        # Create any required logging space - done already for this subset.
        proj_fh = None
        if jobid != '':
            proj_fh = f'{args.proj_dir}/phase_logs/{args.phase}.log'
        try:
            assemble_single_process(args, jobid=jobid, fh=proj_fh, logid=id)
            passes += 1
        except Exception as err:
            fails += 1

            # Report/log status
            if not args.bypass.skip_report:
                try:
                    status = err.get_str()
                except AttributeError:
                    status = type(err).__name__ + ' ' + str(err)
                # Messes up the csv if there are commas
                status = status.replace(',','-')
                log_status(args.phase, args.proj_dir, status, logger, jobid=jobid, dryrun=args.dryrun)
            elif not args.binpack and args.subset == 1:
                # Only raise error if we're not bin packing AND skipping the reporting.
                # If reporting is skipped, the error is not displayed directly but fails are recorded at the end.
                raise err
            else:
                pass
    logger.info('Pipeline phase execution finished')
    logger.info(f'Success: {passes}, Error: {fails}') 
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('phase',    type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('proj_code',type=str, help='Project identifier code')

    # Action-based - standard flags
    parser.add_argument('-f','--forceful',dest='forceful',      action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-v','--verbose', dest='verbose',       action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',        action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )
    parser.add_argument('-Q','--quality', dest='quality',       action='store_true', help='Create refs from scratch (no loading), use all NetCDF files in validation')
    parser.add_argument('-B','--backtrack',   dest='backtrack', action='store_true', help='Backtrack to previous position, remove files that would be created in this job.')
    parser.add_argument('-A', '--alloc-bins', dest='binpack',   action='store_true', help='Use binpacking for allocations (otherwise will use banding)')

    # Environment variables
    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-p','--proj_dir',  dest='proj_dir',     help='Project directory for pipeline')

    # Single job within group
    parser.add_argument('-t','--time-allowed', dest='time_allowed',help='Time limit for this job')
    parser.add_argument('-G','--groupID',      dest='groupID',     default=None,       help='Group identifier label')
    parser.add_argument('-M','--memory',    dest='memory',         default='2G',       help='Memory allocation for this job (i.e "2G" for 2GB)')
    parser.add_argument('-s','--subset',    dest='subset',         default=1,type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id',      default='main',     help='Repeat id (1 if first time running, <phase>_<repeat> otherwise)')

    # Specialised
    parser.add_argument('-b','--bypass-errs',   dest='bypass', default='DBSCLR', help=BypassSwitch().help())
    parser.add_argument('-n','--new_version',   dest='new_version',              help='If present, create a new version')
    parser.add_argument('-m','--mode',          dest='mode',   default=None,     help='Print or record information (log or std)')
    parser.add_argument('-O','--override_type', dest='override_type',            help='Specify cloud-format output type, overrides any determination by pipeline.')
    
    args = parser.parse_args()

    success = main(args)
    if not success:
        raise Exception

    