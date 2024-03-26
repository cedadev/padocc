
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
from pipeline.utils import get_attribute, BypassSwitch, get_codes, get_proj_file
from pipeline.errors import ProjectCodeError, MissingVariableError, BlacklistProjectCode

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
    init_config(args, fh=fh, **kwargs)

def run_scan(args, logger, fh=None,**kwargs) -> None:
    """
    Start scanning process for individual dataset

    :param args:    (obj) Set of command line arguments supplied by argparse.

    :param logger:  (obj) Logging object for info/debug/error messages.

    :param fh:      (str) Path to file for logger I/O when defining new logger.

    :returns: None
    """
    from pipeline.scan import scan_config
    logger.info('Starting scan process')
    scan_config(args,fh=fh, **kwargs)

def run_compute(args, logger, fh=None, logid=None, **kwargs) -> None:
    """
    Setup computation parameters for individual dataset

    :params args:   (obj) Set of command line arguments supplied by argparse.

    :params logger: (obj) Logging object for info/debug/error messages.

    :params fh:     (str) Path to file for logger I/O when defining new logger.

    :params logid:  (str) Passed to Indexer for specifying a logger component.

    :returns: None
    """
    from pipeline.compute.serial_process import Indexer

    logger.info(f'Starting computation step for {args.proj_code}')

    cfg_file = f'{args.proj_dir}/base-cfg.json'
    detail_file = f'{args.proj_dir}/detail-cfg.json'

    if not os.path.isfile(cfg_file):
        logger.error(f'cfg file missing or not provided - {cfg_file}')
        return None
    
    if not os.path.isfile(detail_file):
        logger.error(f'cfg file missing or not provided - {detail_file}')
        return None
    
    version_no = 1
    complete, escape = False, False
    while not (complete or escape):
        out_json = f'{args.proj_dir}/kerchunk-{version_no}a.json'
        out_parq = f'{args.proj_dir}/kerchunk-{version_no}a.parq'

        if os.path.isfile(out_json) or os.path.isfile(out_parq):
            if args.forceful:
                complete = True
            elif args.new_version:
                version_no += 1
            else:
                escape = True
        else:
            complete = True

    concat_msg = '' # CMIP and CCI may be different?

    if complete and not escape:

        t1 = datetime.now()
        ds = Indexer(args.proj_code, cfg_file=cfg_file, detail_file=detail_file, 
                workdir=args.workdir, issave_meta=True, thorough=args.quality, forceful=args.forceful,
                verb=args.verbose, mode=args.mode,
                version_no=version_no, concat_msg=concat_msg, bypass=args.bypass, groupID=args.groupID, 
                dryrun=args.dryrun, fh=fh, logid=logid)
        ds.create_refs()

        compute_time = (datetime.now()-t1).total_seconds()

        detailfile = f'{args.proj_dir}/detail-cfg.json'
        with open(detailfile) as f:
            detail = json.load(f)
        if 'timings' not in detail:
            detail['timings'] = {}
        detail['timings']['convert_actual'] = ds.convert_time
        detail['timings']['concat_actual']  = ds.concat_time
        detail['timings']['compute_actual'] = compute_time
        with open(detailfile,'w') as f:
            f.write(json.dumps(detail))

    else:
        logger.error('Output file already exists and there is no plan to overwrite')
        return None

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
    validate_dataset(args, fh=fh, **kwargs)

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
    
def assemble_single_process(args, logger, jobid='', fh=None) -> None:
    """
    Process a single task and assemble required parameters. This task may sit within a subset,
    repeat id or larger group, but everything from here is concerned with the processing of 
    a single dataset (task).

    :param args:        (obj) Set of command line arguments supplied by argparse.

    :param logger:      (obj) Logging object for info/debug/error messages.

    :param jobid:       (str) From SLURM_ARRAY_JOB_ID

    :param fh:          (str) Path to file for logger I/O when defining new logger.

    :returns: None
    """

    if args.groupID:

        # Avoid stray groupdir definition in environment variables
        cmd_groupdir = f'{args.workdir}/groups/{args.groupID}'
        if cmd_groupdir != args.groupdir:
            logger.warning(f'Overriding environment-defined groupdir value with: {cmd_groupdir}')
            args.groupdir = cmd_groupdir

        # Assume using an integer (SLURM_ARRAY_TASK_ID)
        proj_code = int(args.proj_code)

        if args.binpack:
            # Binpacking requires separate system for getting the right project code
            raise NotImplementedError

        args.proj_code = get_proj_code(args.workdir, args.groupID, proj_code, args.repeat_id, subset=args.subset, id=id)
        args.proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{args.proj_code}'

        # Get rid of this section if necessary
        # Made redundant with use of error logging PPC but still needed - job-error suppression required.
        if jobid != '':
            errs_dir = f'{args.workdir}/groups/{args.groupID}/errs'
            if not os.path.isdir(f'{errs_dir}/{jobid}_{args.repeat_id}'):
                os.makedirs(f'{errs_dir}/{jobid}_{args.repeat_id}')

            proj_code_file = f'{args.workdir}/groups/{args.groupID}/proj_codes/{args.repeat_id}.txt'

            if not os.path.isfile(f'{errs_dir}/{jobid}_{args.repeat_id}/proj_codes.txt'):
                os.system(f'cp {proj_code_file} {errs_dir}/{jobid}_{args.repeat_id}/proj_codes.txt')

    else:
        args.proj_dir = f'{args.workdir}/in_progress/{args.proj_code}'

    #if blacklisted(args.proj_code, args.groupdir, logger) and not args.backtrack:
        #raise BlacklistProjectCode

    if not args.phase in drivers:
        logger.error(f'"{args.phase}" not recognised, please select from {list(drivers.keys())}') 
        return None

    logger.debug('Pipeline variables (reconfigured):')
    logger.debug(f'WORKDIR : {args.workdir}')
    logger.debug(f'GROUPDIR: {args.groupdir}')
    logger.debug('Using attributes:')
    logger.debug(f'proj_code: {args.proj_code}')
    logger.debug(f'proj_dir : {args.proj_dir}')

    # Refresh log for this phase
    proj_log = f'{args.proj_dir}/phase_logs/{args.phase}.log'
    if not os.path.isdir(f'{args.proj_dir}/phase_logs'):
        os.makedirs(f'{args.proj_dir}/phase_logs')
    if jobid != '':
        if os.path.isfile(proj_log):
            os.system(f'rm {proj_log}')
        if os.path.isfile(fh):
            os.system(f'rm {fh}')
    if not args.bypass.skip_report:
        log_status(args.phase, args.proj_dir, 'pending', logger, jobid=jobid, dryrun=args.dryrun)

    if jobid != '':
        logger = reset_file_handler(logger, args.verbose, proj_log)
        drivers[args.phase](args, logger, fh=proj_log, logid=id)
        logger = reset_file_handler(logger, args.verbose, fh)
    else:
        drivers[args.phase](args, logger)
    passes += 1
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
            fh = f'{args.groupdir}/errs/{jobid}_{taskid}_{args.phase}_{args.repeat_id}.log'

    logger = init_logger(args.verbose, args.mode, 'main', fh=fh)

    args.bypass = BypassSwitch(switch=args.bypass)

    logger.debug('Pipeline variables:')
    logger.debug(f'WORKDIR : {args.workdir}')
    logger.debug(f'GROUPDIR: {args.groupdir}')

    if not args.workdir:
        logger.error('No working directory given as input or from environment')
        raise MissingVariableError(type='$WORKDIR')
    
    if not os.access(args.workdir, os.W_OK):
        logger.error('Workdir provided is not writable')
        raise IOError('Workdir not read/writable')

    logger.debug('Passed initial writability checks')

    passes, fails = 0, 0

    for id in range(args.subset):
        print()
        if args.subset > 1:
            logger.info(f'Starting process for {id+1}/{args.subset}')
        try:
            assemble_single_process(args, logger, jobid=jobid, fh=fh)
            passes += 1
        except Exception as err:
            # Capture all errors - any error handled here is a setup error
            # Implement allocation override here - no error thrown if using allocation.

            # Add error traceback
            tb = traceback.format_exc()
            logger.error(tb)

            # Reset file handler back to main.
            if jobid != '':
                logger = reset_file_handler(logger, args.verbose, fh)
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
            elif not args.binpack:
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
    parser.add_argument('-f','--forceful',dest='forceful',action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',  action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )
    parser.add_argument('-Q','--quality', dest='quality', action='store_true', help='Quality assured checks - thorough run')
    parser.add_argument('-b','--bypass-errs', dest='bypass', default='DBSCMR', help=BypassSwitch().help())
    parser.add_argument('-B','--backtrack', dest='backtrack', action='store_true', help='Backtrack to previous position, remove files that would be created in this job.')
    parser.add_argument('-A', '--alloc-bins', dest='binpack',action='store_true', help='input file (for init phase)')

    # Environment variables
    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-p','--proj_dir',    dest='proj_dir',      help='Project directory for pipeline')

    # Single job within group
    parser.add_argument('-G','--groupID',   dest='groupID', default=None, help='Group identifier label')
    parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job')
    parser.add_argument('-M','--memory', dest='memory', default='2G', help='Memory allocation for this job (i.e "2G" for 2GB)')
    parser.add_argument('-s','--subset',    dest='subset',    default=1,   type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='main', help='Repeat id (1 if first time running, <phase>_<repeat> otherwise)')

    # Specialised
    parser.add_argument('-n','--new_version', dest='new_version',   help='If present, create a new version')
    parser.add_argument('-m','--mode',        dest='mode', default=None, help='Print or record information (log or std)')
    
    args = parser.parse_args()

    success = main(args)
    if not success:
        raise Exception

    