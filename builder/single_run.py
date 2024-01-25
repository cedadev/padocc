
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import sys
import argparse
import os
import json
import logging

class MissingVariableError(Exception):
    def __init__(self, type='$', verbose=0):
        self.message = f'Missing variable: {type}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class ExpectTimeoutError(Exception):
    def __init__(self, required=0, current='', verbose=0):
        self.message = f'Scan requires minimum {required} - current {current}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class ProjectCodeError(Exception):
    def __init__(self, verbose=0):
        self.message = f'Project Code Extraction Failed'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

levels = [
    logging.WARN,
    logging.INFO,
    logging.DEBUG
]

def init_logger(verbose, mode, name):
    """Logger object init and configure with formatting"""
    verbose = min(verbose, len(levels)-1)

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

def run_init(args, logger):
    """Start initialisation for single dataset"""
    from pipeline.init import init_config
    logger.info('Starting init process')
    return init_config(args)

def run_scan(args, logger):
    """Start scanning process for individual dataset"""
    from pipeline.scan import scan_config
    logger.info('Starting scan process')
    return scan_config(args)

def run_compute(args, logger):
    """Setup computation parameters for individual dataset"""
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

        return Indexer(args.proj_code, cfg_file=cfg_file, detail_file=detail_file, 
                workdir=args.workdir, issave_meta=True, thorough=args.quality, forceful=args.forceful,
                verb=args.verbose, mode=args.mode,
                version_no=version_no, concat_msg=concat_msg, bypass=args.bypass).create_refs()
    else:
        logger.error('Output file already exists and there is no plan to overwrite')
        return None

def run_validation(args, logger):
    """Start validation of single dataset"""
    from pipeline.validate import validate_dataset
    logger.info('Starting validation process')
    return validate_dataset(args)

drivers = {
    'init':run_init,
    'scan':run_scan,
    'compute': run_compute,
    'validate': run_validation
}

def get_proj_code(groupdir, pid, repeat_id, subset=0, id=0):
    """Get the correct code given a slurm id from a group of project codes"""
    try:
        with open(f'{groupdir}/proj_codes_{repeat_id}.txt') as f:
            proj_code = f.readlines()[int(pid)*subset + id].strip()
    except:
        raise ProjectCodeError
    return proj_code

def get_attribute(env, args, var, logger):
    if os.getenv(env):
        return os.getenv(env)
    elif hasattr(args, var):
        return getattr(args, var)
    else:
        logger.error(f'Missing attribute {var}')
        return None
    
def main(args):
    """Main function for single run processing"""

    logger = init_logger(args.verbose, args.mode, 'main')

    args.workdir  = get_attribute('WORKDIR', args, 'workdir', logger)
    args.groupdir = get_attribute('GROUPDIR', args, 'groupdir', logger)

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

    for id in range(args.subset):
        print()
        logger.info(f'Starting process for {id+1}/{args.subset}')
        try:
            if args.groupID:

                # Avoid stray groupdir definition in environment variables
                cmd_groupdir = f'{args.workdir}/groups/{args.groupID}'
                if cmd_groupdir != args.groupdir:
                    logger.warning(f'Overriding environment-defined groupdir value with: {cmd_groupdir}')
                    args.groupdir = cmd_groupdir

                subset_id = args.proj_code
                args.proj_code = get_proj_code(args.groupdir, subset_id, args.repeat_id, subset=args.subset, id=id)
                args.proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{args.proj_code}'

                # Get ID from within a job?
                if os.getenv('SLURM_ARRAY_JOB_ID'):
                    jobid = os.getenv('SLURM_ARRAY_JOB_ID')
                    errs_dir = f'{args.workdir}/groups/{args.groupID}/errs'
                    if not os.path.isdir(f'{errs_dir}/{jobid}_{args.phase}'):
                        os.makedirs(f'{errs_dir}/{jobid}_{args.phase}')

                    proj_code_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{subset_id}.txt'

                    if not os.path.isfile(f'{errs_dir}/{jobid}_{args.phase}/proj_codes.txt'):
                        os.system(f'cp {proj_code_file} {errs_dir}/{jobid}_{args.phase}/proj_codes.txt')

            else:
                args.proj_dir = f'{args.workdir}/in_progress/{args.proj_code}'

            if args.phase in drivers:
                logger.debug('Pipeline variables (reconfigured):')
                logger.debug(f'WORKDIR : {args.workdir}')
                logger.debug(f'GROUPDIR: {args.groupdir}')
                logger.debug('Using attributes:')
                logger.debug(f'proj_code: {args.proj_code}')
                logger.debug(f'proj_dir : {args.proj_dir}')
                drivers[args.phase](args, logger)
            else:
                logger.error(f'"{args.phase}" not recognised, please select from {list(drivers.keys())}')
        except Exception as err:
            # Capture all errors - any error handled here is fatal
            raise err
    logger.info('Pipeline phase execution finished')
    print('Success')
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('phase',    type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('proj_code',type=str, help='Project identifier code')

    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-G','--groupID',   dest='groupID',      help='Group identifier label')
    parser.add_argument('-p','--proj_dir',    dest='proj_dir',      help='Project directory for pipeline')
    parser.add_argument('-n','--new_version', dest='new_version',   help='If present, create a new version')
    parser.add_argument('-m','--mode',        dest='mode', default=None, help='Print or record information (log or std)')
    parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job')
    parser.add_argument('-b','--bypass-errs', dest='bypass', action='store_true', help='Bypass all error messages - skip failed jobs')

    parser.add_argument('-s','--subset',    dest='subset',    default=1,   type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='1', help='Repeat id (1 if first time running, <phase>_<repeat> otherwise)')

    parser.add_argument('-f', dest='forceful', action='store_true', help='Force overwrite of steps if previously done')

    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=0, help='Print helpful statements while running')
    parser.add_argument('-d','--dryrun',  dest='dryrun',  action='store_true', help='Perform dry-run (i.e no new files/dirs created)' )

    parser.add_argument('-Q','--quality', dest='quality', action='store_true', help='Quality assured checks - thorough run')

    args = parser.parse_args()

    success = main(args)
    if not success:
        raise Exception

    