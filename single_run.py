
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import sys
import argparse
import os
import json
import logging

levels = [
    logging.ERROR,
    logging.WARN,
    logging.INFO,
    logging.DEBUG
]

def init_logger(verbose, mode, name):
    """Logger object init and configure with formatting"""
    verbose = min(verbose, len(levels-1))

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

def output(msg,verb=True, mode=None, log=None, pref=0):
    prefixes = ['INFO','ERR']
    prefix = prefixes[pref]
    if verb:
        if mode == 'log':
            log += f'{prefix}: {msg}\n'
        else:
            print(f'> {prefix}: {msg}')
    return log

def run_init(args, logger):
    from scripts.init import init_config
    logger.info('Starting init process')
    init_config(args)

def run_scan(args, logger):
    from scripts.scan import setup_main
    logger.info('Starting scan process')
    setup_main(args)

def run_compute(args, logger):
    from scripts.compute.serial_process import Indexer

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

        Indexer(args.proj_code, cfg_file=cfg_file, detail_file=detail_file, 
                workdir=args.workdir, issave_meta=False, forceful=args.forceful,
                verb=args.verbose, mode=args.mode,
                version_no=version_no, concat_msg=concat_msg).create_refs()
    else:
        logger.error('Output file already exists and there is no plan to overwrite')
        return None

drivers = {
    'init':run_init,
    'scan':run_scan,
    'compute': run_compute
}

def get_proj_code(groupdir, pid, repeat_id, subset=0, id=0):
    with open(f'{groupdir}/proj_codes_{repeat_id}.txt') as f:
        proj_code = f.readlines()[int(pid)*subset + id].strip()
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

    logger = init_logger(args.verbose, args.mode, 'main')

    args.workdir  = get_attribute('WORKDIR', args, 'workdir', logger)
    args.groupdir = get_attribute('GROUPDIR', args, 'groupdir', logger)

    if not args.workdir:
        logger.error('No working directory given as input or from environment')
        return None
    
    if not os.access(args.workdir, os.W_OK):
        logger.error('Workdir provided is not writable')
        return None

    for id in range(args.subset):

        if args.groupID:
            subset_id = args.proj_code
            args.proj_code = get_proj_code(args.groupdir, subset_id, args.repeat_id, subset=args.subset, id=id)
            args.proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{args.proj_code}'
        else:
            args.proj_dir = f'{args.workdir}/in_progress/{args.proj_code}'

        if args.phase in drivers:
            try:
                drivers[args.phase](args, logger)
            except Exception as e:
                logger.error(f'issue with proj_code "{args.proj_code}" - {e}')
        else:
            logger.error(f'"{args.phase}" not recognised, please select from {list(drivers.keys())}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('phase',    type=str, help='Phase of the pipeline to initiate')
    parser.add_argument('proj_code',type=str, help='Project identifier code')

    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-G','--groupID',   dest='groupID',      help='Group identifier label')
    parser.add_argument('-p','--proj_dir',    dest='proj_dir',      help='Project directory for pipeline')
    parser.add_argument('-n','--new_version', dest='new_version',   help='If present, create a new version')
    parser.add_argument('-m','--mode',        dest='mode',          help='Print or record information (log or std)')
    parser.add_argument('-t','--time-allowed',dest='time_allowed',  help='Time limit for this job')

    parser.add_argument('-s','--subset',    dest='subset',    default=1,   type=int, help='Size of subset within group')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='1', help='Repeat id (1 if first time running, <phase>_<repeat> otherwise)')

    parser.add_argument('-f',dest='forceful', action='store_true', help='Force overwrite of steps if previously done')

    parser.add_argument('-v','--verbose',dest='verbose' , action='count', default=0, help='Print helpful statements while running')

    args = parser.parse_args()
    main(args)

    