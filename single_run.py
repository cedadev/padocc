
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import sys
import argparse
import os
import json

def output(msg,verb=True, mode=None, log=None, pref=0):
    prefixes = ['INFO','ERR']
    prefix = prefixes[pref]
    if verb:
        if mode == 'log':
            log += f'{prefix}: {msg}\n'
        else:
            print(f'> {prefix}: {msg}')
    return log

def run_init(args):
    from scripts.init import init_config
    init_config(args)

def run_scan():
    pass

def run_compute(args):
    from scripts.compute.serial_process import Indexer

    log = output(f'Starting computation step for {args.proj_code}', verb=args.verbose, mode=args.mode,log=None)

    cfg_file = f'{args.proj_dir}/base-cfg.json'
    detail_file = f'{args.proj_dir}/detail-cfg.json'

    if not os.path.isfile(cfg_file):
        print(f'Error: cfg file missing or not provided - {cfg_file}')
        return None
    
    if not os.path.isfile(detail_file):
        print(f'Error: cfg file missing or not provided - {detail_file}')
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
        log = output('Output file already exists and there is no plan to overwrite', mode=args.mode,log=None, pref=1)
        return None


drivers = {
    'init':run_init,
    'scan':run_scan,
    'compute': run_compute
}

def get_proj_code(groupdir, pid, groupid):
    with open(f'{groupdir}/proj_codes.txt') as f:
        proj_code = f.readlines()[int(pid)].strip()
    return proj_code

def get_attribute(env, args, var):
    if os.getenv(env):
        return os.getenv(env)
    elif hasattr(args, var):
        return getattr(args, var)
    else:
        print(f'Error: Missing attribute {var}')
        return None
    

def main(args):
    args.workdir  = get_attribute('WORKDIR', args, 'workdir')
    args.groupdir = get_attribute('GROUPDIR', args, 'groupdir')

    if not args.workdir:
        print('Error: No working directory given as input or from environment')
        return None
    
    if args.groupID:
        args.proj_code = get_proj_code(args.groupdir, args.proj_code, args.groupID)
        if not args.proj_dir:
            # Load in all base config settings
            args.proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{args.proj_code}'

    if not args.proj_dir:
        # Load in all base config settings
        args.proj_dir = f'{args.workdir}/in_progress/{args.proj_code}'

    # Run the specified phase
    if args.phase in drivers:
        drivers[args.phase](args)
    else:
        print(f'Error: "{args.phase}" not recognised, please select from {list(drivers.keys())}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('phase', type=str,    help='Phase of the pipeline to initiate')
    parser.add_argument('proj_code',type=str, help='Project identifier code')
    parser.add_argument('-w',dest='workdir',  help='Working directory for pipeline')
    parser.add_argument('-g',dest='groupdir', help='Group directory for pipeline')
    parser.add_argument('-G',dest='groupID',  help='Group identifier label')
    parser.add_argument('-p',dest='proj_dir', help='Project directory for pipeline')
    parser.add_argument('-f',dest='forceful', action='store_true', help='Force overwrite of steps if previously done')
    parser.add_argument('-n',dest='new_version', help='If present, create a new version')
    parser.add_argument('-v',dest='verbose' , action='store_true', help='Print helpful statements while running')
    parser.add_argument('-m',dest='mode'    , help='Print or record information (log or std)')


    args = parser.parse_args()
    main(args)

    