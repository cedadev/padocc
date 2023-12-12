
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import json
import logging

config = {
    'proj_code': None,
    'workdir': None,
    'proj_dir':None,
    'pattern': None,
    'update': None,
    'remove': None
}

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

def get_updates(logger):
    """Get key-value pairs for updating in final metadata"""
    logger.debug('Getting update key-pairs')
    inp = None
    valsdict = {}
    while inp != 'exit':
        inp = input('Attribute: ("exit" to escape):')
        if inp != 'exit':
            val = input('Value: ')
            valsdict[inp] = val
    return valsdict

def get_removals(logger):
    """Get attribute names to remove in final metadata"""
    logger.debug('Getting removals')
    valsarr = []
    while inp != 'exit':
        inp = input('Attribute: ("exit" to escape):')
        if inp != 'exit':
            valsarr.append(inp)
    return valsarr

def get_proj_code(path, prefix=''):
    return path.replace(prefix,'').replace('/','_')

def load_from_input_file(args, logger):
    """Configure project directory and base config from input file"""
    logger.debug('Ingesting input config file')
    if os.path.isfile(args.input):
        with open(args.input) as f:
            refs = json.load(f)

        proj_dir = refs['proj_dir']
        if not os.path.isdir(proj_dir):
            os.makedirs(proj_dir)
        if not os.path.isfile(f'{proj_dir}/base-cfg.json'):
            os.system(f'cp {args.input} {proj_dir}/base-cfg.json')
    else:
        logger.error(f'Input file {args.input} does not exist')
        return None

def text_file_to_csv(args, logger):
    """Convert text file list of patterns to a csv for a set of projects"""
    logger.debug('Converting text file to csv')

    if not os.path.isdir(args.workdir):
        os.path.makedirs(args.workdir)
    if args.groupdir and not os.path.isdir(args.groupdir):
        os.path.makedirs(args.groupdir)

    new_inputfile = f'{args.groupdir}/filelists/{args.group}.txt'
    prefix = '' # Remove from path for proj_code
    os.system(f'cp {args.input} {new_inputfile}')

    with open(new_inputfile) as f:
        datasets = [r.strip() for r in f.readlines()]

    records = ''
    for ds in datasets:
        proj_code = get_proj_code(ds, prefix=prefix)
        proj_dir  = f'{args.workdir}/in_progress/{args.group}/{proj_code}'
        pattern   = f'{os.path.realpath(ds)}/*.nc'
        records  += f'{proj_code},{args.workdir},{proj_dir},{pattern},,\n'

    with open(f'{args.groupdir}/datasets.csv','w') as f:
        f.write(records)
    
    # Output completed csv setup part

def make_dirs(args, logger):
    """Set up directory structure for working directory"""
    logger.debug('Creating project directories')

    # Open csv and gather data
    with open(f'{args.groupdir}/datasets.csv') as f:
        datasets = {r.strip().split(',')[0]:r.strip().split(',')[1:] for r in f.readlines()[:]}

    # Map dataset parameters from csv to config JSON
    params     = list(config.keys())
    proj_codes = list(datasets.keys())
    for dskey in proj_codes:
        cfg = dict(config)
        ds  = datasets[dskey]

        cfg[params[0]] = dskey # Set project code
        # Set all other parameters
        for x, p in enumerate(params[1:]):
            cfg[p] = ds[x]

        # Save config file
        if not os.path.isdir(cfg['proj_dir']):
            os.makedirs(cfg['proj_dir'])
    
            with open(f'{cfg["proj_dir"]}/base-cfg.json','w') as f:
                f.write(json.dumps(cfg))
        
        else:
            logger.warn(f'{cfg["proj_code"]} already exists - skipping')

    logger.info(f'Exported {len(proj_codes)} dataset config files')

    with open(f'{args.groupdir}/proj_codes_1.txt','w') as f:
        f.write('\n'.join(proj_codes))

    logger.info(f'Written as group ID:{args.groupID}')

def init_config(args):
    """Main configuration script, load configurations from input sources"""

    logger = init_logger(args.verbose, args.mode, 'init')

    groupID = None
    if hasattr(args, 'groupID'):
        groupID = getattr(args,'groupID')

    if groupID:
        logger.debug('Starting group initialisation')
        if not hasattr(args,'input'):
            logger.error('Group run requires input file in csv or txt format')
            return None
        
        if '.txt' in args.input:
            text_file_to_csv(args, logger) # Includes creating csv
        elif '.csv' in args.input:
            new_csv = f'{args.groupdir}/datasets.csv'
            os.system(f'cp {args.input} {new_csv}')

        make_dirs(args, logger)

    else:
        logger.debug('Starting single project initialisation')

        if hasattr(args,'input'):
            load_from_input_file(args, logger)
        else:
            try:
                get_input(args, logger)
            except KeyboardInterrupt:
                logger.info('Aborting user input process and exiting')
                return None
            except Exception as e:
                logger.error(f'User Input Error - {e}')
                return None

def get_input(args, logger):
    """Get command-line inputs for specific project configuration"""

    # Get basic inputs
    logger.debug('Getting user inputs for new project')

    if os.getenv('SLURM_JOB_ID'):
        logger.error('Cannot run input script as Slurm job - aborting')
        return None

    proj_code = input('Project Code: ')
    pattern   = input('Wildcard Pattern: (leave blank if not applicable) ')
    if pattern == '':
        filelist  = input('Path to filelist: ')
        pattern   = None
    else:
        filelist  = None

    if os.getenv('WORKDIR'):
        workdir = os.getenv('WORKDIR')

    if args.workdir and args.workdir != workdir:
        print('Environment workdir does not match provided address')
        print('ENV:',workdir)
        print('ARG:',args.workdir)
        choice = input('Choose to keep the ENV value or overwrite with the ARG value: (E/A) :')
        if choice == 'E':
            pass
        elif choice == 'A':
            os.environ['WORKDIR'] = args.workdir
            workdir = args.workdir
        else:
            print('Invalid input, exiting')
            return None

    proj_dir = f'{workdir}/in_progress/{proj_code}'
    if os.path.isdir(proj_dir):
        if args.forceful:
            pass
        else:
            print('Error: Directory already exists -',proj_dir)
            return None
    else:
        os.makedirs(proj_dir)

    config = {
        'proj_code': proj_code,
        'workdir'  : workdir,
        'proj_dir' : proj_dir
    }
    do_updates = input('Do you wish to add overrides to metadata values? (y/n): ')
    if do_updates == 'y':
        config['update'] = get_updates()
    
    do_removals = input('Do you wish to remove known attributes from the metadata? (y/n): ')
    if do_removals == 'y':
        config['remove'] = get_removals(remove=True)

    if pattern:
        config['pattern'] = pattern

    with open(f'{proj_dir}/base-cfg.json','w') as f:
        f.write(json.dumps(config))
    print(f'Written cfg file at {proj_dir}/base-cfg.json')

if __name__ == '__main__':
    print('Kerchunk Pipeline Config Initialiser - run using master scripts')