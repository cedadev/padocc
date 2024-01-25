
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import json
import logging
import glob

config = {
    'proj_code': None,
    'pattern': None,
    'update': None,
    'remove': None
}

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
    parts = path.replace(prefix,'').replace('/','_').split('_')
    if '*.' in parts[-1]:
        parts = parts[:-2]
    return '_'.join(parts)
    
def make_filelist(pattern, proj_dir, logger):
    """Create list of files associated with this project"""
    logger.debug(f'Making list of files for project {proj_dir.split("/")[-1]}')

    if pattern.endswith('.txt'):
        os.system(f'cp {pattern} {proj_dir}/allfiles.txt')
    elif os.path.isdir(proj_dir):
        os.system(f'ls {pattern} > {proj_dir}/allfiles.txt')
    else:
        logger.error(f'Project Directory not located - {proj_dir}')
        return None
    return True

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

def text_file_to_csv(args, logger, prefix=None):
    """Convert text file list of patterns to a csv for a set of projects"""
    logger.debug('Converting text file to csv')

    if not os.path.isdir(args.workdir):
        if args.dryrun:
            logger.debug(f'DRYRUN: Skip making workdir {args.workdir}')
        else:
            os.makedirs(args.workdir)
    if args.groupdir and not os.path.isdir(args.groupdir):
        if args.dryrun:
            logger.debug(f'DRYRUN: Skip making groupdir {args.groupdir}')
        else:
            os.makedirs(args.groupdir)

    new_inputfile = f'{args.workdir}/groups/filelists/{args.groupID}.txt'

    if new_inputfile != args.input:
        if args.dryrun:
            logger.debug(f'DRYRUN: Skip copying input file {args.input} to {new_inputfile}')
        else:
            os.system(f'cp {args.input} {new_inputfile}')

    with open(new_inputfile) as f:
        datasets = [r.strip() for r in f.readlines()]

    if not os.path.isfile(f'{args.groupdir}/datasets.csv') or args.forceful:
        records = ''
        logger.info('Creating filesets for each dataset')
        for index, ds in enumerate(datasets):
            skip = False

            pattern = str(ds)
            if not (pattern.endswith('.nc') or pattern.endswith('.tif')):
                logger.debug('Identifying extension')
                fileset = [r.split('.')[-1] for r in glob.glob(f'{pattern}/*')]
                if len(set(fileset)) > 1:
                    logger.error(f'File type not specified for {pattern} - found multiple ')
                    skip = True
                elif len(set(fileset)) == 0:
                    skip = True
                else:
                    extension = list(set(fileset))[0]
                    pattern = f'{pattern}/*.{extension}'
                logger.debug(f'Found .{extension} common type')

            if not skip:
                proj_code = get_proj_code(ds, prefix=prefix)
                logger.debug(f'Assembled project code: {proj_code}')
                proj_dir  = f'{args.workdir}/in_progress/{args.groupID}/{proj_code}'
                if 'latest' in pattern:
                    pattern = pattern.replace('latest', os.readlink(pattern))

                records  += f'{proj_code},{pattern},,\n'
                logger.debug(f'Added entry and created fileset for {index+1}/{len(datasets)}')
        if args.dryrun:
            logger.debug(f'DRYRUN: Skip creating csv file {args.groupdir}/datasets.csv')    
        else:        
            with open(f'{args.groupdir}/datasets.csv','w') as f:
                f.write(records)
    else:
        logger.warn(f'Using existing csv file at {args.groupdir}/datasets.csv')
    return True
    
    # Output completed csv setup part

def make_dirs(args, logger):
    """Set up directory structure for working directory"""
    logger.info('Creating project directories')

    # Open csv and gather data
    with open(f'{args.groupdir}/datasets.csv') as f:
        datasets = {r.strip().split(',')[0]:r.strip().split(',')[1:] for r in f.readlines()[:]}

    # Map dataset parameters from csv to config JSON
    params     = list(config.keys())
    proj_codes = list(datasets.keys())

    if proj_codes[0] == 'proj_code':
        proj_codes = proj_codes[1:]
    
    for index, proj_code in enumerate(proj_codes):
        cfg_values = dict(config)      # Ensure no linking
        ds_values  = datasets[proj_code]
        pattern    = ds_values[0]

        logger.info(f'Creating directories/filelists for {index+1}/{len(proj_codes)}')

        cfg_values[params[0]] = proj_code
        # Set all other parameters
        if len(params) == len(ds_values)+1:
            for x, p in enumerate(params[1:]):
                cfg_values[p] = ds_values[x]
        else:
            logger.warning(f'Project code {index}:{proj_code} from {args.groupID} does not have correct number of fields.')
            logger.warning(f'Fields specified must be {params}, not {ds_values}')

        if 'latest' in pattern:
            pattern = pattern.replace('latest', os.readlink(pattern))

        if args.groupID:
            proj_dir = f'{args.groupdir}/{proj_code}'
        else:
            proj_dir = f'{args.workdir}/in_progress/{proj_code}'

        # Save config file
        if not os.path.isdir(proj_dir):
            if args.dryrun:
                logger.debug(f'DRYRUN: Skip making Directories to {proj_dir}')
            else:
                os.makedirs(proj_dir)
        else:
            if not args.forceful:
                logger.warn(f'{proj_code} directory already exists')

        status = make_filelist(pattern, proj_dir, logger)
        if not status:
            logger.error(f'Issue creating filelist for {proj_code}')
        else:
            base_file = f'{proj_dir}/base-cfg.json'

            if not os.path.isfile(base_file) or args.forceful:
                if args.dryrun:
                    logger.debug(f'DRYRUN: Skip writing base file {base_file}')
                else:
                    with open(base_file,'w') as f:
                        f.write(json.dumps(cfg_values))
            else:
                logger.warn(f'{base_file} already exists - skipping')

    logger.info(f'Exporting {len(proj_codes)} dataset config files')

    if args.dryrun:
        logger.debug(f'DRYRUN: Skip writing {len(proj_codes)} project codes list {args.groupdir}/proj_codes_1.txt')
    else:
        with open(f'{args.groupdir}/proj_codes_1.txt','w') as f:
            f.write('\n'.join(proj_codes))

    logger.info(f'Written as group ID: {args.groupID}')

def init_config(args):
    """Main configuration script, load configurations from input sources"""

    logger = init_logger(args.verbose, args.mode, 'init')
    logger.info('Starting initialisation')

    groupID = None
    if hasattr(args, 'groupID'):
        groupID = getattr(args,'groupID')
    if hasattr(args,'group'):
        groupID = getattr(args,'group')

    if groupID:
        logger.debug('Starting group initialisation')
        if not hasattr(args,'input'):
            logger.error('Group run requires input file in csv or txt format')
            return None
        
        if '.txt' in args.input:
            logger.debug('Converting text file to csv')
            status = text_file_to_csv(args, logger) # Includes creating csv
            if not status:
                return None
        elif '.csv' in args.input:
            logger.debug('Ingesting csv file')
            new_csv = f'{args.groupdir}/datasets.csv'
            if not os.path.isdir(args.groupdir):
                os.makedirs(args.groupdir)

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
