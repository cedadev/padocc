
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import json
import logging
import glob

from padocc.core import FalseLogger
from padocc.operations import GroupOperation, ProjectOperation
from padocc.core.utils import extract_file

config = {
    'proj_code': None,
    'pattern': None,
    'update': None,
    'remove': None
}

def _get_updates(
        logger: logging.logger | FalseLogger = FalseLogger()):
    """
    Get key-value pairs for updating in final metadata.
    """

    logger.debug('Getting update key-pairs')
    inp = None
    valsdict = {}
    while inp != 'exit':
        inp = input('Attribute: ("exit" to escape):')
        if inp != 'exit':
            val = input('Value: ')
            valsdict[inp] = val
    return valsdict

def _get_removals(
        logger: logging.logger | FalseLogger = FalseLogger()):
    """
    Get attribute names to remove in final metadata.
    """

    logger.debug('Getting removals')
    valsarr = []
    inp = None
    while inp != 'exit':
        inp = input('Attribute: ("exit" to escape):')
        if inp != 'exit':
            valsarr.append(inp)
    return valsarr

def _get_proj_code(path: str, prefix: str = ''):
    """Determine project code from path (prefix removed), appropriate for CMIP6"""
    parts = path.replace(prefix,'').replace('/','_').split('_')
    if '*.' in parts[-1]:
        parts = parts[:-2]
    return '_'.join(parts)

def _create_csv_from_text(text, logger):
    """
    Padocc accepts a text file where the individual entries can be 
    broken down into DOIs for the different projects.
    """
    raise NotImplementedError
    return

    logger.debug('Converting text file to csv')

    if new_inputfile != input_file:
        if self._dryrun:
            self.logger.debug(f'DRYRUN: Skip copying input file {input_file} to {new_inputfile}')
        else:
            os.system(f'cp {input_file} {new_inputfile}')

    with open(new_inputfile) as f:
        datasets = [r.strip() for r in f.readlines()]

    if not os.path.isfile(f'{self.groupdir}/datasets.csv') or self._forceful:
        records = ''
        self.logger.info('Creating filesets for each dataset')
        for index, ds in enumerate(datasets):

            skip = False

            pattern = str(ds)
            if not (pattern.endswith('.nc') or pattern.endswith('.tif')):
                self.logger.debug('Identifying extension')
                fileset = [r.split('.')[-1] for r in glob.glob(f'{pattern}/*')]
                if len(set(fileset)) > 1:
                    self.logger.error(f'File type not specified for {pattern} - found multiple ')
                    skip = True
                elif len(set(fileset)) == 0:
                    skip = True
                else:
                    extension = list(set(fileset))[0]
                    pattern = f'{pattern}/*.{extension}'
                self.logger.debug(f'Found .{extension} common type')

            if not skip:
                proj_op = ProjectOperation(
                    self.workdir, 
                    _get_proj_code(ds, prefix=prefix),
                    groupID = self.groupID)
                
                self.logger.debug(f'Assembled project code: {proj_op}')

                if 'latest' in pattern:
                    pattern = pattern.replace('latest', os.readlink(pattern))

                records  += f'{proj_op},{pattern},,\n'
                self.logger.debug(f'Added entry and created fileset for {index+1}/{len(datasets)}')
        if self._dryrun:
            self.logger.debug(f'DRYRUN: Skip creating csv file {self.groupdir}/datasets.csv')    
        else:        
            with open(f'{self.groupdir}/datasets.csv','w') as f:
                f.write(records)
    else:
        self.logger.warn(f'Using existing csv file at {self.groupdir}/datasets.csv')

def _get_input(
        workdir : str,
        logger  : logging.logger | FalseLogger = FalseLogger(), 
        forceful : bool = None):
    """
    Get command-line inputs for specific project configuration. 
    Init requires the following parameters: proj_code, pattern/filelist, workdir.
    """

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
        env_workdir = os.getenv('WORKDIR')

    if workdir and workdir != env_workdir:
        print('Environment workdir does not match provided address')
        print('ENV:',env_workdir)
        print('ARG:',workdir)
        choice = input('Choose to keep the ENV value or overwrite with the ARG value: (E/A) :')
        if choice == 'E':
            pass
        elif choice == 'A':
            os.environ['WORKDIR'] = workdir
            env_workdir = workdir
        else:
            print('Invalid input, exiting')
            return None

    proj_dir = f'{workdir}/in_progress/{proj_code}'
    if os.path.isdir(proj_dir):
        if forceful:
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
        config['update'] = _get_updates()
    
    do_removals = input('Do you wish to remove known attributes from the metadata? (y/n): ')
    if do_removals == 'y':
        config['remove'] = _get_removals(remove=True)

    if pattern:
        config['pattern'] = pattern

    # Should return input content in a proper format (for a single project.)

    return config

class InitOperation(GroupOperation):

    def __init__(
            self, 
            workdir : str, 
            groupID : str = None, 
            dryrun  : bool = True,
            forceful: bool = False,
            **kwargs
        ) -> None:
        """
        Initialise an init operation with setup parameters.

        :param workdir:         (str) Path to the current working directory.

        :param groupID:         (str) Name of current dataset group.

        :param dryrun:          (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.

        :param forceful:        (bool) Continue with processing even if final output file 
            already exists.

        :returns None:
        """

        super().__init__(workdir, groupID=groupID, dryrun=dryrun, forceful=forceful, **kwargs)

        self._setup_directories()

    @classmethod
    def help(cls):
        print("Create an instance of me with the following arguments: workdir, groupID")
        print('Then run "init_config" and pass me an input_file and I will create your pipeline group!')

    def run(self, input_file: str):
        """
        Run initialisation by loading configurations from input sources, determine
        input file type and use appropriate functions to instantiate group and project
        directories.
        
        :param input_file:      (str) Path to an input file from which to initialise the project.

        :returns:   None
        """
        self.logger.info('Starting initialisation')

        if not input_file:
            if self.groupID:
                self.logger.error('Initialisation requires input file in csv or txt format')
                return

            try:
                manual_config = _get_input(self.logger, self.workdir, forceful=self._forceful)
            except KeyboardInterrupt:
                self.logger.info('Aborting user input process and exiting')
                return
            except Exception as e:
                self.logger.error(f'User Input Error - {e}')
                return

            self._init_project(manual_config)
            return

        if not input_file.startswith('/'):
            pwd = os.getcwd()
            self.logger.info(f'Copying input file from relative path - resolved to {pwd}')
            input_file = os.path.join(pwd, input_file)

        if self.groupID:
            self.logger.debug('Starting group initialisation')
            if '.txt' in input_file:
                self.logger.debug('Converting text file to csv')
                textcontent  = extract_file(input_file)
                group_config = _create_csv_from_text(textcontent)

            elif '.csv' in input_file:
                self.logger.debug('Ingesting csv file')

                group_config = extract_file(input_file)
            self._init_group(group_config)

        else:
            # Only base-cfg style files are accepted here.
            self.logger.debug('Starting single project initialisation')

            if not input_file.endswith('.json'):
                self.logger.error(
                    'Format of input file not recognised.'
                    ' - single projects must be initialised using a ".json" file.')

            with open(input_file) as f:
                provided_config = json.load(f)
            self._init_project(provided_config)

    def _init_project(self, config: dict):
        """
        Create a first-time ProjectOperation and save created files. 
        """
        proj_op = ProjectOperation(
            config['proj_code'],
            config['workdir'],
            self.groupID,
            first_time = True,
            ft_kwargs=config,
            logger=self.logger
        )

        proj_op.save_files()

    def _init_group(self, datasets : list):
        """
        Create a new group within the working directory, and all 
        associated projects.
        """

        self.logger.info('Creating project directories')
        # Group config is the contents of datasets.csv
        self.datasets.set(datasets)

        if 'proj_code' in datasets[0]:
            datasets = datasets[1:]
        
        def _open_json(file):
            with open(file) as f:
                return json.load(f)

        proj_codes = []
        for index in range(len(datasets)):
            cfg_values = {}
            ds_values  = datasets[index].split(',')

            proj_code               = ds_values[0]
            cfg_values['pattern']   = ds_values[1]
            proj_codes.append(proj_code)

            if len(ds_values) > 2:
                if os.path.isfile(ds_values[2]):
                    cfg_values['update'] = _open_json(ds_values[2])
                else:
                    cfg_values['update'] = ds_values[2]

            if len(ds_values) > 3:
                if os.path.isfile(ds_values[3]):
                    cfg_values['remove'] = _open_json(ds_values[3])
                else:
                    cfg_values['remove'] = ds_values[3]

            self.logger.info(f'Creating directories/filelists for {index+1}/{len(datasets)}')

            proj_op = ProjectOperation(
                self.workdir, 
                proj_code, 
                groupID=self.groupID,
                logger=self.logger,
                first_time=True,
                **cfg_values,
            )

            proj_op.update_status('init','complete')
            proj_op.save_files()

        self.logger.info(f'Created {len(datasets)*6} files, {len(datasets)*2} directories in group {self.groupID}')
        self.add_proj_codeset('main',proj_codes)
        self.logger.info(f'Written as group ID: {self.groupID}')
        self.save_files()

if __name__ == '__main__':
    print('Kerchunk Pipeline Config Initialiser - run using master scripts')
