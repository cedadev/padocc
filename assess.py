import os
import sys
import argparse
import glob
import logging

levels = [
    logging.WARN,
    logging.INFO,
    logging.DEBUG
]
"""
"""

# Hints for errors
HINTS = {
    'TrueShapeValidationError': "Kerchunk array shape doesn't match netcdf - missing timesteps or write issue?",
    'slurmstepd': "Ran out of time in job",
    'INFO [main]': "No error recorded",
    'MissingKerchunkError': "Missing the Kerchunk file",
    'KerchunkDriverFatalError': "Kerchunking failed for one or more files",
    'ExpectTimeoutError': "Time remaining estimate exceeded allowed job time (scan)"
}

phases = ['scan', 'compute', 'validate']
checks = ['/detail-cfg.json','/*kerchunk*','/*.complete']

def format_str(string, length):
    while len(string) < length:
        string += ' '
    return string[:length]

def init_logger(verbose, mode, name):
    """Logger object init and configure with formatting

    Parameters
    ----------
    verbose : (int)
        Display level can range from 0-2 for WARNING, INFO and DEBUG.

    mode : int
        Unused mode for saving data.

    name : str
        Name of master script from which logger is defined.
    
    :return: Logging-type object"""
    verbose = min(verbose, len(levels)-1)

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

def find_redos(phase, workdir, groupID, check, ignore=[]):
    checkdir = f'{workdir}/in_progress/{groupID}/'
    proj_codes = os.listdir(checkdir)

    #if phase == 'validate':
        #checkdir = f'{args.workdir}/complete/{args.groupID}/'
    redo_pcodes = []
    complete = []
    for pcode in proj_codes:
        check_file = checkdir + pcode + check
        #if phase == 'validate':
            #print(check_file)
        if pcode not in ignore:
            if glob.glob(check_file):
                if phase == 'validate':
                    complete.append(pcode)
                else:
                    pass
            else:
                redo_pcodes.append(pcode)
    return redo_pcodes, complete

def get_code_from_val(path, code):
    path = path.split('*')[0]
    if os.path.isfile(f'{path}proj_codes.txt'):
        with open(f'{path}proj_codes.txt') as f:
            try:
                code = f.readlines()[int(code)]
            except IndexError:
                print('code',code)
                code = 'N/A'
    else:
        code = 'N/A'
    return code

def extract_keys(filepath, logger, savetype=None, examine=None):
    keys       = {}
    savedcodes = []
    total      = 0
    listfiles  = glob.glob(filepath)
    logger.info(f'Found {len(listfiles)} files to assess')

    for efile in listfiles:
        logger.debug(f'Starting {efile}')
        total += 1
        with open(os.path.join(filepath, efile)) as f:
            log = [r.strip() for r in f.readlines()]
        logger.debug(f'Opened {efile}')
        # Extract Error type from Error file last line
        if len(log) > 0:
            if type(log[-1]) == str:
                key = log[-1].split(':')[0]
            else:
                key = log[-1][0]

            logger.debug(f'Identified error type {key}')
            # Count error types
            if key in keys:
                keys[key] += 1
            else:
                keys[key] = 1
            # Select specific errors to examine
            if key == savetype:
                ecode = efile.split('/')[-1].split('.')[0]
                code = get_code_from_val(filepath, ecode)
                savedcodes.append((efile, code, log))
                if examine:
                    print(f'{efile} - {code}')
                    print()
                    print('\n'.join(log))
                    x=input()
                    if x == 'E':
                        raise Exception
    return savedcodes, keys, total

def check_errs(path, logger, savetype=None, examine=None):

    savedcodes, errs, total = extract_keys(path, logger, savetype=savetype, examine=examine)
    
    # Summarise results
    print(f'Found {total} error files:')
    for key in errs.keys():
        if errs[key] > 0:
            known_hint = 'Unknown'
            if key in HINTS:
                known_hint = HINTS[key]
            print(f'{key}: {errs[key]}    - ({known_hint})')

    return savedcodes

def get_attribute(env, args, var):
    """Assemble environment variable or take from passed argument."""
    if os.getenv(env):
        return os.getenv(env)
    elif hasattr(args, var):
        return getattr(args, var)
    else:
        print(f'Error: Missing attribute {var}')
        return None
    
def save_sel(codes, groupdir, label, logger):
    if len(codes) > 1:
        codeset = ''.join([code[1] for code in codes])
        with open(f'{groupdir}/proj_codes_{label}.txt','w') as f:
            f.write(codeset)

        logger.info(f'Written {len(codes)} to proj_codes_{label}')
    else:
        logger.info('No codes identified, no files written')

def show_options(option, groupdir, operation, logger):
    if option == 'jobids':
        logger.info('Detecting IDs from previous runs:')
        if operation == 'outputs':
            os.system(f'ls {groupdir}/outs/')
        else:
            os.system(f'ls {groupdir}/errs/')
    else:
        logger.info('Detecting labels from previous runs:')
        labels = glob.glob(f'{args.workdir}/groups/{args.groupID}/proj_codes*')
        for l in labels:
            pcode = l.split('/')[-1].replace("proj_codes_","").replace(".txt","")
            if pcode == '1':
                pcode = 'main'
            logger.info(f'{format_str(pcode,20)} - {l}')

def cleanup(cleantype, groupdir, logger):
    if cleantype == 'proj_codes':
        projset = glob.glob(f'{groupdir}/proj_codes_*')
        for p in projset:
            if 'proj_codes_1' not in p:
                os.system(f'rm {p}')
    elif cleantype == 'errors':
        os.system(f'rm {groupdir}/errs/*')
    elif cleantype == 'outputs':
        os.system(f'rm {groupdir}/outs/*')
    else:
        pass

def progress_check(args, logger):
    if args.phase not in phases:
        logger.error(f'Phase not accepted here - {args.phase}')
        return None
    else:
        logger.info(f'Discovering dataset progress within group {args.groupID}')
        redo_pcodes = []
        for index, phase in enumerate(phases):
            redo_pcodes, completes = find_redos(phase, args.workdir, args.groupID, checks[index], ignore=redo_pcodes)
            logger.info(f'{phase}: {len(redo_pcodes)} datasets')
            if completes:
                logger.info(f'complete: {len(completes)} datasets')
            if phase == args.phase:
                break

    # Write pcodes
    if not args.repeat_label:
        id = 1
        new_projcode_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{args.phase}_{id}.txt'
        while os.path.isfile(new_projcode_file):
            id += 1
            new_projcode_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{args.phase}_{id}.txt'

        args.repeat_label = f'{args.phase}_{id}'

    new_projcode_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{args.repeat_label}.txt'

    if args.write:
        with open(new_projcode_file,'w') as f:
            f.write('\n'.join(redo_pcodes))

        # Written new pcodes
        print(f'Written {len(redo_pcodes)} pcodes, repeat label: {args.repeat_label}')

def error_check(args, logger):
    job_path = f'{args.workdir}/groups/{args.groupID}/errs/{args.jobID}'
    logger.info(f'Checking error files for {args.groupID} ID: {args.jobID}')

    savedcodes, errs, total = extract_keys(f'{job_path}/*.err', logger, savetype=args.inspect, examine=args.examine)
    
    # Summarise results
    print(f'Found {total} error files:')
    for key in errs.keys():
        if errs[key] > 0:
            known_hint = 'Unknown'
            if key in HINTS:
                known_hint = HINTS[key]
            print(f'{key}: {errs[key]}    - ({known_hint})')

    if args.repeat_label and args.write:
        save_sel(savedcodes, args.groupdir, args.repeat_label, logger)
    elif args.repeat_label:
        logger.info(f'Skipped writing {len(savedcodes)} to proj_codes_{args.repeat_label}')
    else:
        pass

def output_check(args, logger):
    job_path = f'{args.workdir}/groups/{args.groupID}/errs/{args.jobID}'
    logger.info(f'Checking output files for {args.groupID} ID: {args.jobID}')
    raise NotImplementedError

operations = {
    'progress': progress_check,
    'errors': error_check,
    'outputs': output_check
}

def assess_main(args):

    logger = init_logger(args.verbose, args.mode, 'assessor')

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')
    args.groupdir = f'{args.workdir}/groups/{args.groupID}'

    if args.show_opts:
        show_options(args.show_opts, args.groupdir, args.operation, logger)
        return None

    if args.cleanup:
        cleanup(args.cleanup, args.groupdir, logger)
        return None

    if args.operation in operations:
        operations[args.operation](args, logger)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('groupID',type=str, help='Group identifier code')
    parser.add_argument('operation',type=str, help='Operation to perform', choices=['progress','errors','outputs'])


    parser.add_argument('-j','--jobid', dest='jobID', help='Identifier of job to inspect')
    parser.add_argument('-p','--phase', dest='phase', default='validate', help='Pipeline phase to inspect')
    parser.add_argument('-s','--show-opts', dest='show_opts', help='Show options for jobids, repeat label')

    parser.add_argument('-r','--repeat_label', dest='repeat_label', default=None, help='Save a selection of codes which failed on a given error - input a repeat id.')
    parser.add_argument('-i','--inspect', dest='inspect', help='Inspect error/output of a given type/label')
    parser.add_argument('-E','--examine', dest='examine', action='store_true', help='Examine log outputs individually.')
    parser.add_argument('-c','--clean-up', dest='cleanup', default=None, help='Clean up group directory of errors/outputs/dataset lists', choices=['proj_codes','errors','outputs'])


    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=1, help='Print helpful statements while running')
    parser.add_argument('-m','--mode',        dest='mode', default=None, help='Print or record information (log or std)')
    parser.add_argument('-W','--write',  dest='write',  action='store_true', help='Write outputs to files' )

    args = parser.parse_args()

    assess_main(args)
        