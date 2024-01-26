import os
import argparse
import glob

from pipeline.logs import init_logger
from pipeline.errors import MissingVariableError

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

def format_str(string: str, length: int):
    """Simple function to format a string to a correct length"""
    while len(string) < length:
        string += ' '
    return string[:length]

def find_codes(phase: str, workdir: str, groupID: str, check: str, ignore=[]):
    """Find project codes for datasets that failed at various stages of the pipeline
    
    Checks 'in-progress' and 'complete' directories for datasets and config files to determine progress of all datasets.
    
    Parameters
    ----------
    phase : str
        Check config and output files corresponding to a pipeline phase.
    workdir : str
        Path to current working directory of the pipeline.
    groupID : str
        Check pipeline for a specific group ID.
    check : str
        File type or specific file required for this phase to be considered complete
    ignore : list (str-like)

    Returns
    -------
    redo_pcodes : list (str-like)
        List of project codes to re-run for this phase.
    complete : list (str-like)
        List of project codes considered to be complete for the whole pipeline
       """
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

def get_code_from_val(path: str, index: str):
    """Takes some index value from command line and fetches the corresponding project code.
    
    Project codes stored in proj_codes.txt under each job id.

    Returns
    -------
    proj_code : str
        Project code that matches the provided index
    """
    path = path.split('*')[0]
    if os.path.isfile(f'{path}/proj_codes.txt'):
        with open(f'{path}/proj_codes.txt') as f:
            try:
                code = f.readlines()[int(index)]
            except IndexError:
                print('code',index)
                code = 'N/A'
    else:
        code = 'N/A'
    return code

def extract_keys(filepath: str, logger, savetype=None, examine=None):
    """Extract keys from error/output files, collect into groups and examine a particular type if required.
    
    Parameters
    ----------
    filepath : str
        String path to the error/output files
    logger : Logging.object
        Logger object for warning/info/debug messages.
    savetype : str or bool
        Error code to compare to error files and save matching codes to savedcodes array.
    examine : bool
        Boolean switch for halting if matching code is found.

    Returns
    -------
    savedcodes : list
        List of tuples for each file which abides by the savetype. Stored values in each tuple are:
         - efile (str) : Path to error/output file
         - code (str)  : Project code of error file
         - log (str)   : Full error log for this project code
    keys : dict
        Dictionary of number of occurrences across all error logs of specific error codes.
    total : int
        Total number of error files found under the path provided. 
    """
    keys       = {}
    savedcodes = []
    listfiles  = glob.glob(filepath)
    total      = len(listfiles)
    logger.info(f'Found {len(listfiles)} files to assess')

    for efile in listfiles:
        logger.debug(f'Starting {efile}')
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
                path = filepath[:-1]
                code = get_code_from_val(path, ecode)
                savedcodes.append((efile, code, log))
                if examine:
                    print(f'{efile} - {code}')
                    print()
                    print('\n'.join(log))
                    x=input()
                    if x == 'E':
                        raise Exception
    return savedcodes, keys, len(listfiles)

def get_attribute(env: str, args, var: str):
    """Assemble environment variable or take from passed argument.
    
    Finds value of variable from Environment or ParseArgs object, or reports failure
    """
    if os.getenv(env):
        return os.getenv(env)
    elif hasattr(args, var):
        return getattr(args, var)
    else:
        raise MissingVariableError(type='$WORKDIR')
    
def save_sel(codes: list, groupdir: str, label: str, logger):
    """Save selection of codes to a file with a given repeat label. 
    
    Requires a groupdir (directory belonging to a group), list of codes and a label for the new file.
    """
    if len(codes) > 1:
        codeset = ''.join([code[1] for code in codes])
        with open(f'{groupdir}/proj_codes_{label}.txt','w') as f:
            f.write(codeset)

        logger.info(f'Written {len(codes)} to proj_codes_{label}')
    else:
        logger.info('No codes identified, no files written')

def show_options(option: str, groupdir: str, operation: str, logger):
    """Use OS tools to list contents of relevant directories to see all jobids or labels.
    
    List output or error directories (one per job id), or list all proj_codes text files."""
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

def cleanup(cleantype: str, groupdir: str, logger):
    """Remove older versions of project code files, error or output logs. Clear directories."""
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
    """Check general progress of pipeline for a specific group.
    
    Lists progress up to the provided phase, options to save all project codes stuck at a specific phase to a repeat_id for later use."""
    if args.phase not in phases:
        logger.error(f'Phase not accepted here - {args.phase}')
        return None
    else:
        logger.info(f'Discovering dataset progress within group {args.groupID}')
        redo_pcodes = []
        for index, phase in enumerate(phases):
            redo_pcodes, completes = find_codes(phase, args.workdir, args.groupID, checks[index], ignore=redo_pcodes)
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
    """Check error files and summarise results
    
    Extract savedcodes and total number of errors from each type given a specific path, save selection of codes for later use if required.
    """
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
    """Not implemented output log checker"""
    job_path = f'{args.workdir}/groups/{args.groupID}/errs/{args.jobID}'
    logger.info(f'Checking output files for {args.groupID} ID: {args.jobID}')
    raise NotImplementedError

operations = {
    'progress': progress_check,
    'errors': error_check,
    'outputs': output_check
}

def assess_main(args):
    """Main assessment function, different tools diverge from here."""

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
    parser.add_argument('operation',type=str, help='Operation to perform - choose from `progress`,`errors`,`outputs`.')


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
        