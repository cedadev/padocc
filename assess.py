import os
import argparse
import glob
import json
import sys

from pipeline.logs import init_logger, get_attribute
from pipeline.errors import MissingVariableError

# Hints for errors
HINTS = {
    'TrueShapeValidationError': "Kerchunk array shape doesn't match netcdf - missing timesteps or write issue?",
    'slurmstepd': "Ran out of time in job",
    'INFO [main]': "No error recorded",
    'MissingKerchunkError': "Missing the Kerchunk file",
    'KerchunkDriverFatalError': "Kerchunking failed for one or more files",
    'ExpectTimeoutError': "Time remaining estimate exceeded allowed job time (scan)",
    'BlackListProjectCode': "Problematic Project currently on blacklist so being ignored"
}

phases = ['scan', 'compute', 'validate', 'complete']
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
        List of project codes considered to be complete for the whole pipeline.
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

def get_code_from_val(path: str, index: str, filename='proj_codes'):
    """Takes some index value from command line and fetches the corresponding project code.
    
    Project codes stored in proj_codes.txt under each job id.

    Returns
    -------
    proj_code : str
        Project code that matches the provided index
    """
    path = path.split('*')[0]
    if os.path.isfile(f'{path}/{filename}.txt'):
        with open(f'{path}/{filename}.txt') as f:
            try:
                code = f.readlines()[int(index)].strip()
            except IndexError:
                print('code',index)
                code = 'N/A'
    else:
        code = 'N/A'
    return code

def get_rerun_command(phase: str, ecode: str, groupID: str, repeat_id: str):
    print(f'python single_run.py {phase} {ecode} -G {groupID} -r {repeat_id} -vvv -d')

def examine_log(log: str, efile: str, code: str, ecode=None, phase=None, groupID=None, repeat_id=None):
    print()
    print('\n'.join(log))
    print(f'{efile} - {code}')
    print('Rerun suggested command:    ',end='')
    if phase and ecode:
        get_rerun_command(phase, ecode, groupID, repeat_id)
    paused=input('Type "E" to exit assessment: ')
    if paused == 'E':
        sys.exit()

def extract_keys(filepath: str, logger, savetype=None, examine=None, phase=None, groupID=None, repeat_id=None):
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
    keys       = {'Warning': 0}
    savedcodes = []
    listfiles  = glob.glob(filepath)
    total      = len(listfiles)
    logger.info(f'Found {len(listfiles)} files to assess')

    for efile in listfiles:
        logger.debug(f'Starting {efile}')
        log = []
        show_warn = False
        with open(os.path.join(filepath, efile)) as f:
            for r in f.readlines():
                log.append(r.strip())
                if 'WARNING' in r.strip():
                    show_warn = (savetype == 'Warning')
                    keys['Warning'] += 1
        logger.debug(f'Opened {efile}')
        # Extract Error type from Error file last line
        if len(log) > 0:
            if type(log[-1]) == str:
                key = log[-1].split(':')[0]
            else:
                key = log[-1][0]

            if '/var/spool/slurmd' in key:
                key = 'SlurmMemoryError'

            if key == 'slurmstepd':
                key = 'SlurmTimeoutError'

            logger.debug(f'Identified error type {key}')
            # Count error types
            if key in keys:
                keys[key] += 1
            else:
                keys[key] = 1

            matchtype = (key == savetype or (type(savetype) == list and key in savetype) or show_warn)

            ecode = efile.split('/')[-1].split('.')[0]
            path = '/'.join(filepath.split('/')[:-1]) + '/'
            code = get_code_from_val(path, ecode)

            if matchtype and not blacklisted(code, f'{filepath}/../', logger):
                # Save matching types
                savedcodes.append((efile, code, log))
                if examine:
                    # Examine logs if matching types or warnings is on
                    examine_log(log, efile, code, ecode=ecode, phase=phase, groupID=groupID, repeat_id=repeat_id)
    return savedcodes, keys, len(listfiles)

def merge_old_new(old_codes, new_codes, index_old='', index_new='', reason=None):
    merge_dict = {}
    indices = [index_old, index_new]
    for ord, codeset in enumerate([old_codes, new_codes]):
        index = indices[ord]
        for code in codeset:
            proj_code = code
            value = None
            if index != '':
                proj_code = code[index]
            if reason:
                if ord != 0 and len(code) > 1:
                    value = reason
                else:
                    value = code[1]
            merge_dict[proj_code] = value
    merged = []
    for mkey in merge_dict.keys():
        if merge_dict[mkey]:
            merged.append(f'{mkey}, {merge_dict[mkey]}')
        else:
            merged.append(f'{mkey}')
    return merged

def save_sel(codes: list, groupdir: str, label: str, logger, overwrite=0):
    """Save selection of codes to a file with a given repeat label. 
    
    Requires a groupdir (directory belonging to a group), list of codes and a label for the new file.
    """
    if len(codes) > 0:
        codeset = '\n'.join([code[1].strip() for code in codes])
        if os.path.isfile(f'{groupdir}/proj_codes_{label}.txt'):
            if overwrite == 0:
                logger.info(f'Skipped writing {len(codes)} to proj_codes_{label} - file exists and overwrite not set')
            elif overwrite == 1:
                logger.info(f'Adding {len(codes)} to existing proj_codes_{label}')
                with open(f'{groupdir}/proj_codes_{label}.txt') as f:
                    old_codes = [r.strip() for r in f.readlines()]
                merged = merge_old_new(old_codes, codes, index_new=1)
                # Need check for duplicates here
                with open(f'{groupdir}/proj_codes_{label}.txt','w') as f:
                    f.write('\n'.join(merged))
            elif overwrite == 2:
                logger.info(f'Overwriting with {len(codes)} in existing proj_codes_{label} file')
                with open(f'{groupdir}/proj_codes_{label}.txt','w') as f:
                    f.write(codeset)
        else:
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
    elif option == 'labels':
        logger.info('Detecting labels from previous runs:')
        labels = glob.glob(f'{args.workdir}/groups/{args.groupID}/proj_codes*')
        for l in labels:
            pcode = l.split('/')[-1].replace("proj_codes_","").replace(".txt","")
            if pcode == '1':
                pcode = 'main'
            logger.info(f'{format_str(pcode,20)} - {l}')
    else:
        logger.info(f'{option} not accepted - use "jobids" or "labels"')

def cleanup(cleantype: str, groupdir: str, logger):
    """Remove older versions of project code files, error or output logs. Clear directories."""
    if cleantype == 'labels':
        projset = glob.glob(f'{groupdir}/proj_codes_*')
        for p in projset:
            if 'proj_codes_1' not in p:
                os.system(f'rm {p}')
    elif cleantype == 'errors':
        os.system(f'rm -rf {groupdir}/errs/*')
    elif cleantype == 'outputs':
        os.system(f'rm -rf {groupdir}/outs/*')
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
        ignore_pcodes = []
        blacklist = get_blacklist(args.groupdir)
        if blacklist:
            ignore_pcodes = [b.split(',')[0] for b in blacklist]
            logger.info(f'blacklist: {len(blacklist)} datasets')

        for index, phase in enumerate(phases[:-1]): # Ignore complete check as this happens as a byproduct
            redo_pcodes, completes = find_codes(phase, args.workdir, args.groupID, checks[index], ignore=ignore_pcodes)
            logger.info(f'{phase}: {len(redo_pcodes)} datasets')
            if completes:
                logger.info(f'complete: {len(completes)} datasets')
            if phase == args.phase:
                break
            ignore_pcodes += redo_pcodes
        if args.phase == 'complete':
            redo_pcodes = completes
        

    # Write pcodes
    if not args.repeat_id:
        id = 1
        new_projcode_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{args.phase}_{id}.txt'
        while os.path.isfile(new_projcode_file):
            id += 1
            new_projcode_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{args.phase}_{id}.txt'

        args.repeat_id = f'{args.phase}_{id}'

    new_projcode_file = f'{args.workdir}/groups/{args.groupID}/proj_codes_{args.repeat_id}.txt'

    if args.write:
        with open(new_projcode_file,'w') as f:
            f.write('\n'.join(redo_pcodes))

        # Written new pcodes
        print(f'Written {len(redo_pcodes)} pcodes, repeat label: {args.repeat_id}')

def error_check(args, logger):
    """Check error files and summarise results
    
    Extract savedcodes and total number of errors from each type given a specific path, save selection of codes for later use if required.
    """
    job_path = f'{args.workdir}/groups/{args.groupID}/errs/{args.jobID}'
    logger.info(f'Checking error files for {args.groupID} ID: {args.jobID}')

    savedcodes, errs, total = extract_keys(f'{job_path}/*.err', logger, 
                                           savetype=args.inspect, examine=args.examine,
                                           phase=args.phase, groupID=args.groupID, 
                                           repeat_id='_'.join(args.jobID.split('_')[1:]))
    
    # Summarise results
    print(f'Found {total} error files:')
    for key in errs.keys():
        if errs[key] > 0 and key != 'Warning':
            known_hint = 'Unknown'
            if key in HINTS:
                known_hint = HINTS[key]
            print(f'{key}: {errs[key]}    - ({known_hint})')

    print('')
    print(f'Identified {errs["Warning"]} files with Warnings')

    if args.inspect:
        logger.info(f'Found {len(savedcodes)} proj_codes with matching errory type "{args.inspect}"')

    if args.write:
        if args.blacklist:
            add_to_blacklist(savedcodes, args.groupdir, args.reason, logger)
        elif args.repeat_id:
            save_sel(savedcodes, args.groupdir, args.repeat_id, logger, overwrite=args.overwrite)
        else:
            logger.info('No repeat_id supplied, proj_codes were not saved.')
    else:
        if len(savedcodes) != 0:
            logger.info(f'Skipped writing {len(savedcodes)}')

def output_check(args, logger):
    """Not implemented output log checker"""
    job_path = f'{args.workdir}/groups/{args.groupID}/errs/{args.jobID}'
    logger.info(f'Checking output files for {args.groupID} ID: {args.jobID}')
    raise NotImplementedError

def add_to_blacklist(savedcodes, groupdir, reason, logger):
    blackfile = f'{groupdir}/blacklist_codes.txt'
    if not os.path.isfile(blackfile):
        os.system(f'touch {blackfile}')
    logger.debug(f'Starting blacklist concatenation')

    merged = ''
    with open(blackfile) as f:
        blackcodes = [r.strip().split(',') for r in f.readlines()]

    merged = merge_old_new(blackcodes, savedcodes, index_old=0, index_new=1, reason=reason)
    print(merged)
    blacklist = '\n'.join([f'{m}' for m in merged])
    
    with open(blackfile,'w') as f:
        f.write(blacklist)
    logger.info(f'Blacklist now contains {len(merged)} codes.')

def get_blacklist(groupdir):
    blackfile = f'{groupdir}/blacklist_codes.txt'
    if os.path.isfile(blackfile):
        with open(blackfile) as f:
            blackcodes = [r.strip().split(',')[0] for r in f.readlines()]
    else:
        return None
    return blackcodes

def blacklisted(proj_code: str, groupdir: str, logger):
    blacklist = get_blacklist(groupdir)
    if blacklist:
        for code in blacklist:
            if proj_code in code:
                return True
        return False
    else:
        logger.debug('No blacklist file preset for this group')
        return False

def retro_errors(args, logger):
    """Retrospective analysis of errors for all project codes within this group.
     - Saved in ErrorSummary.json file"""
    
    proj_file = f'{args.groupdir}/proj_codes_1.txt'
    summ_file  = f'{args.groupdir}/ErrorSummary.json'

    with open(proj_file) as f:
        proj_codes = [r.strip() for r in f.readlines()]

    try:
        with open(summ_file) as f:
            summ_refs = json.load(f)
    except FileNotFoundError:
        logger.info("No prior summary error file detected")
        summ_refs = {}
    
    if args.write:

        logger.info(f"Initialising {len(proj_codes)} error entries")
        # Initialise summary file
        for pcode in proj_codes:
            if pcode not in summ_refs:
                summ_refs[pcode] = None

        errs = {}

        logs = glob.glob(f'{args.groupdir}/errs/*')
        for l in logs:
            logger.info(f'Reading error files for {l.split("/")[-1]}')
            pcode_file = f'{l}/proj_codes.txt'
            try:
                with open(pcode_file) as f:
                    repeat_pcodes = [r.strip() for r in f.readlines()]
            except FileNotFoundError:
                logger.info(f'Skipped {l.split("/")[-1]} - old version')
            for x, rpc in enumerate(repeat_pcodes):
                rpc_err = f'{l}/{x}.err'
                try:
                    with open(rpc_err) as f:
                        err = f.readlines()[-1].split(':')[0]
                except FileNotFoundError:
                    logger.debug(f'Not able to locate error file {x}')
                summ_refs[rpc] = err

    for rpc in summ_refs.keys():
        err = summ_refs[rpc]
        if err not in errs:
            errs[err] = 1
        else:
            errs[err] += 1

    print('Retrospective Error Summary:')
    print(errs)

operations = {
    'progress': progress_check,
    'errors': error_check,
    'outputs': output_check,
    'blacklist': add_to_blacklist,
    'retro': retro_errors
}

def assess_main(args):
    """Main assessment function, different tools diverge from here."""

    logger = init_logger(args.verbose, args.mode, 'assessor')

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')

    if args.groupID == 'A':
        os.system(f'ls {args.workdir}/groups/')
        return None
    args.groupdir = f'{args.workdir}/groups/{args.groupID}'

    if ',' in args.inspect:
        args.inspect = args.inspect.split(',')

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
    parser.add_argument('operation',type=str, help=f'Operation to perform - choose from {operations.keys()}.')

    parser.add_argument('-B','--blacklist', dest='blacklist', action='store_true', help='Use when saving project codes to the blacklist')
    parser.add_argument('-R','--blacklist-reason', dest='reason', help='Provide the reason for saving project codes to the blacklist')

    parser.add_argument('-j','--jobid', dest='jobID', help='Identifier of job to inspect')
    parser.add_argument('-p','--phase', dest='phase', default='validate', help='Pipeline phase to inspect')
    parser.add_argument('-s','--show-opts', dest='show_opts', help='Show options for jobids, labels')

    parser.add_argument('-r','--repeat_id', dest='repeat_id', default=None, help='Save a selection of codes which failed on a given error - input a repeat id.')
    parser.add_argument('-i','--inspect', dest='inspect', default='', help='Inspect error/output of a given type/label')
    parser.add_argument('-E','--examine', dest='examine', action='store_true', help='Examine log outputs individually.')
    parser.add_argument('-c','--clean-up', dest='cleanup', default=None, help='Clean up group directory of errors/outputs/labels')

    parser.add_argument('-O','--overwrite', dest='overwrite', action='count', help='Force overwrite of steps if previously done')

    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=1, help='Print helpful statements while running')
    parser.add_argument('-m','--mode',        dest='mode', default=None, help='Print or record information (log or std)')
    parser.add_argument('-W','--write',  dest='write',  action='store_true', help='Write outputs to files' )

    args = parser.parse_args()

    assess_main(args)
        