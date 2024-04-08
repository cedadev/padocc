__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import argparse
import glob
import json
import sys
from datetime import datetime
import re

from pipeline.logs import init_logger, log_status, get_log_status, FalseLogger
from pipeline.utils import get_attribute, format_str, \
    mem_to_val, get_codes, set_codes, get_proj_file, \
    set_proj_file
import pipeline.errors as errs

# Hints for custom errors - unused
"""
HINTS = {}
for obj in dir(errs):
    object = getattr(errs, obj)
    try:
        inst = object()
        HINTS[inst.get_str()] = inst.message
    except:
        pass
"""

def get_rerun_command(phase: str, ecode: str, groupID: str, repeat_id: str) -> None:
    """
    Print a rerun command for inspecting a single dataset using single_run.py

    :param phase:       (str) The current phase

    :param ecode:       (str) The index of the project code to be inspected by running
                        in serial.

    :param groupID:     (str) The name of the group which this project code belongs to.

    :param repeat_id:   (str) The subset within the group (default is main)

    :returns:
    """
    if repeat_id != 'main': 
        print(f'python single_run.py {phase} {ecode} -G {groupID} -r {repeat_id} -vv -d')
    else:
        print(f'python single_run.py {phase} {ecode} -G {groupID} -vv -d')

def get_index_of_code(workdir: str, groupID: str, repeat_id: str, code: str) -> int:
    """
    Get the index of a project code within some repeat set of codes

    :param workdir:     (str) The current pipeline working directory.

    :param groupID:     (str) The name of the group which this project code belongs to.

    :param repeat_id:   (str) The subset within the group (default is main)

    :param code:        (str) The project code for which to get the index.

    :returns pindex:    (int) The index of the project code within this group-subgroup.
    """
    proj_codes = get_codes(groupID, workdir, f'proj_codes/{repeat_id}')
    pindex = 0
    pcode = proj_codes[pindex]
    while pcode != code:
        pindex += 1
        pcode = proj_codes[pindex]
    return pindex

def examine_log(workdir: str, proj_code: str, phase: str, groupID=None, repeat_id=None, error=None):
    """Open and examine a log file from a previous run of a given phase
    - Show full error log
    - Suggest rerun/examination command
    """
    if groupID:
        proj_dir = f'{workdir}/in_progress/{groupID}/{proj_code}'

    phase_log = f'{proj_dir}/phase_logs/{phase}.log'
    if os.path.isfile(phase_log):
        with open(phase_log) as f:
            log = [r.strip() for r in f.readlines()]
    else:
        print(f'Phase log file not found: {phase_log}')
        log = []

    print()
    print('\n'.join(log))
    print(f'Project Code: {proj_code} - {error}')
    print('Rerun suggested command:    ',end='')

    ecode = get_index_of_code(workdir, groupID, repeat_id, proj_code)
    get_rerun_command(phase, ecode, groupID, repeat_id)

    paused=input('Type "E" to exit assessment: ')
    if paused == 'E':
        raise KeyboardInterrupt

def merge_old_new(old_codes, new_codes, index_old='', index_new='', reason=None):
    """Merge an existing list of project codes with a new set
    - Uses indexes if not a 1d list
    """

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

def save_selection(codes: list, groupdir: str, label: str, logger, overwrite=0, index=False):
    """Save selection of codes to a file with a given repeat label. 
    
    Requires a groupdir (directory belonging to a group), list of codes and a label for the new file.
    """

    # Annoying it seems to require force-removing 'None' values.
    if not overwrite:
        overwrite=0

    if len(codes) > 0:
        if index:
            codeset = '\n'.join([code[index].strip() for code in codes])
        else:
            codeset = '\n'.join(codes)

        write = True
        if get_codes(groupdir, None, f'proj_codes/{label}'):
            if overwrite == 0:
                print(f'Skipped writing {len(codes)} to proj_codes/{label} - file exists and overwrite not set')
                write = False
            elif overwrite == 1:
                print(f'Adding {len(codes)} to existing proj_codes/{label}')
                old_codes = get_codes(groupdir, None, f'proj_codes/{label}')
                codeset = '\n'.join(merge_old_new(old_codes, codes))
            elif overwrite >= 2:
                print(f'Overwriting with {len(codes)} in existing proj_codes/{label} file')
        if write:
            print(f'writing {len(codes)} to proj_codes/{label}')
            set_codes(groupdir, None, f'proj_codes/{label}', codeset)
    else:
        print('No codes identified, no files written')

def show_options(args, logger):
    """Use OS tools to list contents of relevant directories to see all jobids or labels.
    
    List output or error directories (one per job id), or list all proj_codes text files."""

    # Load the blacklist here 
    if args.option in ['blacklist', 'virtuals','parquet'] or 'variable' in args.option:
        blackset = get_codes(args.groupID, args.workdir, 'blacklist_codes')
        if blackset:
            blackcodes = {b.split(',')[0]: b.split(',')[1] for b in blackset}
        else:
            blackcodes = {}

    if args.option == 'groups':
        os.system(f'ls {args.workdir}/groups/')

    elif args.option == 'jobids':
        print('Detecting IDs from previous runs:')
        if args.operation == 'outputs':
            os.system(f'ls {args.groupdir}/outs/')
        else:
            os.system(f'ls {args.groupdir}/errs/')

    elif args.option == 'labels':
        print('Detecting labels from previous runs:')
        labels = glob.glob(f'{args.groupdir}/proj_codes/*')
        for l in labels:
            pcode = l.split('/')[-1].replace(".txt","")
            if pcode == '1':
                pcode = 'main'
            print(f'{format_str(pcode,20)} - {l}')
    elif args.option == 'blacklist':
        print('Current Blacklist:')
        for b in blackcodes.keys():
            print(f'{b} - {blackcodes[b]}')
    elif args.option == 'parquet' or args.option == 'virtuals':
        print(f'Finding datasets that match criteria: {args.option}')
        proj_codes = get_codes(args.groupdir, None, 'proj_codes/main.txt')
        if not proj_codes:
            logger.error(f'No proj_codes file found for {args.groupID}')
            raise FileNotFoundError(file='proj_codes/main.txt')
        non_selected = 0
        supplemental = None
        for x, p in enumerate(proj_codes):
            virtual = False
            parquet = False
            if p in blackcodes:
                continue

            details = get_proj_file(
                f'{args.workdir}/in_progress/{args.groupID}/{p}',
                'detail-cfg.json')
            if details:
                if 'virtual_concat' in details:
                    if details['virtual_concat']:
                        virtual = True
                if 'type' in details:
                    if details['type'] == 'parq':
                        parquet = True
                        supplemental = details['kerchunk_data']
            else:
                print(f'Missing or obstructed detailfile for {p}')
                        
            if virtual and args.option == 'virtual':
                print(f'{x}: {p} - Virtual present')
            elif parquet and args.option == 'parquet':
                print(f'{x}: {p} - Parquet advised ({supplemental})')
            else:
                non_selected += 1
        if args.option == 'parquet':
            print(f'JSON datasets        : {non_selected}')
        else:
            print(f'Non-virtual datasets : {non_selected}')
        print(f'Total datasets       : {len(proj_codes) - len(blackcodes)}')
    elif 'variable' in args.option: 
        raise NotImplementedError
    else:
        print(f'{args.option} not accepted - use "jobids" or "labels"')

def cleanup(args, logger):
    cleantype = args.cleanup
    groupdir  = args.groupdir
    """Remove older versions of project code files, error or output logs. Clear directories."""
    cleaned = False
    if cleantype == 'labels' or cleantype == 'all':
        projset = glob.glob(f'{groupdir}/proj_codes/*')
        for p in projset:
            if 'main' not in p:
                os.system(f'rm {p}')
        cleaned = True
    if cleantype == 'errors' or cleantype == 'all':
        os.system(f'rm -rf {groupdir}/errs/*')
        cleaned = True
    if cleantype == 'outputs' or cleantype == 'all':
        os.system(f'rm -rf {groupdir}/outs/*')
        cleaned = True
    if not cleaned:
        logger.info(f"Cleaning skipped - '{args.cleanup}' is not a known option")
        
def seek_unknown(proj_dir):
    phase = None
    status = 'complete'
    if len(glob.glob(f'{proj_dir}/*complete*')) > 0:
        phase = 'complete'
    elif len(glob.glob(f'{proj_dir}/kerchunk-*.*')) > 0:
        phase = 'compute'
    elif len(glob.glob(f'{proj_dir}/detail-cfg.json')) > 0:
        phase = 'scan'
    elif not os.path.isdir(proj_dir):
        phase = 'init'
        status = 'incomplete'
    else:
        phase = 'init'
    
    log_status(phase, proj_dir, status, FalseLogger())

def force_datetime_decode(datestamp):
    parts = datestamp.split(' ')
    if '/' in parts[0]:
        date = parts[0]
        time = parts[1]
    else:
        date = parts[1]
        time = parts[0]
    month, day, year = date.split('/')
    if len(str(year)) == 2:
        year = '20' + str(year)
    hr, mt = time.split(':')
    dt = datetime(int(year), int(month), int(day), hour=int(hr), minute=int(mt))
    return dt

def progress_check(args, logger):
    """Give a general overview of progress within the pipeline
    - How many datasets currently at each stage of the pipeline
    - Errors within each pipeline phase
    - Allows for examination of error logs
    - Allows saving codes matching an error type into a new repeat group
    """
    blacklist  = get_codes(args.groupID, args.workdir, 'blacklist_codes')
    proj_codes = get_codes(args.groupID, args.workdir, f'proj_codes/{args.repeat_id}')

    if args.write:
        print('Write permission granted:')
        print(' - Will seek status of unknown project codes')
        print(' - Will update status with "JobCancelled" for >24hr pending jobs')

    groupdir = f'{args.workdir}/groups/{args.groupID}'

    done_set = {}
    extras = {'blacklist': {}}
    complete = 0

    for b in blacklist:
        entry = b.replace(' ','').split(',')
        if entry[1] in extras['blacklist']:
            extras['blacklist'][entry[1]].append(0)
        else:
            extras['blacklist'][entry[1]] = [0]
        done_set[entry[0]] = True

    phases = {'init':{}, 'scan': {}, 'compute': {}, 'validate': {}}
    savecodes = []
    longest_err = 0
    for idx, p in enumerate(proj_codes):
        try:
            if p not in done_set:
                proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{p}'
                current = get_log_status(proj_dir)
                if not current:
                    seek_unknown(proj_dir)
                    if 'unknown' in extras:
                        extras['unknown']['no data'].append(idx)
                    else:
                        extras['unknown'] = {'no data':[idx]}
                    continue
                entry = current.split(',')
                if len(entry[1]) > longest_err:
                    longest_err = len(entry[1])

                if entry[1] == 'pending' and args.write:
                    timediff = (datetime.now() - force_datetime_decode(entry[2])).total_seconds()
                    if timediff > 86400: # 1 Day - fixed for now
                        entry[1] = 'JobCancelled'
                        log_status(entry[0], proj_dir, entry[1], FalseLogger())
                
                match_phase = (bool(args.phase) and args.phase == entry[0])
                match_error = (bool(args.error) and any([err == entry[1].split(' ')[0] for err in args.error]))

                if bool(args.phase) != (args.phase == entry[0]):
                    total_match = False
                elif bool(args.error) != (any([err == entry[1].split(' ')[0] for err in args.error])):
                    total_match = False
                else:
                    total_match = match_phase or match_error

                if total_match:
                    if args.examine:
                        examine_log(args.workdir, p, entry[0], groupID=args.groupID, repeat_id=args.repeat_id, error=entry[1])
                    if args.new_id or args.blacklist:
                        savecodes.append(p)

                if entry[0] == 'complete':
                    complete += 1
                else:
                    if entry[1] in phases[entry[0]]:
                        phases[entry[0]][entry[1]].append(idx)
                    else:
                        phases[entry[0]][entry[1]] = [idx]
        except KeyboardInterrupt as err:
            raise err
        except Exception as err:
            examine_log(args.workdir, p, entry[0], groupID=args.groupID, repeat_id=args.repeat_id, error=entry[1])
            print(f'Issue with analysis of error log: {p}')
    num_codes  = len(proj_codes)
    print()
    print(f'Group: {args.groupID}')
    print(f'  Total Codes: {num_codes}')

    def summary_dict(pdict, num_codes, status_len=5, numbers=0):
        """Display summary information for a dictionary structure of the expected format."""
        for entry in pdict.keys():
            pcount = len(list(pdict[entry].keys()))
            num_types = sum([len(pdict[entry][pop]) for pop in pdict[entry].keys()])
            if pcount > 0:
                print()
                fmentry = format_str(entry,10, concat=False)
                fmnum_types = format_str(num_types,5, concat=False)
                fmcalc = format_str(f'{num_types*100/num_codes:.1f}',4, concat=False)
                print(f'   {fmentry}: {fmnum_types} [{fmcalc}%] (Variety: {int(pcount)})')

                # Convert from key : len to key : [list]
                errkeys = reversed(sorted(pdict[entry], key=lambda x:len(pdict[entry][x])))
                for err in errkeys:
                    num_errs = len(pdict[entry][err])
                    if num_errs < numbers:
                        print(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs} (IDs = {list(pdict[entry][err])})')
                    else:
                        print(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs}')
    if not args.new_id:
        print()
        print('Pipeline Current:')
        if not args.long and longest_err > 30:
            longest_err = 30
        summary_dict(phases, num_codes, status_len=longest_err, numbers=int(args.numbers))
        print()
        print('Pipeline Complete:')
        print()
        complete_percent = format_str(f'{complete*100/num_codes:.1f}',4)
        print(f'   complete  : {format_str(complete,5)} [{complete_percent}%]')
        summary_dict(extras, num_codes, status_len=longest_err, numbers=0)
        print()

    if args.new_id:
        logger.debug(f'Preparing to write {len(savecodes)} codes to proj_codes/{args.new_id}.txt')
        if args.write:
            save_selection(savecodes, groupdir, args.new_id, logger, overwrite=args.overwrite)
        else:
            print('Skipped writing new codes - Write flag not present')

    if args.blacklist:
        logger.debug(f'Preparing to add {len(savecodes)} codes to the blacklist')
        if args.write:
            add_to_blacklist(savecodes, args.groupdir, args.reason, logger)
        else:
            print('Skipped blacklisting codes - Write flag not present')

def add_to_blacklist(savedcodes, groupdir, reason, logger):
    """Add a set of codes to the blacklist for a given reason"""
    blackfile = f'{groupdir}/blacklist_codes.txt'
    if not os.path.isfile(blackfile):
        os.system(f'touch {blackfile}')
    logger.debug(f'Starting blacklist concatenation')

    merged = ''
    blackcodes = get_codes(groupdir, None, 'blacklist_codes')

    merged = merge_old_new(blackcodes, savedcodes, index_old=0, reason=reason)
    blacklist = '\n'.join([f'{m}' for m in merged])

    set_codes(groupdir, None, 'blacklist_codes', blacklist)
    print(f'Added {len(merged) - len(blackcodes)} new codes to blacklist')

def upgrade_version(args, logger):
    """Upgrade the version info in the kerchunk file"""

    proj_codes = []
    if not args.repeat_id:
        logger.warning('No repeat ID specified - upgrade whole group using "-r 1"')
        return None
    
    if not args.upgrade:
        logger.warning('New version not specified - should be of the format "krX.X"')
        return None

    projfile = f'{args.groupdir}/proj_codes/{args.repeat_id}.txt'
    if os.path.isfile(projfile):
        with open(projfile) as f:
            proj_codes = [r.strip() for r in f.readlines()]
    else:
        logger.warning(f'Repeat id {args.repeat_id} not found for {args.groupID}')
        return None
        
    # Upgrade each code
    for code in proj_codes:
        try:
            proj_dir   = f'{args.workdir}/in_progress/{args.groupID}/{code}'
            print(f'Upgrading {code} to {args.upgrade}')

            logger.debug(f'Updating detail-cfg for {code}')
            details = get_proj_file(proj_dir, 'detail-cfg.json')
            if details:
                details['version_no'] = args.upgrade
                if args.reason:
                    details['version_reason'] = args.reason
                if args.write:
                    set_proj_file(proj_dir, 'detail-cfg.json', details, logger)

            logger.debug(f'Locating kerchunk file for {code}')
            in_filename = False
            if args.phase == 'validate':
                # Locate kerchunk file with latest version
                files = glob.glob(f'{args.workdir}/in_progress/{args.groupID}/{code}/kerchunk*.json')
            elif args.phase == 'complete':
                # TEMPORARY DIVERSION
                files = glob.glob(f'{args.workdir}/needs_version_updating/{code}*.json')
                in_filename = True
            if files:
                kfile = files[-1]
            else:
                print(f'Skipping for {code} - file not found')
                continue

            logger.debug(f'Found kerchunk file {kfile}')
            with open(kfile) as f:
                refs = json.load(f)
            attrs = json.loads(refs['refs']['.zattrs'])
            # Open with json and upgrade correctly
                
            logger.debug(f'Adding history to {code}')
            now = datetime.now()
            if 'history' in attrs:
                if type(attrs['history']) == str:
                    hist = attrs['history'].split('\n')
                else:
                    hist = attrs['history']

                hist.append(f'Kerchunk file revised on {now.strftime("%D")} - {args.reason}')
                attrs['history'] = '\n'.join(hist)
            else:
                attrs['history'] = f'Kerchunk file revised on {now.strftime("%D")} - {args.reason}'
            old_version = 'kr1.0' # Hardcoded for now
            attrs['kerchunk_revision'] = args.upgrade

            logger.debug(attrs)
            
            refs['refs']['.zattrs'] = json.dumps(attrs)
            if in_filename:
                logger.debug('Replacing revision number in file')
                kfile = kfile.replace(old_version, args.upgrade)
            if args.write:
                if not os.path.isfile(kfile):
                    os.system(f'touch {kfile}')
                with open(kfile,'w') as f:
                    f.write(json.dumps(refs))
                print(f'Written new attributes for {code} to {kfile}')
            else:
                print(f'Skipped writing new attributes for {code} to {kfile}')
        except:
            print('Failed for',code)

def analyse_data(g, workdir):
    """Show some statistics of netcdf and kerchunk data amounts for this particular group"""
    ncf, ker, kus, nus = 0, 0, 0, 0
    complete, scanned = 0, 0
    projset = get_codes(g, workdir, 'proj_codes/main')

    # Add individual error log checking PPC here.
    
    for p in projset:
        proj_dir = f'{workdir}/in_progress/{g}/{p}'
        details = get_proj_file(proj_dir, 'detail-cfg.json')
        if details:
            scanned += 1
            if 'netcdf_data' in details:
                ncf += mem_to_val(details['netcdf_data'])
                ker += mem_to_val(details['kerchunk_data'])
                if os.path.isfile(f'{workdir}/in_progress/{g}/{p}/kerchunk-1a.json.complete'):
                    kus += mem_to_val(details['kerchunk_data'])
                    nus += mem_to_val(details['netcdf_data'])
                    complete += 1
    return ncf, ker, kus, nus, scanned, complete

def summary_data(args, logger):
    """Display summary info in terms of data representation"""
    from pipeline.logs import FalseLogger
    from pipeline.scan import format_float
    if ',' in args.groupID:
        groups = args.groupID.split(',')
    else:
        groups = [args.groupID]

    Tncf, Tker, Tkus, Tnus, Tscan, Tcomp = 0, 0, 0, 0, 0, 0
    for g in groups:
        print()
        print(g)
        ncf, ker, kus, nus, scanned, complete = analyse_data(g, args.workdir)
        print(f'   Datasets          : {len(get_codes(g, args.workdir, "proj_codes/main"))}')
        print(f'    - Unavailable    : {len(get_codes(g, args.workdir, "blacklist_codes"))}')
        print(f'   Data:')
        print(f'    - NetCDF         : {format_float(ncf, FalseLogger())}')
        print(f'    - Kerchunk Estm  : {format_float(ker, FalseLogger())} ({scanned})')
        print(f'    - NetCDF Actual  : {format_float(nus, FalseLogger())}')
        print(f'    - Kerchunk Actual: {format_float(kus, FalseLogger())} ({complete})')
        print()

        Tncf += ncf
        Tker += ker
        Tkus += kus
        Tnus += nus
        Tscan += scanned
        Tcomp += complete

    if len(groups) > 1:
        print(f'Total Across {len(groups)} groups')
        print(f'         NetCDF: {format_float(Tncf, FalseLogger())}')
        print(f'  Kerchunk Estm: {format_float(Tker, FalseLogger())} ({Tscan})')
        print(f'NetCDF Actual  : {format_float(Tnus, FalseLogger())}')
        print(f'Kerchunk Actual: {format_float(Tkus, FalseLogger())} ({Tcomp})')

operations = {
    'progress': progress_check,
    'blacklist': add_to_blacklist,
    'upgrade': upgrade_version,
    'summarise': summary_data,
    'display': show_options,
    'cleanup': cleanup,
}

def assess_main(args):
    """Main assessment function, different tools diverge from here."""

    logger = init_logger(args.verbose, args.mode, 'assessor')

    args.workdir  = get_attribute('WORKDIR', args, 'workdir')

    if ',' in args.error:
        args.error = args.error.split(',')
    else:
        args.error = [args.error]

    if args.groupID == 'A':
        groups = []
        for d in glob.glob(f'{args.workdir}/groups/*'):
            if os.path.isdir(d):
                groups.append(d.split('/')[-1])
        print(groups)
    elif ',' in args.groupID:
        groups = args.groupID.split(',')
    else:
        groups = [args.groupID]
    
    if args.operation in operations:
        for groupID in groups:
            args.groupID = groupID
            args.groupdir = f'{args.workdir}/groups/{args.groupID}'
            operations[args.operation](args, logger)
    else:
        print(f'{args.operation} - Unknown operation, not one of {list(operations.keys())}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('operation',type=str, help=f'Operation to perform - choose from {operations.keys()}.')
    parser.add_argument('groupID',type=str, help='Group identifier code')

    # Blacklist with a given reason
    parser.add_argument('-B','--blacklist', dest='blacklist', action='store_true', help='Use when saving project codes to the blacklist')
    parser.add_argument('-R','--reason', dest='reason', default='No reason given',help='Provide the reason for handling project codes when saving to the blacklist or upgrading')

    # Special options
    parser.add_argument('-s','--show-opts', dest='option', help='Show options for jobids, labels')
    parser.add_argument('-c','--clean-up', dest='cleanup', default=None, help='Clean up group directory of errors/outputs/labels')
    parser.add_argument('-U','--upgrade', dest='upgrade', default=None, help='Upgrade to new version')
    parser.add_argument('-l','--long', dest='long', action='store_true', help='Show long error message (no concatenation past 20 chars.)')
    # Note this will be replaced with upgrader tool at some point

    # Select subgroups and save new repeat groups
    parser.add_argument('-j','--jobid', dest='jobID', default=None, help='Identifier of job to inspect')
    parser.add_argument('-p','--phase', dest='phase', default=None, help='Pipeline phase to inspect')
    parser.add_argument('-r','--repeat_id', dest='repeat_id', default='main', help='Inspect an existing ID for errors')
    parser.add_argument('-n','--new_id', dest='new_id', default=None, help='Create a new repeat ID, specify selection of codes by phase, error etc.')
    parser.add_argument('-N','--numbers', dest='numbers', default=0, help='Show project code numbers for quicker reruns across different errors.')

    # Error inspection
    parser.add_argument('-e','--error', dest='error', default='', help='Inspect error of a specific type')
    parser.add_argument('-E','--examine', dest='examine', action='store_true', help='Examine log outputs individually.')

    # Output to new text files - need both to ensure no accidental writing
    parser.add_argument('-W','--write',  dest='write',  action='count', help='Write outputs to files' )
    parser.add_argument('-O','--overwrite', dest='overwrite', action='count', help='Force overwrite of steps if previously done')

    # Generic Pipeline
    parser.add_argument('-w','--workdir',   dest='workdir',      help='Working directory for pipeline')
    parser.add_argument('-g','--groupdir',  dest='groupdir',     help='Group directory for pipeline')
    parser.add_argument('-v','--verbose', dest='verbose', action='count', default=1, help='Print helpful statements while running')
    parser.add_argument('-m','--mode',        dest='mode', default=None, help='Print or record information (log or std)')

    args = parser.parse_args()

    assess_main(args)
        