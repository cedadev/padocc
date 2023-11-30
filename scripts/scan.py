## Tool for scanning a netcdf file or set of netcdf files for kerchunkability

# Determine total number of netcdf chunks in first file
# Determine number of netcdf files

# Calculate total number of chunks and output

__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr
import os, sys
from datetime import datetime
import glob
import math
import json
import logging

import numpy as np

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

def format_float(value, logger):
    """Format byte-value with proper units"""
    logger.debug(f'Formatting value {value} in bytes')

    unit_index = -1
    units = ['K','M','G','T','P']
    while value > 1000:
        value = value / 1000
        unit_index += 1
    return f'{value:.2f} {units[unit_index]}B'

def map_to_kerchunk(nfile, logger):
    """Perform Kerchunk reading on specific file"""
    logger.info(f'Running Kerchunk reader for {nfile}')

    try:
        logger.debug(f'Using HDF5 reader for {nfile}')
        tdict = SingleHdf5ToZarr(nfile, inline_threshold=1).translate()
        return tdict['refs']
    except OSError:
        logger.debug(f'Switching to NetCDF3 reader for {nfile}')
        try:
            tdict = NetCDF3ToZarr(nfile, inline_threshold=1).translate()
            return tdict['refs']
        except Exception as e:
            logger.warn(f'Kerchunk mapping failed for {nfile}')
            return False

def get_internals(testfile, logger):
    """Map to kerchunk data and perform calculations on test netcdf file."""
    refs = map_to_kerchunk(testfile, logger)
    if not refs:
        return None
    logger.info(f'Starting summation process for {testfile}')

    # Perform summations, extract chunk attributes
    sizes = []
    vars = {}
    chunks = 0
    for chunkkey in refs.keys():
        if len(refs[chunkkey]) >= 2:
            try:
                sizes.append(int(refs[chunkkey][2]))
                chunks += 1
                vars[chunkkey.split('/')[0]] = 1
            except ValueError:
                pass
    return np.sum(sizes), chunks, sorted(list(vars.keys()))

def make_filelist(pattern, proj_dir, logger):
    """Create list of files associated with this project"""
    logger.debug(f'Making list of files for project {proj_dir.split("/")[-1]}')

    if os.path.isdir(proj_dir):
        os.system(f'ls {pattern} > {proj_dir}/allfiles.txt')
    else:
        logger.error(f'Project Directory not located - {proj_dir}')

def eval_sizes(files):
    """Get a list of file sizes on disk from a list of filepaths"""
    return [os.stat(files[count]).st_size for count in range(len(files))]

def get_seconds(time_allowed):
    """Convert time in MM:SS to seconds"""
    if not time_allowed:
        return 10000000000
    mins, secs = time_allowed.split(':')
    return int(secs) + 60*int(mins)

def main(files, proj_dir, proj_code, logger, time_allowed=None):
    """Main process handler for scanning phase"""
    logger.debug(f'Assessment for {proj_code}')

    # Set up conditions, skip for small file count < 5
    success, escape, is_varwarn, is_skipwarn = False, False, False, False
    cpf, volms = [],[]
    trial_files = 5
    if len(files) < 5:
        details = {'skipped':True}
        with open(f'{proj_dir}/detail-cfg.json','w') as f:
            f.write(json.dumps(details))
        print(f'Skipped scanning - {proj_code}/detail-cfg.json blank file created')
        return None
    
    # Perform scans for sample (max 5) files
    count = 0
    vs = None
    while not escape and len(cpf) < trial_files:
        logger.debug(f'Attempting file {count+1} (min 5, max 100)')
        # Add random file selector here
        try:
            # Measure time and ensure job will not overrun if it can be prevented.
            t1 = datetime.now()
            volume, chunks_per_file, vars = get_internals(files[count])
            t2 = (datetime.now() - t1).total_seconds()
            if count == 0 and t2 > get_seconds(time_allowed)/trial_files:
                logger.error(f'Time estimate exceeds allowed time for job - {t2}')
                escape = True

            cpf.append(chunks_per_file)
            volms.append(volume)

            if not vs:
                vs = vars
            if vars != vs:
                logger.warn('Variables differ between files')
                is_varwarn = True
            logger.info(f'Data saved for file {count+1}')
        except Exception as e:
            logger.warn(f'Skipped file {count} - {e}')
            is_skipwarn = True
        if count >= 100:
            escape = True
        count += 1
    if count > 100:
        print('Filecount Exceeded: No valid files in first 100 tried')
    
    avg_cpf = sum(cpf)/len(cpf)
    avg_vol = sum(volms)/len(volms)
    avg_chunk = avg_vol/avg_cpf
    kchunk_const = 167 # Bytes per Kerchunk ref (standard/typical)

    spatial_res = 180*math.sqrt(2*len(vs)/avg_cpf)
    details = {
        'data_represented' : format_float(avg_vol*len(files), 2), 
        'num_files'        : str(len(files)),
        'chunks_per_file'  : f'{avg_cpf:.1f}',
        'total_chunks'     : f'{(avg_cpf * len(files)):.2f}',
        'estm_chunksize'   : format_float(avg_chunk,2),
        'estm_spatial_res' : f'{spatial_res:.2f} deg',
        'variable_count'   : len(vs),
        'addition'         : f'{kchunk_const*100/avg_chunk:.3f} %',
        'var_err'          : is_varwarn,
        'file_err'         : is_skipwarn,
        'type'             : 'JSON'
    }

    if escape:
        details['scan_status'] = 'FAILED'
    
    c2m = 1.67e-4 # Memory for each chunk in kerchunk in MB

    if avg_cpf * len(files) * c2m > 500e6:
        details['type':'parq']

    with open(f'{proj_dir}/detail-cfg.json','w') as f:
        # Replace with dumping dictionary
        f.write(json.dumps(details))
    vprint(f'Written config info to {proj_code}/detail-cfg.json')

def setup_main(args):

    logger = init_logger(args.verbose, args.mode, 'scan')

    cfg_file = f'{args.proj_dir}/base-cfg.json'
    if os.path.isfile(cfg_file):
        with open(cfg_file) as f:
            cfg = json.load(f)
    else:
        print(f'Error: cfg file missing or not provided - {cfg_file}')
        return None

    proj_code = cfg['proj_code']
    workdir   = cfg['workdir']
    proj_dir  = cfg['proj_dir']
    print(proj_code, workdir, proj_dir)

    try:
        pattern   = cfg['pattern']
    except KeyError:
        pattern = None


    filelist = f'{proj_dir}/allfiles.txt'
    if pattern:
        make_filelist(pattern, proj_dir)
    
    if not os.path.isfile(filelist):
        print('Error: No filelist detected - ',filelist)
        return None

    with open(filelist) as f:
        files = [r.strip() for r in f.readlines()]
        numfiles = len(files)
    if not os.path.isfile(f'{proj_dir}/detail-cfg.json') or args.forceful:
        main(files, proj_dir, proj_code, args.time_allowed)
    else:
        print('Skipped scanning - detailed config already exists')

# Assume deal with the first file in a directory

def get_proj_code(groupdir, pid):
    with open(f'{groupdir}/proj_codes.txt') as f:
        proj_code = f.readlines()[int(pid)].strip()
    return proj_code


def scan_files(args):

    print('Initialising Scan', args.proj_code)

    if args.groupID:
        if not args.groupdir:
            args.groupdir = f'{args.workdir}/groups/{args.groupID}'
        args.proj_code = get_proj_code(args.groupdir, args.proj_code)
        args.proj_dir = f'{args.workdir}/in_progress/{args.proj_code}'

    setup_main(args)