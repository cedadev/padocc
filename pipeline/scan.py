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
import math
import json
import numpy as np

from pipeline.logs import *
from pipeline.errors import ExpectTimeoutError, FilecapExceededError

def format_float(value: int, logger):
    """Format byte-value with proper units"""
    logger.debug(f'Formatting value {value} in bytes')
    if value:
        unit_index = -1
        units = ['K','M','G','T','P']
        while value > 1000:
            value = value / 1000
            unit_index += 1
        return f'{value:.2f} {units[unit_index]}B'
    else:
        return None

def safe_format(value: int, fstring: str):
    """Attempt to format a string given some fstring template."""
    try:
        return fstring.format(value=value)
    except:
        return ''

def map_to_kerchunk(args, nfile: str, ctype: str, logger):
    """Perform Kerchunk reading on specific file"""
    logger.info(f'Running Kerchunk reader for {nfile}')
    from pipeline.compute.serial_process import Converter

    quickConvert = Converter(logger, bypass_errs=args.bypass)

    kwargs = {}
    supported_extensions = ['ncf3','hdf5','tif']

    logger.debug(f'Attempting conversion for 1 {ctype} extension')
    t1 = datetime.now()
    tdict = quickConvert.convert_to_zarr(nfile, ctype, **kwargs)
    t_len = (datetime.now()-t1).total_seconds()
    ext_index = 0
    while not tdict and ext_index < len(supported_extensions)-1:
        # Try the other ones
        extension = supported_extensions[ext_index]
        logger.debug(f'Attempting conversion for {extension} extension')
        if extension != ctype:
            t1 = datetime.now()
            tdict = quickConvert.convert_to_zarr(nfile, extension, **kwargs)
            t_len = (datetime.now()-t1).total_seconds()
        ext_index += 1
    
    if not tdict:
        logger.error('Scanning failed for all drivers, file type is not Kerchunkable')
        return None, None, None
    else:
        logger.info(f'Scan successful with {ctype} driver')
        return tdict['refs'], ctype, t_len

def get_internals(args, testfile: str, ctype: str, logger):
    """Map to kerchunk data and perform calculations on test netcdf file."""
    refs, ctype, time = map_to_kerchunk(args, testfile, ctype, logger)
    if not refs:
        return None, None, None
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
    return np.sum(sizes), chunks, sorted(list(vars.keys())), ctype, time

def eval_sizes(files: list):
    """Get a list of file sizes on disk from a list of filepaths"""
    return [os.stat(files[count]).st_size for count in range(len(files))]

def get_seconds(time_allowed: str):
    """Convert time in MM:SS to seconds"""
    if not time_allowed:
        return 10000000000
    mins, secs = time_allowed.split(':')
    return int(secs) + 60*int(mins)

def format_seconds(seconds: int):
    """Convert time in seconds to MM:SS"""
    mins = int(seconds/60) + 1
    if mins < 10:
        mins = f'0{mins}'
    return f'{mins}:00'

def perform_safe_calculations(std_vars: list, cpf: list, volms: list, files: list, times: list, logger):
    """Perform all calculations safely to mitigate errors that come through during data collation."""
    kchunk_const = 167 # Bytes per Kerchunk ref (standard/typical)
    if std_vars:
        num_vars = len(std_vars)
    else:
        num_vars = None
    if not len(cpf) == 0:
        avg_cpf = sum(cpf)/len(cpf)
    else:
        logger.warning('CPF set as none, len cpf is zero')
        avg_cpf = None
    if not len(volms) == 0:
        avg_vol = sum(volms)/len(volms)
    else:
        logger.warning('Volume set as none, len volumes is zero')
        avg_vol = None
    if avg_cpf:
        avg_chunk = avg_vol/avg_cpf
    else:
        avg_chunk = None
        logger.warning('Average chunks is none since CPF is none')
    if num_vars and avg_cpf:
        spatial_res = 180*math.sqrt(2*num_vars/avg_cpf)
    else:
        spatial_res = None

    if files and avg_vol:
        data_represented = avg_vol*len(files)
        num_files = len(files)
    else:
        data_represented = None
        num_files = None

    if files and avg_cpf:
        total_chunks = avg_cpf * len(files)
    else:
        total_chunks = None

    if avg_chunk:
        addition = kchunk_const*100/avg_chunk
    else:
        addition = None

    if files and len(times) > 0:
        estm_time = int(np.mean(times)*len(files))
    else:
        estm_time = 0

    return avg_cpf, num_vars, avg_chunk, spatial_res, data_represented, num_files, total_chunks, addition, estm_time

def scan_dataset(args, files: list, proj_dir: str, proj_code: str, logger):
    """Main process handler for scanning phase"""
    logger.debug(f'Assessment for {proj_code}')

    # Set up conditions, skip for small file count < 5
    escape, is_varwarn, is_skipwarn = False, False, False
    cpf, volms, times = [],[],[]
    trial_files = 5
    if len(files) < 5:
        details = {'skipped':True}
        if args.dryrun:
            logger.info(f'DRYRUN: Skip writing to {proj_dir}/detail-cfg.json')
        else:
            with open(f'{proj_dir}/detail-cfg.json','w') as f:
                f.write(json.dumps(details))
            logger.info(f'Skipped scanning - {proj_code}/detail-cfg.json blank file created')
        return None
    else:
        logger.info(f'Identified {len(files)} files for scanning')
    
    # Perform scans for sample (max 5) files
    count    = 0
    std_vars = None
    ctypes   = []
    filecap = min(100,len(files))
    while not escape and len(cpf) < trial_files:
        logger.info(f'Attempting scan for file {count+1} (min 5, max 100)')
        # Add random file selector here

        scanfile = files[count]
        if '.' in scanfile:
            extension = f'.{scanfile.split(".")[-1]}'
        else:
            extension = '.nc'

        try:
            # Measure time and ensure job will not overrun if it can be prevented.
            volume, chunks_per_file, vars, ctype, time = get_internals(args, scanfile, extension, logger)
            if count == 0 and time > get_seconds(args.time_allowed)/trial_files:
                raise ExpectTimeoutError(required=format_seconds(time*5), current=args.time_allowed)

            cpf.append(chunks_per_file)
            volms.append(volume)
            ctypes.append(ctype)
            times.append(time)

            if not std_vars:
                std_vars = vars
            if vars != std_vars:
                logger.warning('Variables differ between files')
                is_varwarn = True
            logger.info(f'Data saved for file {count+1}')
        except ExpectTimeoutError as err:
            raise err
        except Exception as e:
            if args.bypass:
                logger.warning(f'Skipped file {count} - {e}')
                is_skipwarn = True
            else:
                raise e
        count += 1
        if count >= filecap:
            escape = True
    if escape:
        raise FilecapExceededError(filecap)

    logger.info('Scan complete, compiling outputs')
    (avg_cpf, num_vars, avg_chunk, 
     spatial_res, data_represented, num_files, 
     total_chunks, addition, estm_time) = perform_safe_calculations(std_vars, cpf, volms, files, times, logger)
    
    details = {
        'data_represented' : format_float(data_represented, logger), 
        'num_files'        : num_files,
        'chunks_per_file'  : safe_format(avg_cpf,'{value:.1f}'),
        'total_chunks'     : safe_format(total_chunks,'{value:.2f}'),
        'estm_chunksize'   : format_float(avg_chunk,logger),
        'estm_spatial_res' : safe_format(spatial_res,'{value:.2f}') + ' deg',
        'estm_time'        : format_seconds(estm_time),
        'variable_count'   : num_vars,
        'addition'         : safe_format(addition,'{value:.3f}') + ' %',
        'var_err'          : is_varwarn,
        'file_err'         : is_skipwarn,
        'type'             : 'JSON'
    }

    if escape:
        details['scan_status'] = 'FAILED'

    if len(set(ctypes)) == 1:
        details['driver'] = ctypes[0]
    
    c2m = 1.67e-4 # Memory for each chunk in kerchunk in MB

    if avg_cpf and files:
        if avg_cpf * len(files) * c2m > 500e6:
            details['type'] = 'parq'
    else:
        details['type'] = 'N/A'

    if args.dryrun:
        logger.info(f'DRYRUN: Skip writing to {proj_dir}/detail-cfg.json')
    else:
        with open(f'{proj_dir}/detail-cfg.json','w') as f:
            # Replace with dumping dictionary
            f.write(json.dumps(details))
        logger.info(f'Written output file {proj_code}/detail-cfg.json')

def scan_config(args):
    """Configure scanning and access main section"""

    logger = init_logger(args.verbose, args.mode, 'scan')
    logger.debug(f'Setting up scanning process')

    cfg_file = f'{args.proj_dir}/base-cfg.json'
    if os.path.isfile(cfg_file):
        with open(cfg_file) as f:
            cfg = json.load(f)
    else:
        logger.error(f'cfg file missing or not provided - {cfg_file}')
        return None

    proj_code = cfg['proj_code']
    workdir   = cfg['workdir']
    proj_dir  = cfg['proj_dir']
    logger.debug(f'Extracted attributes: {proj_code}, {workdir}, {proj_dir}')

    filelist = f'{proj_dir}/allfiles.txt'
    
    if not os.path.isfile(filelist):
        logger.error(f'No filelist detected - {filelist}')
        return None

    with open(filelist) as f:
        files = [r.strip() for r in f.readlines()]

    if not os.path.isfile(f'{proj_dir}/detail-cfg.json') or args.forceful:
        scan_dataset(args, files, proj_dir, proj_code, logger)
    else:
        logger.warning(f'Skipped scanning {proj_code} - detailed config already exists')

if __name__ == '__main__':
    print('Kerchunk Pipeline Config Scanner - run using master scripts')