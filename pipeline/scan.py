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

from pipeline.logs import init_logger
from pipeline.utils import get_attribute, BypassSwitch
from pipeline.errors import *
from pipeline.compute.serial_process import Converter, Indexer

def format_float(value: int, logger):
    """Format byte-value with proper units"""
    logger.debug(f'Formatting value {value} in bytes')
    if value:
        unit_index = 0
        units = ['','K','M','G','T','P']
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

def trial_kerchunk(args, nfile: str, ctype: str, logger):
    """Perform Kerchunk reading on specific file"""
    logger.info(f'Running Kerchunk reader for {nfile}')

    quickConvert = Converter(logger, bypass_driver=args.bypass.skip_driver)

    kwargs = {}
    supported_extensions = ['ncf3','hdf5','tif']

    usetype = ctype

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
            usetype = extension
        ext_index += 1
    
    if not tdict:
        logger.error('Scanning failed for all drivers, file type is not Kerchunkable')
        raise KerchunkDriverFatalError
    else:
        logger.info(f'Scan successful with {usetype} driver')
        return tdict, usetype, t_len
    
def load_from_previous(args, cache_id, logger):
    cachefile = f'{args.proj_dir}/cache/{cache_id}.json'
    if os.path.isfile(cachefile):
        logger.info(f"Found existing cached file {cache_id}.json")
        with open(cachefile) as f:
            refs = json.load(f)
        return refs
    else:
        return None
        
def perform_scan(args, testfile: str, ctype: str, logger, savecache=True, cache_id=None, thorough=False):
    """Map to kerchunk data and perform calculations on test netcdf file."""
    if cache_id and not thorough:
        refs = load_from_previous(args, cache_id, logger)
        time = 0
        if not refs:
            refs, ctype, time = trial_kerchunk(args, testfile, ctype, logger)
    else:
        refs, ctype, time = trial_kerchunk(args, testfile, ctype, logger)
    if not refs:
        return None, None, None, None, None

    logger.debug('Starting Analysis of references')

    # Perform summations, extract chunk attributes
    sizes = []
    vars = {}
    chunks = 0
    kdict = refs['refs']
    for chunkkey in kdict.keys():
        if len(kdict[chunkkey]) >= 2:
            try:
                sizes.append(int(kdict[chunkkey][2]))
                chunks += 1
            except ValueError:
                pass
        if '/.zarray' in chunkkey:
            var = chunkkey.split('/')[0]
            chunksize = 0
            if var not in vars:
                if type(kdict[chunkkey]) == str:
                    chunksize = json.loads(kdict[chunkkey])['chunks']
                else:
                    chunksize = dict(kdict[chunkkey])['chunks']
                vars[var] = chunksize

    # Save refs individually within cache.
    if savecache:
        cachedir = f'{args.proj_dir}/cache'
        if not os.path.isdir(cachedir):
            os.makedirs(cachedir)
        with open(f'{cachedir}/{cache_id}.json','w') as f:
            f.write(json.dumps(refs))

    return np.sum(sizes), chunks, vars, ctype, time

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

def write_skip(proj_dir, proj_code, logger):
    details = {'skipped':True}
    with open(f'{proj_dir}/detail-cfg.json','w') as f:
        f.write(json.dumps(details))
    logger.info(f'Skipped scanning - {proj_code}/detail-cfg.json blank file created')

def scan_dataset(args, files: list, logger):
    """Main process handler for scanning phase"""
    proj_code = args.proj_code
    proj_dir  = args.proj_dir
    detailfile = f'{proj_dir}/detail-cfg.json'
    logger.debug(f'Assessment for {proj_code}')

    # Set up conditions, skip for small file count < 5
    escape, is_varwarn, is_skipwarn = False, False, False
    cpf, volms, times = [],[],[]
    trial_files = 5

    if len(files) < 5:
        write_skip(proj_dir, proj_code, logger)
        return None
    else:
        logger.info(f'Identified {len(files)} files for scanning')
    
    # Perform scans for sample (max 5) files
    count    = 0
    std_vars   = None
    std_chunks = None
    ctypes   = []

    scanfile = files[0]
    if '.' in scanfile:
        ctype = f'.{scanfile.split(".")[-1]}'
    else:
        ctype = 'ncf3'

    filecap = min(100,len(files))
    while not escape and len(cpf) < trial_files:
        logger.info(f'Attempting scan for file {count+1} (min 5, max 100)')
        # Add random file selector here
        scanfile = files[count]
        try:
            # Measure time and ensure job will not overrun if it can be prevented.
            volume, chunks_per_file, varchunks, ctype, time = perform_scan(args, scanfile, ctype, logger, 
                                                                           savecache=True, cache_id=str(count),
                                                                           thorough=args.quality)
            vars = sorted(list(varchunks.keys()))
            if not std_vars:
                std_vars = vars
            if vars != std_vars:
                logger.warning(f'Variables differ between files - {vars} vs {std_vars}')
                is_varwarn = True

            if not std_chunks:
                std_chunks = varchunks
            for var in std_vars:
                if std_chunks[var] != varchunks[var]:
                    raise ConcatFatalError(var=var, chunk1=std_chunks[var], chunk2=varchunks[var])

            if count == 0 and time > get_seconds(args.time_allowed)/trial_files:
                raise ExpectTimeoutError(required=format_seconds(time*5), current=args.time_allowed)

            cpf.append(chunks_per_file)
            volms.append(volume)
            ctypes.append(ctype)
            times.append(time)

            logger.info(f'Data recorded for file {count+1}')
        except ExpectTimeoutError as err:
            raise err
        except ConcatFatalError as err:
            raise err
        except Exception as err:
            raise err
        count += 1
        if count >= filecap:
            escape = True
    if escape:
        raise FilecapExceededError(filecap)

    logger.info('Scan complete, compiling outputs')
    (avg_cpf, num_vars, avg_chunk, 
     spatial_res, data_represented, num_files, 
     total_chunks, addition, estm_time) = perform_safe_calculations(std_vars, cpf, volms, files, times, logger)
    
    c2m = 167 # Memory for each chunk in kerchunk in B

    details = {
        'netcdf_data'      : format_float(data_represented, logger), 
        'kerchunk_data'    : format_float(avg_cpf * num_files * c2m, logger), 
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
    logger.info('Performing concatenation attempt with minimal files')
    try:
        assemble_trial_concatenation(args, ctype, logger)
    except Exception as err:
        logger.error('Error in concatenating files')
        raise err

def assemble_trial_concatenation(args, ctype, logger):

    cfg_file    = f'{args.proj_dir}/base-cfg.json'
    detail_file = f'{args.proj_dir}/detail-cfg.json'

    idx_trial = Indexer(args.proj_code, cfg_file=cfg_file, detail_file=detail_file, 
                    workdir=args.workdir, issave_meta=True, thorough=False, forceful=args.forceful,
                    verb=args.verbose, mode=args.mode,
                    bypass=args.bypass, groupID=args.groupID, limiter=2, ctype=ctype)
    
    idx_trial.create_refs()
    with open(detail_file,'w') as f:
        f.write(json.dumps(idx_trial.collect_details()))
    logger.debug('Collected new details into detail-cfg.json')


def scan_config(args, fh=None, logid=None, **kwargs):
    """Configure scanning and access main section"""

    logger = init_logger(args.verbose, args.mode, 'scan',fh=fh, logid=logid)
    logger.debug(f'Setting up scanning process')

    cfg_file = f'{args.proj_dir}/base-cfg.json'
    if os.path.isfile(cfg_file):
        with open(cfg_file) as f:
            cfg = json.load(f)
    else:
        os.system(f'ls {args.proj_dir}')
        logger.error(f'cfg file missing or not provided - {cfg_file}')
        return None
    
    args.workdir  = get_attribute('WORKDIR', args, 'workdir')
    args.groupdir = get_attribute('GROUPDIR', args, 'groupdir')

    if args.groupID:
        args.proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{args.proj_code}'
    else:
        args.proj_dir = f'{args.workdir}/in_progress/{args.proj_code}'

    logger.debug(f"""Extracted attributes: {args.proj_code}, 
                                           {args.workdir}, 
                                           {args.proj_dir}
    """)

    filelist = f'{args.proj_dir}/allfiles.txt'
    
    if not os.path.isfile(filelist):
        logger.error(f'No filelist detected - {filelist}')
        return None

    with open(filelist) as f:
        files = [r.strip() for r in f.readlines()]

    if not os.path.isfile(f'{args.proj_dir}/detail-cfg.json') or args.forceful:
        scan_dataset(args, files, logger)
    else:
        logger.warning(f'Skipped scanning {args.proj_code} - detailed config already exists')

if __name__ == '__main__':
    print('Kerchunk Pipeline Config Scanner - run using master scripts')