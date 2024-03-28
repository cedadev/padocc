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
from pipeline.utils import get_attribute, BypassSwitch, get_codes, get_proj_dir, get_proj_file, set_codes
from pipeline.errors import *
from pipeline.compute import KerchunkConverter, KerchunkDSProcessor

def format_float(value: int, logger) -> str | None:
    """
    Format byte-value with proper units
    """
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

def safe_format(value: int, fstring: str) -> str:
    """Attempt to format a string given some fstring template.
    - Handles issues by returning '', usually when value is None initially."""
    try:
        return fstring.format(value=value)
    except:
        return ''
        
def summarise_json(args, count: int, ctype: str, logger) -> tuple:
    """
    Open previously written JSON cached files and perform analysis.
    """
    refs = get_proj_file(args.proj_dir, f'cache/{count}.json')
    if not refs:
        return None, None, None, None

    logger.debug(f'Starting Analysis of references for {count}')

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

    return np.sum(sizes), chunks, vars, ctype

def eval_sizes(files: list) -> list:
    """Get a list of file sizes on disk from a list of filepaths"""
    return [os.stat(files[count]).st_size for count in range(len(files))]

def get_seconds(time_allowed: str) -> int:
    """Convert time in MM:SS to seconds"""
    if not time_allowed:
        return 10000000000
    mins, secs = time_allowed.split(':')
    return int(secs) + 60*int(mins)

def format_seconds(seconds: int) -> str:
    """Convert time in seconds to MM:SS"""
    mins = int(seconds/60) + 1
    if mins < 10:
        mins = f'0{mins}'
    return f'{mins}:00'

def perform_safe_calculations(std_vars: list, cpf: list, volms: list, files: list, logger) -> tuple:
    """
    Perform all calculations safely to mitigate errors that arise during data collation.

    :param std_vars:        (list) A list of the variables collected, which should be the same across
                            all input files.

    :param cpf:             (list) The chunks per file recorded for each input file.

    :param volms:           (list) The total data size recorded for each input file.

    :param files:           (list) A list of the paths to each file.

    :param logger:          (obj) Logging object for info/debug/error messages.

    :returns: Average values of: chunks per file (cpf), number of variables (num_vars), chunk size (avg_chunk),
    spatial resolution of each chunk assuming 2:1 ratio lat/lon (spatial_res), totals of NetCDF and Kerchunk estimate
    data amounts, number of files, total number of chunks and the addition percentage.
    """
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
        netcdf_data = avg_vol*len(files)
        num_files = len(files)
    else:
        netcdf_data = None
        num_files = None

    if files and avg_cpf:
        total_chunks = avg_cpf * len(files)
    else:
        total_chunks = None

    if avg_chunk:
        addition = kchunk_const*100/avg_chunk
    else:
        addition = None

    if avg_cpf and num_files:
        kerchunk_data = avg_cpf * num_files * kchunk_const
    else:
        kerchunk_data = None

    return avg_cpf, num_vars, avg_chunk, spatial_res, netcdf_data, kerchunk_data, num_files, total_chunks, addition

def write_skip(proj_dir: str, proj_code: str, logger) -> None:
    """
    Quick function to write a 'skipped' detail file.
    """
    details = {'skipped':True}
    with open(f'{proj_dir}/detail-cfg.json','w') as f:
        f.write(json.dumps(details))
    logger.info(f'Skipped scanning - {proj_code}/detail-cfg.json blank file created')

def scan_dataset(args, files: list, logger) -> None:
    """Main process handler for scanning phase"""
    proj_code = args.proj_code
    proj_dir  = args.proj_dir

    detailfile = f'{proj_dir}/detail-cfg.json'
    cfgfile    = f'{proj_dir}/base-cfg.json'
    logger.debug(f'Assessment for {proj_code}')

    # Set up conditions, skip for small file count < 5
    escape, is_varwarn, is_skipwarn = False, False, False
    cpf, volms = [],[]

    if len(files) < 3:
        write_skip(proj_dir, proj_code, logger)
        return None
    
    # Perform scans for sample (max 5) files
    std_vars   = None
    std_chunks = None
    ctypes   = []
    ctype    = None

    # Create all files in mini-kerchunk set here. Then try an assessment.
    limiter = int(len(files)/20)
    limiter = max(2, limiter)
    limiter = min(100, limiter)

    logger.info(f'Determined {limiter} files to scan')

    mini_ds = KerchunkDSProcessor(
        args.proj_code,
        cfg_file=cfgfile, detail_file=detailfile, workdir=args.workdir, 
        thorough=True, forceful=True, # Always run from scratch forcefully to get best time estimates.
        version_no='trial-', verb=args.verbose, logid='0',
        groupID=args.groupID, limiter=limiter)
    
    mini_ds.create_refs()

    logger.info(f'Summarising scan results for {limiter} files')
    for count in range(limiter):
        try:
            volume, chunks_per_file, varchunks, ctype = summarise_json(args, count, ctype, logger)
            vars = sorted(list(varchunks.keys()))

            # Keeping the below options although may be redundant as have already processed the files
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

            cpf.append(chunks_per_file)
            volms.append(volume)
            ctypes.append(ctype)

            logger.info(f'Data recorded for file {count+1}')
        except ExpectTimeoutError as err:
            raise err
        except ConcatFatalError as err:
            raise err
        except Exception as err:
            raise err

    logger.info('Summary complete, compiling outputs')
    (avg_cpf, num_vars, avg_chunk, 
     spatial_res, netcdf_data, kerchunk_data, num_files, 
     total_chunks, addition) = perform_safe_calculations(std_vars, cpf, volms, files, logger)
    
    c2m = 167 # Memory for each chunk in kerchunk in B

    details = {
        'netcdf_data'      : format_float(netcdf_data, logger), 
        'kerchunk_data'    : format_float(kerchunk_data, logger), 
        'num_files'        : num_files,
        'chunks_per_file'  : safe_format(avg_cpf,'{value:.1f}'),
        'total_chunks'     : safe_format(total_chunks,'{value:.2f}'),
        'estm_chunksize'   : format_float(avg_chunk,logger),
        'estm_spatial_res' : safe_format(spatial_res,'{value:.2f}') + ' deg',
        'timings'        : {
            'convert_estm'   : mini_ds.convert_time,
            'concat_estm'    : mini_ds.concat_time,
            'validate_estm'  : mini_ds.validate_time,
            'convert_actual' : None,
            'concat_actual'  : None,
            'validate_actual': None,
        },
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

def scan_config(args, fh=None, logid=None, **kwargs) -> None:
    """Configure scanning and access main section"""

    logger = init_logger(args.verbose, args.mode, 'scan',fh=fh, logid=logid)
    logger.debug(f'Setting up scanning process')
    
    args.workdir  = get_attribute('WORKDIR', args, 'workdir')
    args.groupdir = get_attribute('GROUPDIR', args, 'groupdir')

    args.proj_dir = get_proj_dir(args.proj_code, args.workdir, args.groupID)

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