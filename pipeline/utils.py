__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import xarray as xr
import json
import fsspec
import logging
import math
import numpy as np
import re

from pipeline.errors import MissingVariableError, MissingKerchunkError, ChunkDataError, \
                            KerchunkDecodeError
from pipeline.logs import FalseLogger

times = {
    'scan'    :'10:00', # No prediction possible prior to scanning
    'compute' :'60:00',
    'validate':'30:00' # From CMIP experiments - no reliable prediction mechanism possible
}

class BypassSwitch:
    """Class to represent all bypass switches throughout the pipeline.
    Requires a switch string which is used to enable/disable specific pipeline 
    switches stored in this class.
    """

    def __init__(self, switch='DBSCLR'):
        if switch.startswith('+'):
            switch = 'DBSCLR' + switch[1:]
        self.switch = switch
        if type(switch) == str:
            switch = list(switch)
        
        self.skip_driver   = ('D' in switch)
        self.skip_boxfail  = ('B' in switch)
        self.skip_softfail = ('S' in switch)
        self.skip_data_sum = ('C' in switch)
        self.skip_xkshape  = ('X' in switch)
        self.skip_report   = ('R' in switch)
        self.skip_scan     = ('F' in switch) # Fasttrack
        self.skip_links    = ('L' in switch)

    def __str__(self):
        """Return the switch string (letters representing switches)"""
        return self.switch
    
    def help(self):
        return str("""
Bypass switch options: \n
  "D" - * Skip driver failures - Pipeline tries different options for NetCDF (default).
      -   Only need to turn this skip off if all drivers fail (KerchunkDriverFatalError).
  "B" - * Skip Box compute errors.
  "S" - * Skip Soft fails (NaN-only boxes in validation) (default).
  "C" - * Skip calculation (data sum) errors (time array typically cannot be summed) (default).
  "X" -   Skip initial shape errors, by attempting XKShape tolerance method (special case.)
  "R" -   Skip reporting to status_log which becomes visible with assessor. Reporting is skipped
          by default in single_run.py but overridden when using group_run.py so any serial
          testing does not by default report the error experienced to the status log for that project.
  "F" -   Skip scanning (fasttrack) and go straight to compute. Required if running compute before scan
          is attempted.
  "L" -   Skip adding links in compute (download links) - this will be required on ingest.
""")
  
def open_kerchunk(kfile: str, logger, isparq=False, retry=False, attempt=1, **kwargs) -> xr.Dataset:
    """
    Open kerchunk file from JSON/parquet formats

    :param kfile:   (str) Path to a kerchunk file (or https link if using a remote file)

    :param logger:  (obj) Logging object for info/debug/error messages.

    :param isparq:  (bool) Switch for using Parquet or JSON Format

    :param remote_protocol: (str) 'file' for local filepaths, 'http' for remote links.
    
    :returns: An xarray virtual dataset constructed from the Kerchunk file
    """
    if isparq:
        logger.debug('Opening Kerchunk Parquet store')
        from fsspec.implementations.reference import ReferenceFileSystem
        fs = ReferenceFileSystem(
            kfile, 
            remote_protocol='file', 
            target_protocol="file", 
            lazy=True)
        return xr.open_dataset(
            fs.get_mapper(), 
            engine="zarr",
            backend_kwargs={"consolidated": False, "decode_times": False}
        )
    else:
        logger.info(f'Attempting to open Kerchunk JSON file - attempt {attempt}')
        try:
            mapper  = fsspec.get_mapper('reference://',fo=kfile, target_options={"compression":None}, **kwargs)
        except json.JSONDecodeError as err:
            logger.error(f"Kerchunk file {kfile} appears to be empty")
            raise MissingKerchunkError
        # Need a safe repeat here
        ds = None
        attempts = 0
        while attempts < 3 and not ds:
            attempts += 1
            try:
                ds = xr.open_zarr(mapper, consolidated=False, decode_times=True)
            except OverflowError:
                ds = None
            except KeyError as err:
                if re.match('.*https.*',str(err)) and not retry:
                    # RemoteProtocol is not https - retry with correct protocol
                    logger.warning('Found KeyError "https" on opening the Kerchunk file - retrying with local filepaths.')
                    return open_kerchunk(kfile, logger, isparq=isparq, retry=True)
                else:
                    raise err
            except Exception as err:
                if 'decode' in str(err):
                    raise KerchunkDecodeError
                raise err #MissingKerchunkError(message=f'Failed to open kerchunk file {kfile}')
        if not ds:
            raise ChunkDataError
        logger.debug('Successfully opened Kerchunk with virtual xarray ds')
        return ds

def get_attribute(env: str, args, var: str) -> str:
    """
    Assemble environment variable or take from passed argument. Find
    value of variable from Environment or ParseArgs object, or reports failure.

    :param env:     (str) Name of environment variable.

    :param args:    (obj) Set of command line arguments supplied by argparse.
    
    :param var:     (str) Name of argparse parameter to check.

    :returns: Value of either environment variable or argparse value.
    """
    try:
        if getattr(args, var):
            return getattr(args, var)
    except AttributeError:
        pass
    if os.getenv(env):
        return os.getenv(env)
    else:
        print(var)
        raise MissingVariableError(type=var)

def format_str(string: str, length: int, concat=False) -> str:
    """
    Simple function to format a string to a correct length.
    """
    string = str(string)
    if len(string) >= length and concat:
        string = string[:length-3] + '...'
    else:
        while len(string) < length:
            string += ' '
    return string[:length]
  
def mem_to_val(value: str) -> float:
    """
    Convert a value in Bytes to an integer number of bytes
    """

    suffixes = {
        'KB': 1000,
        'MB': 1000000,
        'GB': 1000000000,
        'TB': 1000000000000,
        'PB': 1000000000000000}
    suff = suffixes[value.split(' ')[1]]
    return float(value.split(' ')[0]) * suff

def get_blacklist(group: str, workdir: str) -> list:
    """
    Returns a list of the project codes given a filename (repeat id)

    :param group:       (str) Name of current group or path to group directory
                        (groupdir) in which case workdir can be left as None.

    :param workdir:     (str) Path to working directory or None. If this is None,
                        group value will be assumed as the groupdir path.

    :returns: A list of codes if the file is found, an empty list otherwise.
    """
    if workdir:
        codefile = f'{workdir}/groups/{group}/blacklist_codes.txt'
    else:
        codefile = f'{group}/blacklist_codes.txt'
    if os.path.isfile(codefile):
        with open(codefile) as f:
            contents = [r.strip().split(',') for r in f.readlines()]
            if type(contents[0]) != list:
                contents = [contents]
            return contents
    else:
        return []

def get_codes(group: str, workdir: str , filename: str, extension='.txt') -> list:
    """
    Returns a list of the project codes given a filename (repeat id)

    :param group:       (str) Name of current group or path to group directory
                        (groupdir) in which case workdir can be left as None.

    :param workdir:     (str) Path to working directory or None. If this is None,
                        group value will be assumed as the groupdir path.

    :param filename:    (str) Name of text file to access within group (or path
                        within the groupdir to the text file

    :param extension:   (str) For the specific case of non-text-files.

    :returns: A list of codes if the file is found, an empty list otherwise.
    """
    if workdir:
        codefile = f'{workdir}/groups/{group}/{filename}{extension}'
    else:
        codefile = f'{group}/{filename}.txt'
    if os.path.isfile(codefile):
        with open(codefile) as f:
            return [r.strip() for r in f.readlines()]
    else:
        return []
    
def set_codes(group: str, workdir: str, filename: str, contents, extension='.txt', overwrite=0) -> None:
    """
    Returns a list of the project codes given a filename (repeat id)

    :param group:       (str) Name of current group or path to group directory
                        (groupdir) in which case workdir can be left as None.

    :param workdir:     (str) Path to working directory or None. If this is None,
                        group value will be assumed as the groupdir path.

    :param filename:    (str) Name of text file to access within group (or path
                        within the groupdir to the text file
    
    :param contents:    (str) Combined contents to write to the file.

    :param extension:   (str) For the specific case of non-text-files.

    :param overwrite:   (str) Specifier for open() built-in python method, completely
                        overwrite the file contents or append to existing values.

    :returns: None
    """
    codefile = f'{group}/{filename}.txt'
    if workdir:
        codefile = f'{workdir}/groups/{group}/{filename}{extension}'

    ow = 'w'
    if overwrite == 1:
        ow = 'w+'

    with open(codefile, ow) as f:
        f.write(contents)
    
def set_last_run(proj_dir, phase, time, logger=FalseLogger()) -> None:
    detail = get_proj_file(proj_dir, 'detail-cfg.json')
    if detail:
        detail['last_run'] = (phase, time)
        set_proj_file(proj_dir, 'detail-cfg.json', detail, logger)

def get_last_run(proj_dir) -> str:
    detail = get_proj_file(proj_dir, 'detail-cfg.json')
    if detail:
        return detail['last_run']

def get_proj_file(proj_dir: str, proj_file: str) -> dict:
    """
    Returns the contents of a project file within a project code directory.

    :param proj_dir:    (str) The project code directory path.

    :param proj_file:   (str) Name of a file to access within the project directory.

    :returns: A dictionary of the contents of a json file or None if there are problems.
    """
    if not proj_file:
        projfile = proj_dir
    else:
        projfile = f'{proj_dir}/{proj_file}'
    if os.path.isfile(projfile):
        try:
            with open(projfile) as f:
                contents = json.load(f)
            return contents
        except:
            with open(projfile) as f:
                print(f.readlines())
            return None
    else:
        return None
    
def set_proj_file(proj_dir: str, proj_file: str, contents: dict, logger: logging.Logger) -> None:
    """
    Overwrite the contents of a project file within a project code directory.

    :param proj_dir:    (str) The project code directory path.

    :param proj_file:   (str) Name of a file to access within the project directory.

    :param contents:    (dict) Dictionary to write into json format config file within
                        the project directory.

    :returns: A dictionary of the contents of a json file or None if there are problems.
    """
    projfile = f'{proj_dir}/{proj_file}'
    if not os.path.isfile(projfile):
        os.system(f'touch {projfile}')
    try:
        with open(projfile,'w') as f:
            f.write(json.dumps(contents))
        logger.debug(f'{proj_file} updated')
    except Exception as err:
        logger.error(f'{proj_file} unable to update - {err}')
    
def get_proj_dir(proj_code: str, workdir: str, groupID: str) -> str:
    """
    Simple function to assemble the project directory, depends on groupID
    May be redundant in the future if a 'serial' directory is added.
    """
    if groupID:
        return f'{workdir}/in_progress/{groupID}/{proj_code}'
    else:
        return f'{workdir}/in_progress/{proj_code}'

def find_zarrays(refs: dict) -> dict:
    """Quick way of extracting all the zarray components of a ref set."""
    zarrays = {}
    for r in refs['refs'].keys():
        if '.zarray' in r:
            zarrays[r] = refs['refs'][r]
    return zarrays

def find_divisor(num, preferences={'range':{'max':10000, 'min':2000}}):

    # Using numpy for this is MUCH SLOWER!
    divs = [x for x in range(1, int(math.sqrt(num))+1) if num % x == 0]
    opps = [int(num/x) for x in divs] # get divisors > sqrt(n) by division instead
    divisors = np.array(list(set(divs + opps)))

    divset = []
    range_allowed = preferences['range']['max'] - preferences['range']['min']
    iterations = 0
    while len(divset) == 0:
        divset = divisors[np.logical_and(
            divisors < preferences['range']['max'] + range_allowed*iterations,
            divisors > preferences['range']['min']/(iterations+1)
        )]
        iterations += 1

    divisor = int(np.median(divset))
    return divisor

def find_closest(num, closest):

    divs = [x for x in range(1, int(math.sqrt(num))+1) if num % x == 0]
    opps = [int(num/x) for x in divs] # get divisors > sqrt(n) by division instead
    divisors = np.array(list(set(divs + opps)))

    min_diff = 99999999999
    closest_div = None
    for d in divisors:
        if abs(d-closest) < min_diff:
            min_diff = abs(d-closest)
            closest_div = d
    return closest_div