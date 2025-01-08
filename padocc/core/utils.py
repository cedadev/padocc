__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
import xarray as xr
import json
import fsspec
import math
import numpy as np
import re

from padocc.core.errors import (
    MissingVariableError, 
    MissingKerchunkError, 
    ChunkDataError,
    KerchunkDecodeError
)

times = {
    'scan'    :'10:00', # No prediction possible prior to scanning
    'compute' :'60:00',
    'validate':'30:00' # From CMIP experiments - no reliable prediction mechanism possible
}

phases = [
    'scan',
    'compute',
    'validate',
    'catalog'
]

BASE_CFG = {
    'proj_code':None,
    'pattern':None,
    'updates':None,
    'removals':None,
    'version_no':'1.1',
    'data_properties':{
        'aggregated_dims':'Unknown',
        'pure_dims': 'Unknown',
        'coord_dims': 'Unknown',
        'virtual_dims': 'Unknown',
        'aggregated_vars': 'Unknown',
        'scalar_vars':'Unknown',
        'identical_vars':'Unknown'
    },
    'override':{
        'cloud_type':None,
        'file_type':None
    },
    'last_run': (None, None),
}

DETAIL_CFG = {
    'source_data': None,
    'cloud_data': None,
    'scanned_with': None,
    'num_files': None,
    'timings': None,
    'chunk_info':None,
    'kwargs': None,
}

file_configs = {
    'base_cfg':BASE_CFG,
    'detail_cfg':DETAIL_CFG
}

class BypassSwitch:
    """Class to represent all bypass switches throughout the pipeline.
    Requires a switch string which is used to enable/disable specific pipeline 
    switches stored in this class.
    """

    def __init__(self, switch='D'):
        if switch.startswith('+'):
            switch = 'D' + switch[1:]
        self.switch = switch
        if isinstance(switch, str):
            switch = list(switch)
        
        self.skip_driver   = ('D' in switch) # Keep
        self.skip_scan     = ('F' in switch) # Fasttrack
        self.skip_links    = ('L' in switch)
        self.skip_subsets  = ('S' in switch)

    def __str__(self):
        """Return the switch string (letters representing switches)"""
        return self.switch
    
    def help(self):
        return str("""
Bypass switch options: \n
  "D" - * Skip driver failures - Pipeline tries different options for NetCDF (default).
      -   Only need to turn this skip off if all drivers fail (KerchunkDriverFatalError).
  "F" -   Skip scanning (fasttrack) and go straight to compute. Required if running compute before scan
          is attempted.
  "L" -   Skip adding links in compute (download links) - this will be required on ingest.
  "S" -   Skip errors when running a subset within a group. Record the error then move onto the next dataset.
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

def get_attribute(env: str, value: str) -> str:
    """
    Assemble environment variable or take from passed argument. Find
    value of variable from Environment or ParseArgs object, or reports failure.

    :param env:     (str) Name of environment variable.

    :param args:    (obj) Set of command line arguments supplied by argparse.
    
    :param var:     (str) Name of argparse parameter to check.

    :returns: Value of either environment variable or argparse value.
    """
    if value is None:
        if not os.getenv(env):
            raise MissingVariableError(vtype=env)
        else:
            return os.getenv(env)
    else:
        if os.getenv(env):
            print(
                'Overriding environment workdir with user-defined value:'
                f'Env : "{os.getenv(env)}"'
                f'User: "{value}')
            value = os.getenv(env)
        return value

def format_str(
        string: str, 
        length: int, 
        concat: bool = False, 
        shorten: bool = False
    ) -> str:
    """
    Simple function to format a string to a correct length.
    """
    if len(string) < length and shorten:
        return string

    string = str(string)
    if len(string) >= length and concat:
        string = string[:length-3] + '...'
    else:
        while len(string) < length:
            string += ' '

    return string[:length]
  
def format_tuple(tup: tuple[list[int]]) -> str:

    try:
        return f'({",".join([str(t[0]) for t in tup])})'
    except IndexError:
        return str(tup)

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

def extract_file(input_file, prefix=None):
    with open(input_file) as f:
        content = [r.strip() for r in f.readlines()]
    return content

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

def apply_substitutions(subkey: str, subs: dict = None, content: list = None):
    if not subs:
        return content, ""

    if subkey not in subs:
        return content, f"Subkey {subkey} is not valid for substitutions"
    
    content = '\n'.join(content)
    for f, r in subs[subkey].items():
        content = content.replace(f,r)

    return content.split('\n') , ""