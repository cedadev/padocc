__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import json
import math
import os
import re
import glob
from typing import Any, Union, Callable

import fsspec
import numpy as np
import xarray as xr
from datetime import datetime

from .errors import MissingVariableError

times = {
    'scan'    :'10:00', # No prediction possible prior to scanning
    'compute' :'60:00',
    'validate':'30:00' # From CMIP experiments - no reliable prediction mechanism possible
}

phases = [
    'init',
    'scan',
    'compute',
    'validate',
    'catalog'
]

# Which files acceptable to pull from Moles Tags file.
source_opts = [
    '.nc'
]

# Which operations are parallelisable.
parallel_modes = [
    'scan',
    'compute',
    'validate'
]

BASE_CFG = {
    'proj_code':None,
    'pattern':None,
    'updates':None,
    'removals':None,
    'version_no':'1.0',
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
        'cloud_type':'kerchunk',
        'file_type':'json' # Default values
    },
    'last_run': (None, None),
}

DETAIL_CFG = {
    'source_data': None,
    'cloud_data': None,
    'scanned_with': None,
    'num_files': None,
    'timings': {},
    'chunk_info':None,
    'kwargs': {},
}

file_configs = {
    'base_cfg':BASE_CFG,
    'detail_cfg':DETAIL_CFG
}

FILE_DEFAULT = {
    'kerchunk':'json',
    'zarr':None,
}

def list_groups(workdir: str, func: Callable = print):
    """
    List groups in the existing working directory
    """

    if not os.path.isdir(workdir):
        func('[ERROR] Workdir does not exist')
        return False
    
    if workdir.endswith('/'):
        workdir = workdir[:-1]
    topdir = workdir.split('/')[-1]
    
    func(f'Groups in {topdir}')

    fileset = glob.glob(f'{workdir}/groups/*/proj_codes/main.txt')
    for f in fileset:
        groupID = f.split('/')[-3]

        with open(f) as g:
            length = len(g.readlines())

        msg = f' > {groupID}: {length} '
        if length == 0:
            msg += '(empty)'
        func(msg)

def timestamp():
    return datetime.strftime(datetime.now(),'%d/%m/%y %H:%M:%S')

def make_tuple(item: Any) -> tuple:
    """
    Make any object into a tuple.

    :param item: (Any) Insert item into a tuple if not already one.
    """
    if not isinstance(item, tuple):
        return (item,)
    else:
        return item

def deformat_float(item: str) -> str:
    """
    Format byte-value with proper units.

    :param item:    (str) Byte value to format into a float.
    """
    units = ['','K','M','G','T','P']
    value, suffix = item.split(' ')

    ord = units.index(suffix)*1000
    return float(value)*ord

def format_float(value: float) -> str:
    """
    Format byte-value with proper units.

    :param value:   (float) Number of bytes (avg), format to a string
        representation.
    """

    if value is not None:
        unit_index = 0
        units = ['','K','M','G','T','P']
        while value > 1000:
            value = value / 1000
            unit_index += 1
        return f'{value:.2f} {units[unit_index]}B'
    else:
        return None

def get_attribute(env: str, args, value: str) -> str:
    """
    Assemble environment variable or take from passed argument. Find
    value of variable from Environment or ParseArgs object, or reports failure.

    :param env:     (str) Name of environment variable.

    :param args:    (obj) Set of command line arguments supplied by argparse.
    
    :param var:     (str) Name of argparse parameter to check.

    :returns: Value of either environment variable or argparse value.
    """
    if getattr(args, value) is None:
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
        string: Any, 
        length: int, 
        concat: bool = False, 
        shorten: bool = False
    ) -> str:
    """
    Simple function to format a string to a correct length.

    :param string:  (str) Message to format into a string of exact length.

    :param length:  (int) Accepted length of string.

    :param concat:  (bool) If True, will add elipses for overrunning strings.

    :param shorten: (bool) If True will allow shorter messages, otherwise will
        fill with whitespace.
    """
    string = str(string)

    if len(string) < length and shorten:
        return string

    string = str(string)
    if len(string) >= length and concat:
        string = string[:length-3] + '...'
    else:
        while len(string) < length:
            string += ' '

    return string[:length]

def print_fmt_str(
        string: str,
        help_length: int = 40,
        concat: bool = True,
        shorten: bool = False
        ):
    """
    Replacement for callable function in ``help`` methods.
    
    This print-replacement adds whitespace between functions
    and their help descriptions.

    :param string:  (str) Message to format into a string of exact length.

    :param help_length: (int) Accepted length of string.

    :param concat:  (bool) If True, will add elipses for overrunning strings.

    :param shorten: (bool) If True will allow shorter messages, otherwise will
        fill with whitespace.
    """
    
    if '-' not in string:
        print(string)
        return
    
    string, message = string.split('-')

    print(format_str(string, help_length, concat, shorten), end='-')
    print(message)
  
def format_tuple(tup: tuple[list[int]]) -> str:
    """
    Transform tuple to string representation

    :param tup: (tuple) Tuple object to be rendered to string.
    """

    try:
        return f'({",".join([str(t[0]) for t in tup])})'
    except IndexError:
        return str(tup)

def mem_to_val(value: str) -> float:
    """
    Convert a value in Bytes to an integer number of bytes.

    :param value:   (str) Convert number of bytes (XB) to float.
    """

    suffixes = {
        'KB': 1000,
        'MB': 1000000,
        'GB': 1000000000,
        'TB': 1000000000000,
        'PB': 1000000000000000}
    suff = suffixes[value.split(' ')[1]]
    return float(value.split(' ')[0]) * suff

def extract_file(input_file: str) -> list:
    """
    Extract content from a padocc-external file.

    Use filehandlers for files within the pipeline.

    :param input_file: (str) Pipeline-external file.
    """
    with open(input_file) as f:
        content = [r.strip() for r in f.readlines()]
    return content

def extract_json(input_file: str) -> list:
    """
    Extract content from a padocc-external file.

    Use filehandlers for files within the pipeline.

    :param input_file: (str) Pipeline-external file.
    """
    with open(input_file) as f:
        content = json.load(f)
    return content

def find_closest(num: int, closest: float) -> int:
    """
    Find divisions for a dimension for rechunking purposes.
    
    Used in Zarr rechunking and conversion.

    :param num:     (int) Typically the size of the dimension

    :param closest: (float) Find a divisor closest to this value.
    """

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

def apply_substitutions(
        subkey: str, 
        subs: Union[dict,None] = None, 
        content: Union[list,None] = None):
    """
    Apply substitutions to all elements in the provided content list.

    :param subkey:  (str) The key to extract from the provided set of substitutions.
        This is in the case were substitutions are specified for different levels
        of input files.

    :param subs:    (dict) The substitutions applied to the content.

    :param content: (list) The set of filepaths to apply substitutions.
    """
    if not subs:
        return content, ""

    if subkey not in subs:
        return content, f"Subkey {subkey} is not valid for substitutions"
    
    content = '\n'.join(content)
    for f, r in subs[subkey].items():
        content = content.replace(f,r)

    return content.split('\n') , ""

class BypassSwitch:
    """
    Switch container class for multiple error switches.
    
    Class to represent all bypass switches throughout the pipeline.
    Requires a switch string which is used to enable/disable specific pipeline 
    switches stored in this class.
    """

    def __init__(self, switch: str = 'D'):
        """
        Initialisation of switch
        
        :param switch:  (str) Raw input from CLI or otherwise, referring to specific
            switch parameters
        """
        
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
  