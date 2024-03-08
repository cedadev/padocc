__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os

from pipeline.errors import MissingVariableError

def get_attribute(env: str, args, var: str):
    """Assemble environment variable or take from passed argument.
    
    Finds value of variable from Environment or ParseArgs object, or reports failure
    """
    if getattr(args, var):
        return getattr(args, var)
    elif os.getenv(env):
        return os.getenv(env)
    else:
        print(var)
        raise MissingVariableError(type=var)
    
def format_str(string: str, length: int):
    """Simple function to format a string to a correct length"""
    string = str(string)
    while len(string) < length:
        string += ' '
    return string[:length]

class BypassSwitch:
    def __init__(self, switch='DBSCMR'):
        if switch.startswith('+'):
            switch = 'DBSCMR' + switch[1:]
        self.switch = switch
        if type(switch) == str:
            switch = list(switch)
        
        self.skip_driver   = ('D' in switch)
        self.skip_boxfail  = ('B' in switch)
        self.skip_softfail = ('S' in switch)
        self.skip_data_sum = ('C' in switch)
        self.skip_xkshape  = ('X' in switch)
        self.skip_report   = ('R' in switch)

        # Removed scanfile and memory skips

    def __str__(self):
        return self.switch
    
    def help(self):
        return str("""
Bypass switch options: \n
  "F" - * Skip individual file scanning errors.
  "D" - * Skip driver failures - Pipeline tries different options for NetCDF (default).
      -   Only need to turn this skip off if all drivers fail (KerchunkFatalDriverError).
  "B" -   Skip Box compute errors.
  "S" - * Skip Soft fails (NaN-only boxes in validation) (default).
  "C" - * Skip calculation (data sum) errors (time array typically cannot be summed) (default).
  "M" -   Skip memory checks (validate/compute aborts if utilisation estimate exceeds cap).
""")
    
def mem_to_val(value):
    """Convert a value in Bytes to an integer number of bytes"""
    suffixes = {
        'KB': 1000,
        'MB': 1000000,
        'GB': 1000000000,
        'TB': 1000000000000,
        'PB': 1000000000000000}
    suff = suffixes[value.split(' ')[1]]
    return float(value.split(' ')[0]) * suff

def get_codes(group, workdir, filename):
    """Returns a list of the project codes given a filename (repeat id)"""
    with open(f'{workdir}/groups/{group}/{filename}.txt') as f:
        return [r.strip() for r in f.readlines()]