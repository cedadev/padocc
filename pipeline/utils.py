__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import xarray as xr
import json
import fsspec

from pipeline.errors import MissingVariableError, MissingKerchunkError, ChunkDataError

def open_kerchunk(kfile: str, logger, isparq=False, remote_protocol='file'):
    """Open kerchunk file from JSON/parquet formats"""
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
        logger.debug('Opening Kerchunk JSON file')
        try:
            mapper  = fsspec.get_mapper('reference://',fo=kfile, target_options={"compression":None}, remote_protocol=remote_protocol)
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
            except Exception as err:
                raise err #MissingKerchunkError(message=f'Failed to open kerchunk file {kfile}')
        if not ds:
            raise ChunkDataError
        logger.debug('Successfully opened Kerchunk with virtual xarray ds')
        return ds

def get_attribute(env: str, args, var: str):
    """Assemble environment variable or take from passed argument.
    
    Finds value of variable from Environment or ParseArgs object, or reports failure
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

def format_str(string: str, length: int, concat=False):
    """Simple function to format a string to a correct length"""
    string = str(string)
    if len(string) >= length and concat:
        string = string[:length-3] + '...'
    else:
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
    if workdir:
        codefile = f'{workdir}/groups/{group}/{filename}.txt'
    else:
        codefile = f'{group}/{filename}.txt'
    if os.path.isfile(codefile):
        with open(codefile) as f:
            return [r.strip() for r in f.readlines()]
    else:
        return []
    
def set_codes(group, workdir, filename, contents, overwrite=0):
    codefile = f'{group}/{filename}.txt'
    if workdir:
        codefile = f'{workdir}/groups/{group}/{filename}.txt'

    ow = 'w'
    if overwrite == 1:
        ow = 'w+'

    with open(codefile, ow) as f:
        f.write(contents)
    
def get_proj_file(proj_dir, proj_file):
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
    
def set_proj_file(proj_dir, proj_file, contents, logger):
    projfile = f'{proj_dir}/{proj_file}'
    if not os.path.isfile(projfile):
        os.system(f'touch {projfile}')
    try:
        with open(projfile,'w') as f:
            f.write(json.dumps(contents))
        logger.debug(f'{proj_file} updated')
    except Exception as err:
        logger.error(f'{proj_file} unable to update - {err}')
    
def get_proj_dir(proj_code, workdir, groupID):
    if groupID:
        return f'{workdir}/in_progress/{groupID}/{proj_code}'
    else:
        return f'{workdir}/in_progress/{proj_code}'

