__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import xarray as xr
import json
from datetime import datetime
import sys
import fsspec
import random
import numpy as np
import glob
import logging
import math

from pipeline.errors import *
from pipeline.logs import init_logger

## 1. Array Selection Tools

def find_dimensions(dimlen: int, divisions: int):
    """Determine index of slice end position given length of dimension and fraction to assess"""
    # Round down then add 1
    slicemax = int(dimlen/divisions)+1
    return slicemax

def get_vslice(shape: list, dtypes: list, lengths: list, divisions: list, logger):
    """Assemble dataset slice given the shape of the array and dimensions involved"""

    vslice = []
    for x, dim in enumerate(shape):
        if np.issubdtype(dtypes[x], np.datetime64):
            vslice.append(slice(0,find_dimensions(lengths[x],divisions)))
        elif dim == 1:
            vslice.append(slice(0,1))
        else:
            vslice.append(slice(0,find_dimensions(dim,divisions)))
    logger.debug(f'Slice {vslice}')
    return vslice

## 2. File Selection Tools

def get_netcdf_list(proj_dir: str, logger, thorough=False):
    """Open document containing paths to all NetCDF files, make selections"""
    with open(f'{proj_dir}/allfiles.txt') as f:
        xfiles = [r.strip() for r in f.readlines()]
    logger.debug(f'Found {len(xfiles)} files in {proj_dir}/allfiles.txt')

    # Open full set or a subset of the files for testing
    if thorough:
        numfiles = len(xfiles)+1
        logger.info(f'Selecting all {numfiles-1} files')
    else:
        numfiles = int(len(xfiles)/1000)
        if numfiles < 3:
            numfiles = 3
        logger.info(f'Selecting a subset of {numfiles} files')

    if numfiles > len(xfiles):
        numfiles = len(xfiles)
        indexes = [i for i in range(len(xfiles))]
    else:
        indexes = []
        for f in range(numfiles):
            testindex = random.randint(0,numfiles-1)
            while testindex in indexes:
                testindex = random.randint(0,numfiles-1)
            indexes.append(testindex)

    logger.debug(f'Filtered fileset to a list of {len(indexes)} files')

    return indexes, xfiles

def pick_index(nfiles: list, indexes: list):
    """Pick index of new netcdf file randomly, try 100 times"""
    index = random.randint(0,nfiles)
    count = 0
    while index in indexes and count < 100:
        index = random.randint(0,nfiles)
        count += 1
    indexes.append(index)
    return indexes

def locate_kerchunk(args, logger, get_str=False):
    """Gets the name of the latest kerchunk file for this project code"""
    files = os.listdir(args.proj_dir) # Get filename only
    kfiles = []

    for f in files:
        if 'complete' in f and not args.forceful:
            logger.error('File already exists and no override is set')
            raise NoOverwriteError
        if 'kerchunk' in f and 'complete' not in f:
            kfiles.append(f)
    if kfiles == []:
        logger.error(f'No Kerchunk file located at {args.proj_dir} - exiting')
        raise MissingKerchunkError
    
    # Which kerchunk file from set of options
    kf = sorted(kfiles)[0]
    logger.info(f'Selected {kf} from {len(kfiles)} available')
    kfile = os.path.join(args.proj_dir, kf)
    if get_str:
        return kfile
    else:
        return open_kerchunk(kfile, logger)

def open_kerchunk(kfile: str, logger, isparq=False):
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
        mapper  = fsspec.get_mapper('reference://',fo=kfile, target_options={"compression":None})
        # Need a safe repeat here
        ds = None
        attempts = 0
        while attempts < 3 and not ds:
            attempts += 1
            try:
                ds = xr.open_zarr(mapper, consolidated=False, decode_times=True)
            except OverflowError:
                ds = None  
        if not ds:
            raise ChunkDataError
        return ds

def open_netcdfs(args, logger, thorough=False):
    """Returns a single xarray object with one timestep:
     - Select a single file and a single timestep from that file
     - Verify that a single timestep can be selected (Yes: return this xarray object, No: select all files and select a single timestep from that)
     - In all cases, returns a list of xarray objects.
    """
    logger.debug('Performing temporal selections')
    indexes, xfiles = get_netcdf_list(args.proj_dir, logger, thorough=thorough)
    xobjs = []
    many = len(indexes)
    if not thorough:
        for one, i in enumerate(indexes):

            # Memory Size Check
            logger.debug(f'Checking memory size of expected netcdf file for index {i} ({one+1}/{many})')
            if os.path.getsize(xfiles[i]) > 4e9 and not args.forceful: # 4GB file
                logger.error('Memory Exception - ensure you have 12GB or more dedicated to this task')
                raise MemoryError('Projected memory requirement too high - run with forceful flag to bypass', verbose=args.verbose)
        xobjs.append(xr.open_dataset(xfiles[i]))
    else:
        xobjs = [xr.open_mfdataset(xfiles)]

    if len(xobjs) == 0:
        logger.error('No valid timestep objects identified')
        raise NoValidTimeSlicesError(message='Kerchunk', verbose=args.verbose)
    return xobjs, indexes, len(xfiles)

## 3. Validation Testing

def match_timestamp(kobject, xobject, logger):
    """Match timestamp of xarray object to kerchunk object
     - Returns temporally matching kerchunk and xarray objects"""
    
    if hasattr(xobject,'time'):
        # Select timestamp 0 from multi-timestamped NetCDF - after shape testing
        if xobject.time.size > 1:
            timestamp = xobject.time[0]
        else:
            timestamp = xobject.time

        logger.debug(f'Kerchunk object total time stamps: {kobject.time.size}')
        try:
            ksel = kobject.sel(time=timestamp)
            xsel = xobject.sel(time=timestamp)
            assert ksel.time.size == 1 and xsel.time.size == 1
            logger.debug('Kerchunk timestamp selection was successful')
            return ksel, xsel
        except Exception as err:
            raise err
    else:
        logger.debug('Skipped timestamp selection as xobject has no time')
        return kobject, xobject

def compare_data(vname: str, netbox, kerchunk_box, logger, bypass=False):
    """Compare a NetCDF-derived ND array to a Kerchunk-derived one
     - Takes a netbox array of n-dimensions and an equally sized kerchunk_box array
     - Tests for elementwise equality within selection.
     - If possible, tests max/mean/min calculations for the selection to ensure cached values are the same.

     - Expect TypeErrors from summations which are bypassed.
     - Other errors will exit the run.
    """
    logger.debug('Starting xk comparison')

    try: # Tolerance 0.1% of mean value for xarray set
        tolerance = np.abs(np.nanmean(kerchunk_box))/1000
    except TypeError: # Type cannot be summed so skip all summations
        tolerance = None

    testpass = True
    if not np.array_equal(netbox, kerchunk_box):
        logger.warn(f'Failed equality check for {vname}')
        print(netbox, kerchunk_box)
        testpass = False
    try:
        if np.abs(np.nanmax(kerchunk_box) - np.nanmax(netbox)) > tolerance:
            logger.warn(f'Failed maximum comparison for {vname}')
            logger.debug('K ' + str(np.nanmax(kerchunk_box)) + ' N ' + str(np.nanmax(netbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warn(f'Max comparison skipped for non-summable values in {vname}')
        else:
            raise err
    try:
        if np.abs(np.nanmin(kerchunk_box) - np.nanmin(netbox)) > tolerance:
            logger.warn(f'Failed minimum comparison for {vname}')
            logger.debug('K ' + str(np.nanmin(kerchunk_box)) + ' N ' + str(np.nanmin(netbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warn(f'Min comparison skipped for non-summable values in {vname}')
        else:
            raise err
    try:
        if np.abs(np.nanmean(kerchunk_box) - np.nanmean(netbox)) > tolerance:
            logger.warn(f'Failed mean comparison for {vname}')
            logger.debug('K ' + str(np.nanmean(kerchunk_box)) + ' N ' + str(np.nanmean(netbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warn(f'Mean comparison skipped for non-summable values in {vname}')
        else:
            raise err

def validate_shapes(xobj, kobj, step: int, nfiles: list, xv: str, logger):
    """Ensure shapes are equivalent across Kerchunk/NetCDF per variable
     - Accounts for the number of files opened vs how many files in total."""
    xshape = list(xobj[xv].shape)
    kshape = list(kobj[xv].shape)

    if 'time' in xobj[xv].dims:
        try:
            xshape[0] *= nfiles
        except TypeError:
            logger.warning(f'{xv} - {nfiles}*{xshape[0]} failed to assign')
        except:
            pass

    logger.debug(f'{xv} : Comparing shapes {xshape} and {kshape} - {step}')
    
    if xshape != kshape:
        logger.warning(f'Kerchunk/NetCDF mismatch for variable {xv} with shapes - K {kshape} vs N {xshape}')
        raise ShapeMismatchError(var=xv, first=kshape, second=xshape)

def validate_selection(args, xvariable, kvariable, vname: str, divs: int, currentdiv: int, logger):
    """Validate this data selection in xvariable/kvariable objects
      - Recursive function tests a growing selection of data until one is found with real data
      - Repeats with exponentially increasing box size (divisions of all data dimensions)
      - Will halt at 1 division which equates to testing all data
    """

    # Determine number based on 
    repeat = int(math.log2(divs) - math.log2(currentdiv))

    logger.debug(f'Attempt {repeat} - {currentdiv} divs for {vname}')

    vslice = []
    if divs > 1:
        shape = xvariable.shape
        logger.debug(f'Detected shape {shape} for {vname}')
        dtypes  = [xvariable[xvariable.dims[x]].dtype for x in range(len(xvariable.shape))]
        lengths = [len(xvariable[xvariable.dims[x]])  for x in range(len(xvariable.shape))]
        vslice = get_vslice(shape, dtypes, lengths, divs, logger)

        xbox = xvariable[tuple(vslice)]
        kbox = kvariable[tuple(vslice)]
    else:
        xbox = xvariable
        kbox = kvariable

    # Zero shape means no point running divisions - just perform full check
    if shape == {} and vslice == []:
        logger.debug(f'Skipping to full selection (1 division) for {vname}')
        currentdiv = 1

    try:
        kb = np.array(kbox)
        isnan = np.all(kb!=kb)
    except Exception as err:
        if args.bypass:
            logger.warning(f'{err} - check versions')
            isnan = True
        else:
            raise err
        
    if kbox.size >= 1 and not isnan:
        # Evaluate kerchunk vs xarray and stop here
        logger.debug(f'Found non-NaN values with box-size: {int(kbox.size)}')
        compare_data(vname, xbox, kbox, logger, bypass=args.bypass)
    else:
        logger.debug(f'Attempt {repeat} - slice is Null')
        if currentdiv >= 2:
            # Recursive search for increasing size (decreasing divisions)
            validate_selection(args, xvariable, kvariable, vname, divs, int(currentdiv/2), logger)
        else:
            print(np.array(xvariable))
            logger.warn(f'Failed to find non-NaN slice (tried: {int(math.log2(divs))}, var: {vname})')
            if not args.bypass:
                raise SoftfailBypassError

def validate_data(args, xobj, kobj, xv: str, step: int, logger):
    """Run growing selection test for specified variable from xarray and kerchunk datasets"""
    logger.info(f'{xv} : Starting growbox data tests for {step}')

    kvariable, xvariable = match_timestamp(xobj[xv], kobj[xv], logger)

    # Attempt 128 divisions within selection - 128, 64, 32, 16, 8, 4, 2, 1
    return validate_selection(args, xvariable, kvariable, xv, 128, 128, logger)

def validate_timestep(args, xobj, kobj, step: int, nfiles: list, logger):
    """Run all tests for a single file which may or may not equate to 1 timestep"""

    # Run Variable and Shape validation
    xvars = set(xobj.variables)
    kvars = set(kobj.variables)
    if xvars&kvars != xvars: # Overlap of sets - all xvars should be in kvars
        missing = (xvars^kvars)&xvars 
        raise VariableMismatchError(missing=missing)
    else:
        logger.info(f'Passed Variable tests')
        print()
        for xv in xvars:
            validate_shapes(xobj, kobj, step, nfiles, xv, logger)
            logger.info(f'{xv} : Passed Shape test')
        logger.info(f'Passed all Shape tests')
        print()
        for xv in xvars:
            validate_data(args, xobj, kobj, xv, step, logger)
            logger.info(f'{xv} : Passed Data test')

def run_successful(args, logger):
    """Move kerchunk-1a.json file to complete directory with proper name"""
    # in_progress/<groupID>/<proj_code>/kerchunk_1a.json
    # complete/<groupID>/<proj_code.json

    kfile = locate_kerchunk(args, logger, get_str=True)

    if args.groupID:
        complete_dir = f'{args.workdir}/complete/{args.groupID}'
    else:
        complete_dir = f'{args.workdir}/complete/single_runs'

    if not os.path.isdir(complete_dir):
        os.makedirs(complete_dir)

    # Open config file to get correct version

    newfile = f'{complete_dir}/{args.proj_code}_kr1.0.json'
    if args.dryrun:
        logger.info(f'DRYRUN: mv {kfile} {newfile}')
    else:
        os.system(f'mv {kfile} {newfile}')
        os.system(f'touch {kfile}.complete')

def run_backtrack():
    """Not currently implemented"""
    raise NotImplementedError

def validate_dataset(args):
    """Perform validation steps for specific dataset defined here
     - Determine the number of NetCDF files in total
     - Run validation for a minimum subset of those files
    """
    logger = init_logger(args.verbose, args.mode,'validate')
    logger.info(f'Starting tests for {args.proj_code}')

    xobjs, indexes, nfiles = open_netcdfs(args, logger, thorough=args.quality)

    if len(xobjs) == 0:
        raise NoValidTimeSlicesError(message='Xarray/NetCDF')

    ## Open kerchunk file
    kobj = locate_kerchunk(args, logger)
    if not kobj:
        raise MissingKerchunkError

    ## Set up loop variables
    fullset   = False
    total     = len(indexes)

    for step, index in enumerate(indexes):
        xobj = xobjs[step]
        logger.info(f'Running tests for selected file: {index} ({step+1}/{total})')

        try:
            validate_timestep(args, xobj, kobj, step+1, nfiles, logger)
        except ShapeMismatchError:
            fullset = True
            break

    if fullset:
        xobjs, indexes, nfiles = open_netcdfs(args, logger, thorough=True)
        try:
            validate_timestep(args, xobjs[0], kobj, 0, 1, logger)
        except ShapeMismatchError:
            raise TrueShapeValidationError
        except Exception as err:
            raise err
    
    
    logger.info('All tests passed successfully')
    print()
    run_successful(args, logger)

if __name__ == "__main__":
    print('Validation Process for Kerchunk Pipeline - run with single_run.py')