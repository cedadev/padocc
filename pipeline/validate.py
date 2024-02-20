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
from pipeline.logs import init_logger, SUFFIXES, SUFFIX_LIST, BypassSwitch

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

def get_concat_dims(xfiles, detailfile=None):
    concat_dims = {'time':0}
    if os.path.isfile(detailfile):
        with open(detailfile) as f:
            details = json.load(f)
    # Initialise concat dims
    if 'concat_dims' in details:
        concat_dims[details['concat_dims']] = 0

    for xf in xfiles:
        # Open netcdf in lowest memory intensive way possible.
        ds = xr.open_dataset(xf)
        for dim in concat_dims.keys():
            concat_dims[dim] += ds[dim].shape[0]
    return concat_dims

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
        logger.info(f'Selecting a subset of {numfiles}/{len(xfiles)} files')
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

    check_complete = False

    for f in files:
        if 'complete' in f:
            check_complete = True
        elif 'kerchunk' in f and 'complete' not in f:
            kfiles.append(f)
        else:
            pass

    if len(kfiles) > 0:
        # Which kerchunk file from set of options
        kf = sorted(kfiles)[0]
        logger.info(f'Selected {kf} from {len(kfiles)} available')
        kfile = os.path.join(args.proj_dir, kf)
        if get_str:
            return kfile, False
        else:
            return open_kerchunk(kfile, logger, remote_protocol='https'), False
    elif check_complete:
        if not args.forceful:
            logger.error('File already exists and no override is set')
            raise NoOverwriteError
        else:
            logger.info('Locating complete Kerchunk file')
            if args.groupID:
                complete_path = f'{args.workdir}/complete/{args.groupID}/{args.proj_code}*.json'
            else:
                complete_path = f'{args.workdir}/complete/{args.proj_code}*.json'
            complete_versions = glob.glob(complete_path)
            if len(complete_versions) > 0:
                kfile = complete_versions[-1]
                logger.info(f'Identified version {kfile.split("_")[-1].replace(".json","")}')

            else:
                logger.error(f'No complete kerchunk files located at {complete_path}')
                raise MissingKerchunkError
        if get_str:
            return kfile, True
        else:
            return open_kerchunk(kfile, logger, remote_protocol='https'), True
    else:
        logger.error(f'No Kerchunk file located at {args.proj_dir} and no in-place validation indicated - exiting')
        raise MissingKerchunkError
        
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
        mapper  = fsspec.get_mapper('reference://',fo=kfile, target_options={"compression":None}, remote_protocol=remote_protocol)
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
        logger.debug('Successfully opened Kerchunk with virtual xarray ds')
        return ds

def mem_to_value(mem):
    """Convert a memory value i.e 2G into a value"""
    suffix = mem[-1]
    return float(mem[:-1]) * SUFFIXES[suffix]

def value_to_mem(value):
    suffix_index = -1
    while value > 1000:
        value = value/1000
        suffix_index += 1
    return f'{value:.0f}{SUFFIX_LIST[suffix_index]}'

def check_memory(nfiles, indexes, mem, logger):
    logger.info(f'Performing Memory Allowance check for {len(indexes)} files')
    memcap = mem_to_value(mem)
    nftotal = 0
    for index in indexes:
        nftotal += os.path.getsize(nfiles[index]) 

    logger.debug(f'Determined memory requirement is {nftotal} - allocated {memcap}')
    if nftotal > memcap:
        raise ExpectMemoryError(required=value_to_mem(nftotal), current=mem)

def open_netcdfs(args, logger, thorough=False):
    """Returns a single xarray object with one timestep:
     - Select a single file and a single timestep from that file
     - Verify that a single timestep can be selected (Yes: return this xarray object, No: select all files and select a single timestep from that)
     - In all cases, returns a list of xarray objects.
    """
    logger.debug('Performing temporal selections')
    indexes, xfiles = get_netcdf_list(args.proj_dir, logger, thorough=thorough)

    if len(indexes) == len(xfiles):
        thorough = True
    xobjs = []
    if not thorough:
        if not args.bypass.skip_memcheck:
            check_memory(xfiles, indexes, args.memory, logger)
        else:
            logger.warning('Memory checks bypassed')
        for one, i in enumerate(indexes):
            xobjs.append(xr.open_dataset(xfiles[i]))

        if len(xobjs) == 0:
            logger.error('No valid timestep objects identified')
            raise NoValidTimeSlicesError(message='Kerchunk', verbose=args.verbose)
        return xobjs, indexes, xfiles
    else:
        if not args.bypass.skip_memcheck:
            check_memory(xfiles, [i for i in range(len(xfiles))], args.memory, logger)
        else:
            logger.warning('Memory checks bypassed')
        xobj = xr.concat([xr.open_dataset(fx) for fx in xfiles], dim='time', data_vars='minimal')
        return xobj, None, xfiles

## 3. Validation Testing

def match_timestamp(xobject, kobject, logger):
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
            logger.debug(f'Attempting selection with {timestamp}')
            ksel = kobject.sel(time=timestamp)
            xsel = xobject.sel(time=timestamp)
            assert ksel.time.size == 1 and xsel.time.size == 1
            logger.debug('Kerchunk timestamp selection was successful')
            return ksel, xsel
        except Exception as err:
            raise ChunkDataError
    else:
        logger.debug('Skipped timestamp selection as xobject has no time')
        return kobject, xobject

def compare_data(vname: str, xbox, kerchunk_box, logger, bypass=False):
    """Compare a NetCDF-derived ND array to a Kerchunk-derived one
     - Takes a netcdf selection box array of n-dimensions and an equally sized kerchunk_box array
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
    if not np.array_equal(xbox, kerchunk_box):
        logger.warn(f'Failed equality check for {vname}')
        raise ValidationError
    try:
        if np.abs(np.nanmax(kerchunk_box) - np.nanmax(xbox)) > tolerance:
            logger.warn(f'Failed maximum comparison for {vname}')
            logger.debug('K ' + str(np.nanmax(kerchunk_box)) + ' N ' + str(np.nanmax(xbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warn(f'Max comparison skipped for non-summable values in {vname}')
        else:
            raise err
    try:
        if np.abs(np.nanmin(kerchunk_box) - np.nanmin(xbox)) > tolerance:
            logger.warn(f'Failed minimum comparison for {vname}')
            logger.debug('K ' + str(np.nanmin(kerchunk_box)) + ' N ' + str(np.nanmin(xbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warn(f'Min comparison skipped for non-summable values in {vname}')
        else:
            raise err
    try:
        if np.abs(np.nanmean(kerchunk_box) - np.nanmean(xbox)) > tolerance:
            logger.warn(f'Failed mean comparison for {vname}')
            logger.debug('K ' + str(np.nanmean(kerchunk_box)) + ' N ' + str(np.nanmean(xbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warn(f'Mean comparison skipped for non-summable values in {vname}')
        else:
            raise err
    if not testpass:
        logger.error('Validation Error')
        raise ValidationError

def validate_shape_to_tolerance(nfiles: int, xv, dims, xshape, kshape, logger, detailfile=None):
    tolerance = 1/(nfiles*5)
    logger.info(f'Attempting shape bypass using concat-dim tolerance {tolerance*100}%')
    try:
        logger.debug('Finding concat dims recorded in details for this proj_code')
        with open(detailfile) as f:
            concat_dims = json.load(f)['concat_dims']
    except KeyError:
        logger.debug('Unable to determine concat_dims, defaulting to time-only')
        concat_dims = ['time']
        check_dims = []
        for cdim in concat_dims:
            # Match to index in xobj
            for index, dim in enumerate(dims):
                if dim == cdim:
                    check_dims.append(index)
        tolerance_error = False
        general_shape_error = False
        for cdim in range(len(xshape)):
            if cdim in check_dims:
                if abs(xshape[cdim] - kshape[cdim]) / kshape[cdim] > tolerance:
                    tolerance_error = XKShapeToleranceError(
                        tolerance=tolerance,
                        diff=abs(xshape[cdim] - kshape[cdim]) / kshape[cdim],
                        dim=dims[cdim]
                    )
            else:
                if xshape[cdim] != kshape[cdim]:
                    general_shape_error = ShapeMismatchError(var=xv, first=kshape, second=xshape)
        if general_shape_error:
            raise general_shape_error
        elif tolerance_error:
            raise tolerance_error
        else:
            pass

def validate_shapes(xobj, kobj, step: int, nfiles: list, xv: str, logger, proj_code, bypass_shape=False, detailfile=None, concat_dims={}):
    """Ensure shapes are equivalent across Kerchunk/NetCDF per variable
     - Accounts for the number of files opened vs how many files in total."""
    xshape = list(xobj[xv].shape)
    kshape = list(kobj[xv].shape)

    # Perform dimension adjustments if necessary
    logger.debug(f'{xv} - raw shapes - K: {kshape}, X: {xshape}')
    if concat_dims:
        for index, dim in enumerate(xobj[xv].dims):
            if dim in concat_dims:
                xshape[index] = concat_dims[dim]
    else:           
        if 'time' in xobj[xv].dims:
            try:
                xshape[0] *= nfiles
            except TypeError:
                logger.warning(f'{xv} - {nfiles}*{xshape[0]} failed to assign')
            except:
                pass
    logger.debug(f'{xv} - dimension-adjusted shapes - K: {kshape}, X: {xshape}')
    
    if len(xshape) != len(kshape):
        raise ShapeMismatchError(var=xv, first=kshape, second=xshape)
    elif xshape != kshape and bypass_shape: # Special bypass-shape testing
        if concat_dims == {}:
            logger.info('Attempting special bypass using tolerance feature')
            validate_shape_to_tolerance(nfiles, xv, xobj[xv].dims, xshape, kshape, logger, detailfile=detailfile)
        else:
            raise TrueShapeValidationError
    else:
        pass

def validate_selection(xvariable, kvariable, vname: str, divs: int, currentdiv: int, logger, bypass=BypassSwitch()):
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
        if bypass.skip_boxfail:
            logger.warning(f'{err} - check versions')
            isnan = True
        else:
            raise err
        
    if kbox.size >= 1 and not isnan:
        # Evaluate kerchunk vs xarray and stop here
        logger.debug(f'Found non-NaN values with box-size: {int(kbox.size)}')
        compare_data(vname, xbox, kbox, logger, bypass=bypass.skip_data_sum)
    else:
        logger.debug(f'Attempt {repeat} - slice is Null')
        if currentdiv >= 2:
            # Recursive search for increasing size (decreasing divisions)
            validate_selection(xvariable, kvariable, vname, divs, int(currentdiv/2), logger, bypass=bypass)
        else:
            logger.warn(f'Failed to find non-NaN slice (tried: {int(math.log2(divs))}, var: {vname})')
            if not bypass.skip_softfail:
                raise SoftfailBypassError

def validate_data(xobj, kobj, xv: str, step: int, logger, bypass=BypassSwitch()):
    """Run growing selection test for specified variable from xarray and kerchunk datasets"""
    logger.info(f'{xv} : Starting growbox data tests for {step}')

    kvariable, xvariable = match_timestamp(xobj[xv], kobj[xv], logger)

    # Attempt 128 divisions within selection - 128, 64, 32, 16, 8, 4, 2, 1
    return validate_selection(xvariable, kvariable, xv, 128, 128, logger, bypass=bypass)

def validate_timestep(args, xobj, kobj, step: int, nfiles: int, logger, concat_dims={}):
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
            validate_shapes(xobj, kobj, step, nfiles, xv, logger, args.proj_code,
                            bypass_shape=args.bypass.skip_xkshape, 
                            detailfile=f'{args.proj_dir}/detail-cfg.json',
                            concat_dims=concat_dims)
            logger.info(f'{xv} : Passed Shape test')
        logger.info(f'Passed all Shape tests')
        print()
        for xv in xvars:
            validate_data(xobj, kobj, xv, step, logger, bypass=args.bypass)
            logger.info(f'{xv} : Passed Data test')

def run_successful(args, logger):
    """Move kerchunk-1a.json file to complete directory with proper name"""
    # in_progress/<groupID>/<proj_code>/kerchunk_1a.json
    # complete/<groupID>/<proj_code.json

    kfile, in_place = locate_kerchunk(args, logger, get_str=True)

    if in_place:
        logger.info('Skipped moving files for in-place validation')
        return None
    
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

def run_backtrack(args, logger):
    """Backtrack progress on all output files. If quality is specified as well, files are removed rather than backtracked"""

    if args.groupID:
        complete_dir = f'{args.workdir}/complete/{args.groupID}'
    else:
        complete_dir = f'{args.workdir}/complete/single_runs'

    if args.proj_dir:
        proj_dir = args.proj_dir
    else:
        proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{args.proj_code}'

    logger.info("Removing 'complete' indicator file")

    for f in glob.glob(f'{proj_dir}/*complete*'):
        os.remove(f)
    if args.quality:
        logger.info("Removing Kerchunk file")
        for f in glob.glob(f'{complete_dir}/{args.proj_code}*'):
            os.remove(f)
    else:
        logger.info("Backtracking Kerchunk file")
        for x, file in enumerate(glob.glob(f'{complete_dir}/{args.proj_code}*')):
            os.rename(file, f'{proj_dir}/kerchunk-1{list("abcde")[x]}.json')

    logger.info(f'{args.proj_code} Successfully backtracked to pre-validation')
    
def attempt_timestep(args, xobj, kobj, step, nfiles, logger, xfiles, depth=0, concat_dims={}):
    try:
        validate_timestep(args, xobj, kobj, step, nfiles, logger, concat_dims=concat_dims)
    except ShapeMismatchError as err:
        if depth == 2:
            raise TrueShapeValidationError
        else:
            return True
    except XKShapeToleranceError as err:
        if depth < 1:
            # Try new routine to just get the key variable sizes
            concat_dims = get_concat_dims(xfiles, detailfile=f'{args.proj_dir}/detail-cfg.json')
            attempt_timestep(args, xobj, kobj, step+1, nfiles, logger, xfiles, depth=depth+1, concat_dims=concat_dims)
        else:
            return True
    except Exception as err:
        raise err

def validate_dataset(args):
    """Perform validation steps for specific dataset defined here
     - Determine the number of NetCDF files in total
     - Run validation for a minimum subset of those files
    """
    logger = init_logger(args.verbose, args.mode,'validate')
    logger.info(f'Starting tests for {args.proj_code}')

    if hasattr(args, 'backtrack'):
        if args.backtrack:
            run_backtrack(args, logger)
            return None

    if not args.proj_dir:
        if args.groupID:
            args.proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{args.proj_code}'
        else:
            args.proj_dir = f'{args.workdir}/in_progress/{args.proj_code}'

    xobjs, indexes, xfiles = open_netcdfs(args, logger, thorough=args.quality)
    nfiles = len(xfiles)
    if len(xobjs) == 0:
        raise NoValidTimeSlicesError(message='Xarray/NetCDF')
    if indexes == None:
        args.quality = True

    ## Open kerchunk file
    kobj, _v = locate_kerchunk(args, logger)
    if not kobj:
        raise MissingKerchunkError

    ## Set up loop variables
    fullset = bool(args.quality)

    if not fullset:
        logger.info(f"Attempting file subset validation: {len(indexes)}/{nfiles}")
        for step, index in enumerate(indexes):
            xobj = xobjs[step]
            logger.info(f'Running tests for selected file: {index} ({step+1}/{len(indexes)})')
            fullset = attempt_timestep(args, xobj, kobj, step+1, nfiles, logger, xfiles)

    if fullset:
        print()
        logger.info(f"Attempting total validation")
        xobjs, indexes, nfiles = open_netcdfs(args, logger, thorough=True)
        fullset = attempt_timestep(args, xobjs, kobj, 0, 1, logger, xfiles)
    
    logger.info('All tests passed successfully')
    print()
    run_successful(args, logger)

if __name__ == "__main__":
    print('Validation Process for Kerchunk Pipeline - run with single_run.py')