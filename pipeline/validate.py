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

# Pick a specific variable
# Find number of dimensions
# Start in the middle
# Get bigger until we have a box that has non nan values

levels = [
    logging.WARN,
    logging.INFO,
    logging.DEBUG
]

def init_logger(verbose, mode, name):
    """Logger object init and configure with formatting"""
    verbose = min(verbose, len(levels)-1)

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

def compare_xk(vname, netbox, kerchunk_box, logger):
    """Compare a NetCDF-derived ND array to a Kerchunk-derived one"""
    logger.debug('Starting xk comparison')

    tolerance = np.abs(np.nanmean(kerchunk_box))/1000
    # Tolerance 0.1% of mean value for xarray set
    testpass = True
    closeness = np.isclose(netbox, kerchunk_box, rtol=tolerance, equal_nan=True)
    if closeness[closeness == False].size > 0:
        logger.warn(f'Failed elementwise comparison for {vname}')
        logger.error(netbox, kerchunk_box)
        testpass = False
    if np.abs(np.nanmax(kerchunk_box) - np.nanmax(netbox)) > tolerance:
        logger.warn(f'Failed maximum comparison for {vname}')
        logger.debug('K ' + str(np.nanmax(kerchunk_box)) + ' N ' + str(np.nanmax(netbox)))
        testpass = False
    if np.abs(np.nanmin(kerchunk_box) - np.nanmin(netbox)) > tolerance:
        logger.warn(f'Failed minimum comparison for {vname}')
        logger.debug('K ' + str(np.nanmin(kerchunk_box)) + ' N ' + str(np.nanmin(netbox)))
        testpass = False
    if np.abs(np.nanmean(kerchunk_box) - np.nanmean(netbox)) > tolerance:
        logger.warn(f'Failed mean comparison for {vname}')
        logger.debug('K ' + str(np.nanmean(kerchunk_box)) + ' N ' + str(np.nanmean(netbox)))
        testpass = False
    return testpass

def find_dimensions(dimlen, divisions):
    """Determine index of slice end position given length of dimension and fraction to assess"""
    # Round down then add 1
    slicemax = int(dimlen/divisions)+1
    return slicemax

def squeeze_dims(variable):
    """Remove 1-length dimensions from datasets - removes need for comparison if 1-length not present"""
    concat_axes = []
    for x, d in enumerate(variable.shape):
        if d == 1:
            concat_axes.append(x)
    for dim in concat_axes:
        variable = variable.squeeze(axis=dim)
    return variable

def get_vslice(shape, dtypes, lengths, divisions, logger):
    """Assemble dataset slice given the shape of the array and dimensions involved"""
    logger.debug(f'Getting slice at division level {divisions}')

    vslice = []
    for x, dim in enumerate(shape):
        if np.issubdtype(dtypes[x], np.datetime64):
            vslice.append(slice(0,find_dimensions(lengths[x],divisions)))
        elif dim == 1:
            vslice.append(slice(0,1))
        else:
            vslice.append(slice(0,find_dimensions(dim,divisions)))
    return vslice

def test_growbox(xvariable, kvariable, vname, divisions, logger):
    """Run growing-box test for specified variable from xarray and kerchunk datasets"""
    logger.debug(f'Starting growbox for {vname}')

    if divisions % 10 == 0:
        logger.debug(f'Attempting growbox for {divisions} divisions')

    # Squeeze (remove 1-length) dimensions if variable datasets have size greater than 1
    # If only one dimension present, dataset cannot be squeezed further.
    if xvariable.size > 1 and kvariable.size > 1:
        xvariable = squeeze_dims(xvariable)
        kvariable = squeeze_dims(kvariable)

    # Get variable slice for this set of divisions (smallest is 3 divisions, otherwise use whole selection 1 division)
    vslice = []
    if divisions > 1:
        shape = xvariable.shape
        dtypes  = [xvariable[xvariable.dims[x]].dtype for x in range(len(xvariable.shape))]
        lengths = [len(xvariable[xvariable.dims[x]])  for x in range(len(xvariable.shape))]
        vslice = get_vslice(shape, dtypes, lengths, divisions, logger)

        xbox = xvariable[tuple(vslice)]
        kbox = kvariable[tuple(vslice)]
    else:
        xbox = xvariable
        kbox = kvariable

    try:
        kb = np.array(kbox)
        isnan = np.all(kb!=kb)
    except TypeError as err:
        logger.error(f'NaN conversion - {err}')
        isnan = True
    except KeyError as err:
        logger.error(f'{err} - check versions')
        isnan = True
    if kbox.size > 1 and not isnan:
        # Evaluate kerchunk vs xarray and stop here
        logger.debug(f'Found non-nan values with box-size: {int(kbox.size)}')
        return compare_xk(vname, xbox, kbox, logger)
    else:
        if divisions > 2:
            # Recursive search for increasing size (decreasing divisions)
            return test_growbox(xvariable, kvariable, vname, int(divisions/2), logger)
        else:
            logger.warn(f'Failed to find non-NaN slice (divisions: {divisions}, var: {vname})')
            return 422 # Unprocessable Entity

def test_single_timestep(index, xobj, kobj, logger, vars=None):
    """Run all tests for a single file"""
    logger.debug(f'Running tests for {index}')
    if not vars:
        vars = []
        for v in list(xobj.variables):
            if 'time' not in str(v) and str(xobj[v].dtype) in ['float32','int32','int64','float64']:
                vars.append(v)

    # Attempt testing on all non-time variables
    attempts = len(vars)
    sizes = []
    logger.debug('Starting generic metadata tests')
    for v in vars:
        if v not in list(kobj.variables):
            logger.error(f'Variable missing from kerchunk dataset: {v}')
            return False
        else:
            xshape = xobj[v].shape
            kshape = kobj[v].shape
            k1shape = kshape[1:]
            if xshape != kshape and xshape != k1shape:
                logger.error(f'Test Failed: Kerchunk/Xarray shape mismatch for: {v} - ({xobj[v].shape}, {kobj[v].shape})')
                return False
        sizes.append(xobj[v].size)
    logger.debug('All generic tests have passed, proceeding')

    # Make divisions such that initial box has ~1000 items
    defaults = np.array(sizes)/10000
    defaults[defaults < 100] = 100
    logger.debug(f'Starting growbox method ({attempts} variables)')

    # Make testing attempts
    success = True
    for index in range(attempts):

        if int(index/attempts) % 10:
            logger.debug(f' > {int((index/attempts)/10)}%')

        var = vars[index]

        logger.debug(f'Testing grow-box method for {var}')
        status = test_growbox(xobj[var],kobj[var], var, int(defaults[index]), logger) 
        if status == 422:
            logger.debug(f'Grow-box failed softly for {var}')
            success = status 
        elif status:
            logger.debug(f'Grow-box method passed for {var}')
        else:
            logger.error(f'Grow-box method failed for {var}')
            return False
        
    return success

def get_select_files(proj_dir, logger):
    """Open document containing paths to all NetCDF files, make selections"""
    logger.debug('Identifying files to validate')
    with open(f'{proj_dir}/allfiles.txt') as f:
        xfiles = [r.strip() for r in f.readlines()]

    logger.debug(f'Found {len(xfiles)} files - filtering')
    # Open 3 random files in turn
    numfiles = int(len(xfiles)/1000)
    if numfiles < 3:
        numfiles = 3

    if numfiles > len(xfiles):
        numfiles = len(xfiles)
        indexes = [i for i in range(len(xfiles))]
    else:
        indexes = []
        for f in range(numfiles):
            testindex = random.randint(0,numfiles)
            while testindex in indexes:
                testindex = random.randint(0,numfiles)
            indexes.append(testindex)

    logger.debug(f'Filtered fileset to a list of {len(indexes)} files')

    return indexes, xfiles

def open_kerchunk(kfile, logger, isparq=False):
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
        return xr.open_zarr(mapper, consolidated=False)

def pick_index(nfiles, indexes):
    """Pick index of new netcdf file randomly, try 100 times"""
    index = random.randint(0,nfiles)
    count = 0
    while index in indexes and count < 100:
        index = random.randint(0,nfiles)
        count += 1
    indexes.append(index)
    return indexes

def get_kerchunk_file(args, logger):
    """Gets the name of the latest kerchunk file for this project code"""
    files = os.listdir(args.proj_dir) # Get filename only
    kfiles = []
    for f in files:
        if 'kerchunk' in f:
            kfiles.append(f)
    if kfiles == []:
        return None
    kf = sorted(kfiles)[-1]
    logger.info(f'Selected {kf} from {len(kfiles)} available')
    return os.path.join(args.proj_dir, kf) # Latest version

def select_kerchunk(args, kobject, timestamp, indexstamp, rawsize, logger):
    logger.debug(f'Kerchunk object total time stamps: {kobject.time.size}')
    if kobject.time.size != rawsize:
        if args.bypass:
            logger.warning('Time Size discontinuity at base level - bypassed')
        else:
            logger.error('Time Size discontinuity at base level')
            logger.error(f'K {kobject.time.size} N {rawsize}')
            logger.error('Check Kerchunk file includes all files if these two values are wildly different')
            raise ValueError
    try:
        ksel = kobject.sel(time=timestamp)
        assert ksel.time.size == 1
        logger.debug('Kerchunk timestamp selection was successful')
        return ksel
    except Exception as err:
        try:
            ksel = kobject.isel(time=indexstamp)
            assert ksel.time.size == 1
            logger.debug('Kerchunk timestamp selection unsuccessful - switched to index selection')
            return ksel, rawsize
        except Exception as err:
            logger.warn(f'Temporal Selection Error: {err}')
            return False, rawsize

def select_all(args, xfiles, i, logger):
    try:
        xobj = xr.open_mfdataset(xfiles)
        if xobj.time.size > 1:
            xobj = xobj.isel(time=i)
        assert xobj.time.size == 1
        xobjs = [xobj]
        return xobjs
    except AssertionError:
        logger.error('Time selection error: complete time series failed to select')
        return []
    except Exception as err:
        if args.bypass:
            logger.error(f'Unexpected error - {err}')
            return []
        else:
            raise err

def select_netcdfs(args, logger):
    """Returns a single xarray object with one timestep:
       - Select a single file and a single timestep from that file
       - Verify that a single timestep can be selected
         - If yes, return this xarray object
         - If no, select all files and select a single timestep from that.
       - In all cases, returns a list of xarray objects
    """
    logger.debug('Performing temporal selections')
    indexes, xfiles = get_select_files(args.proj_dir, logger)

    xobjs = []
    many = len(indexes)
    for one, i in enumerate(indexes):

        # Memory Size Check
        logger.debug(f'Checking memory size of expected netcdf file for index {i} ({one+1}/{many})')
        if os.path.getsize(xfiles[i]) > 4e9 and not args.forceful: # 3GB file
            logger.error('Memory Exception - ensure you have 12GB or more dedicated to this task')
            return False

        try:
            xobj = xr.open_dataset(xfiles[i])

            rawsize = len(xfiles)*xobj.time.size
            # Currently inoperable - time selection not accurate with 2-dimensional timesteps
            if xobj.time.size > 1:
                logger.debug(f'Multiple time indexes selected for index {i} - {xobj.time.size} total')
                xobj = xobj.isel(time=0)

            assert xobj.time.size == 1
            xobjs.append(xobj)
            logger.debug(f'Added file index {i} time index 0')
        except Exception as err:
            if err != AssertionError:
                logger.warning(f'Unexpected time selection error - {err}')
            print(xfiles)
            xobjs = select_all(args, xfiles, i, logger)

    if len(xobjs) == 0:
        logger.error('No valid timestep objects identified - exiting')
    return xobjs, indexes, rawsize

def perform_validations(xobj, kobj, step, total, totalpass, logger):
    # Proceed with testing
    logger.debug('Proceeding with validation checks')
    status = test_single_timestep(step, xobj, kobj, logger)
    if status == 422:
        logger.debug(f'Testing failed softly for {step+1} of {total}')
        totalpass = status
    elif status:
        logger.debug(f'Testing passed for {step+1} of {total}')
    else:
        logger.error(f'Testing failed for {step+1} of {total}')
        totalpass = False
    return totalpass

def run_successful(args, kfile, logger):
    """Move kerchunk-1a.json file to complete directory with proper name"""
    # in_progress/<groupID>/<proj_code>/kerchunk_1a.json
    # complete/<groupID>/<proj_code.json

    if args.groupID:
        complete_dir = f'{args.workdir}/complete/{args.groupID}'
    else:
        complete_dir = f'{args.workdir}/complete/single_runs'

    if not os.path.isdir(complete_dir):
        os.makedirs(complete_dir)

    # Open config file to get correct version

    newfile = f'{complete_dir}/{args.proj_code}-kr1.0.json'
    if args.dryrun:
        logger.info(f'DRYRUN: mv {kfile} {newfile}')
    else:
        os.system(f'mv {kfile} {newfile}')
        os.system(f'touch {kfile}.complete')

def validate_dataset(args):
    """Perform validation steps for specific dataset defined here"""
    logger = init_logger(args.verbose, args.mode,'validate')
    logger.info(f'Starting tests for {args.proj_code}')

    ## Identify xarray objects and select random indexes with timesteps
    xobjs, indexes, rawsize = select_netcdfs(args, logger)
    if len(xobjs) > 0:
        logger.info(f'Identified {len(xobjs)} valid timesteps for validation')
    else:
        logger.error('No valid timesteps found - suggest examination of input files')
        return None

    ## Open kerchunk file
    logger.info(f'Opening kerchunk dataset')
    kfile = get_kerchunk_file(args, logger)
    if not kfile:
        logger.error(f'No Kerchunk file located at {args.proj_dir} - exiting')
        return None
    kobj = open_kerchunk(kfile, logger)

    ## Set up loop variables
    totalpass = True
    total     = len(xobjs)

    for step, xobj in enumerate(xobjs):
        logger.info(f'Running tests for selected file: {indexes[step]} ({step+1}/{len(indexes)})')

        timestamp = xobj.time
        timeindex = indexes[step]
        ksel = select_kerchunk(args, kobj, timestamp, timeindex, rawsize, logger)
        totalpass = perform_validations(xobj, ksel, step, total, totalpass, logger)

    if totalpass == 422:
        logger.info('Tests passed softly - specific variables have not been verified due to lack of values')
        if args.bypass:
            run_successful(args, kfile, logger)
        else:
            logger.info('Final step not performed - completed dataset will only be created with bypass flag')
    elif totalpass:
        logger.info('All tests passed successfully')
        run_successful(args, kfile, logger)
    else:
        logger.info('One or more tasks failed for this dataset')

if __name__ == "__main__":
    print('Validation Process for Kerchunk Pipeline - run with single_run.py')