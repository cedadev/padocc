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
    logging.ERROR,
    logging.WARN,
    logging.INFO,
    logging.DEBUG
]

def init_logger(verbose, mode, name):
    """Logger object init and configure with formatting"""
    verbose = min(verbose, len(levels-1))

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

    closeness = np.isclose(netbox, kerchunk_box, rtol=tolerance, equal_nan=True)
    if closeness[closeness == False].size > 0:
        logger.warn(f'Failed elementwise comparison for {vname}')
        return False
    if np.abs(np.nanmax(kerchunk_box) - np.nanmax(netbox)) > tolerance:
        logger.warn(f'Failed maximum comparison for {vname}')
        return False
    if np.abs(np.nanmin(kerchunk_box) - np.nanmin(netbox)) > tolerance:
        logger.warn(f'Failed minimum comparison for {vname}')
        return False
    if np.abs(np.nanmean(kerchunk_box) - np.nanmean(netbox)) > tolerance:
        logger.warn(f'Failed mean comparison for {vname}')
        return False
    return True

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
        vslice = get_vslice(shape, dtypes, lengths, divisions)

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
        return compare_xk(vname, xbox, kbox, vname, logger)
    else:
        if divisions > 2:
            # Recursive search for increasing size (decreasing divisions)
            return test_growbox(xvariable, kvariable, vname, int(divisions/2), logger)
        else:
            logger.warn(f'Failed to find non-NaN slice (divisions: {divisions}, var: {vname})')
            return 422 # Unprocessable Entity

def test_single_file(index, xobj, kobj, logger):
    """Run all tests for a single file"""
    logger.debug(f'Running tests for {index}')
    vars = []
    for v in list(xobj.variables):
        if 'time' not in str(v) and str(xobj[v].dtype) in ['float32','int32','int64','float64']:
            vars.append(v)

    # Attempt testing on all non-time variables
    attempts = len(vars)
    sizes = []
    logger.info('Starting generic metadata tests')
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
    logger.info('All generic tests have passed, proceeding')

    # Make divisions such that initial box has ~1000 items
    defaults = np.array(sizes)/10000
    defaults[defaults < 100] = 100
    logger.info(f'Starting growbox method ({attempts} variables)')

    # Make testing attempts
    success = True
    for index in range(attempts):

        if int(index/attempts) % 10:
            logger.debug(f' > {int((index/attempts)/10)}%')

        var = vars[index]

        logger.info(f'Testing grow-box method for {var}')
        status = test_growbox(xobj[var],kobj[var], var, int(defaults[index]), logger) 
        if status == 422:
            logger.info(f'Grow-box failed softly for {var}')
            success = status 
        elif status:
            logger.info(f'Grow-box method passed for {var}')
        else:
            logger.error(f'Grow-box method failed for {var}')
            return False
        
    return success

def get_select_files(proj_dir):
    """Open document containing paths to all NetCDF files, make selections"""
    with open(f'{proj_dir}/allfiles.txt') as f:
        xfiles = [r.strip() for r in f.readlines()]

    # Open 3 random files in turn
    numfiles = int(len(xfiles)/1000)
    if numfiles < 3:
        numfiles = 3
    if numfiles > len(xfiles):
        numfiles = len(xfiles)
        fileset = xfiles

    else:
        indexes= []
        for f in range(numfiles):
            testindex = random.randint(0,numfiles)
            while testindex in indexes:
                testindex = random.randint(0,numfiles)
            indexes.append(testindex)

    return indexes, [xfiles[n] for n in indexes]

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
    files = glob.glob(args.proj_dir)
    kfiles = []
    for f in files:
        if 'kerchunk' in f:
            kfiles.append(f)
    if kfiles == []:
        return None
    kf = sorted(kfiles)[-1]
    logger.info(f'Selected {kf} from {len(kfiles)} available')
    return os.path.join(args.proj_dir, kf) # Latest version

def validate_dataset(args):
    """Perform validation steps for specific dataset defined here"""

    # Missing the kerchunk file name - given in config file or worked out?

    logger = init_logger(args.verbose, args.mode,'validate')

    logger.info(f'Starting tests for {args.proj_code}')
    indexes, xfiles = get_select_files(args.proj_dir)

    logger.info(f'Opening kerchunk dataset')

    kfile = get_kerchunk_file(args, logger)
    if not kfile:
        logger.error(f'No Kerchunk file located at {args.proj_dir} - exiting')
        return None
    
    kobj = open_kerchunk(kfile)
    totalpass = True

    for step in range(len(indexes)):
        logger.info(f'Running tests for selected file: {indexes[step]} ({step+1}/{len(indexes)}')
        xfile = xfiles[step]

        # Memory Size Check
        logger.debug('Checking memory size of expected netcdf file')
        if os.path.getsize(xfile) > 4e9 and not args.forceful: # 3GB file
            logger.error('Memory Exception - ensure you have 12GB or more dedicated to this task')
            return False

        # Temporal Aquisition Check
        logger.debug('Checking temporal selection is possible')
        xobj = xr.open_dataset(xfile)
        try:
            ksel = kobj.sel(time=xobj.time)
        except Exception as err:
            try:
                ksel = kobj.isel(time=indexes[step])
            except Exception as err:
                logger.error(f'Temporal Selection Error: {err}')
                return False

        # Proceed with testing
        logger.debug('Proceeding with validation checks')
        status = test_single_file(step, xobj, ksel, logger)
        if status == 422:
            logger.info(f'Testing failed softly for {step+1}')
            totalpass = status
        elif status:
            logger.info(f'Testing passed for {step+1}')
        else:
            logger.error(f'Testing failed for {step+1}')
            return False
    return totalpass
