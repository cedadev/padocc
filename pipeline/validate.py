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
from pipeline.logs import init_logger, SUFFIXES, SUFFIX_LIST
from pipeline.utils import BypassSwitch, open_kerchunk, get_proj_file, get_proj_dir
from ujson import JSONDecodeError
from dask.distributed import LocalCluster

class CloudValidator:
    """
    Encapsulate all validation testing into a single class. Instantiate for a specific project,
    the object could then contain all project info (from detail-cfg) opened only once. Also a 
    copy of the total datasets (from native and cloud sources). Subselections can be passed
    between class methods along with a variable index (class variables: variable list, dimension list etc.)

    Class logger attribute so this doesn't need to be passed between functions.
    Bypass switch contained here with all switches.
    """
    def __init__(self):
        pass

## 1. Array Selection Tools

def find_dimensions(dimlen: int, divisions: int) -> int:
    """
    Determine index of slice end position given length of dimension and fraction to assess.

    :param dimlen:      (int) The length of the specific dimension

    :param divisions:   (int) The number of divisions across this dimensions.

    :returns:   The size of each division for this dimension, given the number of total divisions.
    """
    # Round down then add 1
    divsize = int(dimlen/divisions)+1
    return divsize

def get_vslice(dimensions: list, dtypes: list, lengths: list, divisions: list, logger) -> list:
    """
    Assemble dataset slice given the shape of the array and dimensions involved.
    
    :param shape:       (list) The dimension names for an array currently being assessed.

    :param dtypes:      (list) A list of the datatypes corresponding to each dimension for an array.

    :param lengths:     (list) The lengths of each dimension for an array.

    :param divisions:   (list) The number of divisions for each of the dimensions for an array.

    :param logger:      (obj) Logging object for info/debug/error messages.

    :returns:   Slice for this particular division set. Special cases for datetime-like objects.
    """

    vslice = []
    for x, dim in enumerate(dimensions):
        if np.issubdtype(dtypes[x], np.datetime64):
            vslice.append(slice(0,find_dimensions(lengths[x],divisions)))
        elif dim == 1:
            vslice.append(slice(0,1))
        else:
            vslice.append(slice(0,find_dimensions(dim,divisions)))
    logger.debug(f'Slice {vslice}')
    return vslice

def get_concat_dims(xobjs: list, proj_dir) -> dict: # Ingest into class structure
    """
    Retrieve the sizes of the concatenation dims.

    :param xobjs:       (list) A list of xarray Dataset objects for files which are currently 
                        being assessed, from which to find the shapes of concat dimensions.

    :param proj_code:   (str) The project code in string format (DOI)

    :returns:   A dictionary of the concatenation dimensions and their array shapes.
    """
    concat_dims = {}
    details     = get_proj_file(proj_dir, 'detail-cfg.json')
    if details:
        # Initialise concat dims
        if 'concat_dims' in details:
            for dim in details['concat_dims']:
                concat_dims[dim] = 0

    for ds in xobjs:
        for dim in concat_dims.keys():
            concat_dims[dim] += ds[dim].shape[0]
    return concat_dims

## 2. File Selection Tools

def get_netcdf_list(proj_dir: str, logger, thorough=False) -> tuple: # Ingest into class structure
    """
    Open document containing paths to all NetCDF files, make selections and return a list of files.

    :param proj_dir:    (str) The project code directory path.

    :param logger:      (obj) Logging object for info/debug/error messages.

    :param thorough:    (bool) If True will select all files for testing, otherwise
                        standard validation subsetting (0.1% or 3 files) applies.
    
    :returns:   A tuple containing a list of all the files as well as a list of indexes to 
                specific files for testing. The index list should cover at least 3 files,
                with a maximum of 0.1% of the files selected in the case of > 3000 files.
    """
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

def locate_kerchunk(args, logger, get_str=False, remote_protocol='https') -> xr.Dataset:
    """
    Gets the name of the latest kerchunk file for this project code.

    :param args:        (obj) Set of command line arguments supplied by argparse.

    :param logger:      (obj) Logging object for info/debug/error messages.

    :param get_str:     (bool) If True will return the string filename for the selected
                        Kerchunk file, otherwise the Kerchunk file will be opened as an 
                        xarray Virtual Dataset.

    :param remote_protocol:     (str) Default 'https' for accessing files post-compute
                                since these have been reconfigured for remote testing.
                                Override with 'file' for Kerchunk files with a local 
                                reference.

    :returns:   Xarray Virtual Dataset from a Kerchunk file, or the string filename
                of the Kerchunk file if get_str is enabled.
    """
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
            return open_kerchunk(kfile, logger, remote_protocol=remote_protocol), False
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
            return open_kerchunk(kfile, logger, remote_protocol='file'), True
    else:
        logger.error(f'No Kerchunk file located at {args.proj_dir} and no in-place validation indicated - exiting')
        raise MissingKerchunkError
        
def mem_to_value(mem) -> float:
    """
    Convert a memory value i.e 2G into a value

    :returns:   Int value of e.g. '2G' in bytes.
    """
    suffix = mem[-1]
    return int(float(mem[:-1]) * SUFFIXES[suffix])

def value_to_mem(value) -> str:
    """
    Convert a number of bytes i.e 1000000000 into a string

    :returns:   String value of the above (1000000000 -> 1M)
    """
    suffix_index = -1
    while value > 1000:
        value = value/1000
        suffix_index += 1
    return f'{value:.0f}{SUFFIX_LIST[suffix_index]}'

def open_netcdfs(args, logger, thorough=False, concat_dims='time') -> list: # Ingest into class structure
    """Returns a single xarray object with one timestep:
    
    1. Select a single file and a single timestep from that file
    2. Verify that a single timestep can be selected (Yes: return this xarray object, No: select all files and select a single timestep from that)
    3. In all cases, returns a list of xarray objects.

    :param args:        (obj) Set of command line arguments supplied by argparse.

    :param logger:      (obj) Logging object for info/debug/error messages.

    :param thorough:    (bool) If True will concatenate all selected Datasets to a single
                        combined dataset, rather than a list of individual separate objects.

    :returns:   A list of the xarray datasets (or a single concatenated dataset), along with a list
                of indexes to use for selecting a subset of those files, plus a list of filepaths to
                the original files.
    """
    logger.debug('Performing temporal selections')
    indexes, xfiles = get_netcdf_list(args.proj_dir, logger, thorough=thorough)

    if len(indexes) == len(xfiles):
        thorough = True
    xobjs = []
    if not thorough:
        for i in indexes:
            xobjs.append(xr.open_dataset(xfiles[i]))
        if len(xobjs) == 0:
            logger.error('No valid timestep objects identified')
            raise NoValidTimeSlicesError(message='Kerchunk', verbose=args.verbose)
        return xobjs, indexes, xfiles
    else:
        #xobj = xr.concat([xr.open_dataset(fx) for fx in xfiles], dim=concat_dims, data_vars='minimal')
        xobj = xr.open_mfdataset(xfiles, combine='nested', concat_dim=concat_dims, data_vars='minimal')
        return xobj, None, xfiles

## 3. Validation Testing

def match_timestamp(xobject: xr.Dataset, kobject: xr.Dataset, logger) -> tuple: # Ingest into class structure
    """Match timestamp of xarray object to kerchunk object.
    
    :param xobject:     (obj) An xarray dataset representing the original files opened natively.
    
    :param kobject:     (obj) An xarray dataset representing the Kerchunk file constructed by the pipeline.
    
    :param logger:      (obj) Logging object for info/debug/error messages.
    
    :returns:   A tuple containing subselections of both xarray datasets such that both now have
                matching timestamps."""
    
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

def compare_data(vname: str, xbox: xr.Dataset, kerchunk_box: xr.Dataset, logger, bypass=False) -> None: # Ingest into class structure
    """Compare a NetCDF-derived ND array to a Kerchunk-derived one. This function takes a 
    netcdf selection box array of n-dimensions and an equally sized kerchunk_box array and
    tests for elementwise equality within selection. If possible, tests max/mean/min calculations 
    for the selection to ensure cached values are the same.

    Expect TypeErrors later from summations which are bypassed. Other errors will exit the run.

    :param vname:           (str) The name of the variable described by this box selection

    :param xbox:            (obj) The native dataset selection

    :param kerchunk_box:    (obj) The cloud-format (Kerchunk) dataset selection

    :param logger:          (obj) Logging object for info/debug/error messages.

    :param bypass:          (bool) Single value flag for bypassing numeric data errors (in the
                            case of values which cannot be added).

    :returns:   None but will raise error if data comparison fails.
    """
    logger.debug(f'Starting xk comparison')

    logger.debug('1. Flattening Arrays')
    t1 = datetime.now()

    xbox         = np.array(xbox).flatten()
    kerchunk_box = np.array(kerchunk_box).flatten()

    logger.debug(f'2. Calculating Tolerance - {(datetime.now()-t1).total_seconds():.2f}s')
    try: # Tolerance 0.1% of mean value for xarray set
        tolerance = np.abs(np.nanmean(kerchunk_box))/1000
    except TypeError: # Type cannot be summed so skip all summations
        tolerance = None

    logger.debug(f'3. Comparing with array_equal - {(datetime.now()-t1).total_seconds():.2f}s')
    testpass = True
    try:
        equality = np.array_equal(xbox, kerchunk_box, equal_nan=True)
    except TypeError as err:
        equality = np.array_equal(xbox, kerchunk_box)

    if not equality:
        logger.debug(f'3a. Comparing directly - {(datetime.now()-t1).total_seconds():.2f}s')
        equality = False
        for index in range(xbox.size):
            v1 = np.array(xbox).flatten()[index]
            v2 = np.array(xbox).flatten()[index]
            if v1 != v2:
                print(v1, v2)
                raise ValidationError
            
    logger.debug(f'4. Comparing Max values - {(datetime.now()-t1).total_seconds():.2f}s')
    try:
        if np.abs(np.nanmax(kerchunk_box) - np.nanmax(xbox)) > tolerance:
            logger.warning(f'Failed maximum comparison for {vname}')
            logger.debug('K ' + str(np.nanmax(kerchunk_box)) + ' N ' + str(np.nanmax(xbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warning(f'Max comparison skipped for non-summable values in {vname}')
        else:
            raise err
    logger.debug(f'5. Comparing Min values - {(datetime.now()-t1).total_seconds():.2f}s')
    try:
        if np.abs(np.nanmin(kerchunk_box) - np.nanmin(xbox)) > tolerance:
            logger.warning(f'Failed minimum comparison for {vname}')
            logger.debug('K ' + str(np.nanmin(kerchunk_box)) + ' N ' + str(np.nanmin(xbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warning(f'Min comparison skipped for non-summable values in {vname}')
        else:
            raise err
    logger.debug(f'6. Comparing Mean values - {(datetime.now()-t1).total_seconds():.2f}s')
    try:
        if np.abs(np.nanmean(kerchunk_box) - np.nanmean(xbox)) > tolerance:
            logger.warning(f'Failed mean comparison for {vname}')
            logger.debug('K ' + str(np.nanmean(kerchunk_box)) + ' N ' + str(np.nanmean(xbox)))
            testpass = False
    except TypeError as err:
        if bypass:
            logger.warning(f'Mean comparison skipped for non-summable values in {vname}')
        else:
            raise err
    if not testpass:
        logger.error('Validation Error')
        raise ValidationError

def validate_shape_to_tolerance(nfiles: int, xv: str, dims: tuple, xshape: tuple, kshape: tuple, logger, proj_dir=None) -> None: # Ingest into class structure
    """
    Special case function for validating a shaped array to some tolerance. This is an alternative to
    opening N files, only works if each file has roughly the same total shape. Tolerance is based on 
    the number of files supplied, more files means the tolerance is lower?

    :param nfiles:      (int) The number of native files across the whole dataset.

    :param xv:          (str) The name of the variable within the dataset.

    :param dims:        (tuple) A list of the names of the dimensions in this dataset.

    :param xshape:      (tuple) The shape of the array from the original native files.

    :param kshape:      (tuple) The shape of the array from the cloud formatted dataset.

    :param logger:      (obj) Logging object for info/debug/error messages.

    :param proj_dir:    (str) The project code directory path.
    """
    concat_dims = ['time'] # Default value - does not work for all cases.

    tolerance = 1/(nfiles*5)
    logger.info(f'Attempting shape bypass using concat-dim tolerance {tolerance*100}%')
    detail = get_proj_file(proj_dir, 'detail-cfg.json')
    if detail:
        logger.debug('Finding concat dims recorded in details for this proj_code')
        if 'concat_dims' in detail:
            concat_dims = detail['concat_dims']

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

def validate_shapes(xobj, kobj, nfiles: int, xv: str, logger, bypass_shape=False, proj_dir=None, concat_dims={}) -> None: # Ingest into class structure
    """
    Ensure shapes are equivalent across Kerchunk/NetCDF per variable. Must account for the number 
    of files opened vs how many files in total.
    
    :param xobj:        (obj) The native dataset selection.

    :param kobj:        (obj) The cloud-format (Kerchunk) dataset selection

    :param nfiles:      (int) The number of native files for this whole dataset.

    :param xv:          (str) The name of the variable within the dataset.

    :param logger:      (obj) Logging object for info/debug/error messages.

    :param bypass_shape:    (bool) Switch for bypassing shape errors - diverts to tolerance testing as a backup.

    :param proj_dir:        (str) The project code directory path.

    :param concat_dims:     (dict) Dictionary of concatenation dimensions with their appropriate 
                            sizes by index. (e.g {'time':100})

    :returns:   None but will raise error if shape validation fails.
    """
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
    if xshape != kshape:
        # Incorrect dimensions on the shapes of the arrays
        if xshape != kshape and bypass_shape: # Special bypass-shape testing
            logger.info('Attempting special bypass using tolerance feature')
            validate_shape_to_tolerance(nfiles, xv, xobj[xv].dims, xshape, kshape, logger, proj_dir=proj_dir)
        else:
            raise ShapeMismatchError(var=xv, first=kshape, second=xshape)
        
def check_for_nan(box, bypass, logger, label=None): # Ingest into class structure
    """
    Special function for assessing if a box selection has non-NaN values within it.
    Needs further testing using different data types.
    """
    logger.debug(f'Checking nan values for {label}: Dtype: {str(box.dtype)}')

    if not ('float' in str(box.dtype) or 'int' in str(box.dtype)):
        # Non numeric arrays cannot have NaN values.
        return False
    
    def handle_boxissue(err):
        if type(err) == TypeError:
            return False
        else:
            if bypass.skip_boxfail:
                logger.warning(f'{err} - Uncaught error bypassed')
                return False
            else:
                raise err

    if box.size == 1:
        try:
            isnan = np.isnan(box)
        except Exception as err:
            isnan = handle_boxissue(err)
    else:
        try:
            kb = np.array(box)
            isnan = np.all(kb!=kb)
        except Exception as err:
            isnan = handle_boxissue(err)
        
        if not isnan and box.size >= 1:
            try:
                isnan = np.all(kb == np.mean(kb))
            except Exception as err:
                isnan = handle_boxissue(err)
    return isnan

def validate_selection(xvariable, kvariable, vname: str, divs: int, currentdiv: int, logger, bypass=BypassSwitch()): # Ingest into class structure - note will need to alter 'scan' and 'compute' for a simplified validation option.
    """Validate this data selection in xvariable/kvariable objects
      - Recursive function tests a growing selection of data until one is found with real data
      - Repeats with exponentially increasing box size (divisions of all data dimensions)
      - Will halt at 1 division which equates to testing all data
    """

    # Determine number based on 
    repeat = int(math.log2(divs) - math.log2(currentdiv))

    logger.debug(f'Attempt {repeat} - {currentdiv} divs for {vname}')

    vslice = []
    shape = []
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
    if shape == [] and vslice == []:
        logger.debug(f'Skipping to full selection (1 division) for {vname}')
        currentdiv = 1

    # Need to check datatype and do something special here for strings
    if xbox.dtype == 'object':
        # Skip all other comparison steps for data types
        if not np.array_equal(xbox, kbox):
            raise ValidationError
        else:
            return None

    try_multiple = 0
    knan, xnan = False, True
    # Attempt nan checking multiple times due to network issues.
    while try_multiple < 3 and knan != xnan:
        knan = check_for_nan(kbox, bypass, logger, label='Kerchunk')
        xnan = check_for_nan(xbox, bypass, logger, label='Xarray')
        try_multiple += 1

    if knan != xnan:
        raise NaNComparisonError
        
    if kbox.size >= 1 and not knan:
        # Evaluate kerchunk vs xarray and stop here
        logger.debug(f'Found comparable box-size: {int(kbox.size)} values')
        compare_data(vname, xbox, kbox, logger, bypass=bypass.skip_data_sum)
    else:
        logger.debug(f'Attempt {repeat} - slice is Null')
        if currentdiv >= 2:
            # Recursive search for increasing size (decreasing divisions)
            validate_selection(xvariable, kvariable, vname, divs, int(currentdiv/2), logger, bypass=bypass)
        else:
            logger.warning(f'Failed to find non-NaN slice (tried: {int(math.log2(divs))}, var: {vname})')
            if not bypass.skip_softfail:
                raise SoftfailBypassError

def validate_data(xobj, kobj, xv: str, step: int, logger, bypass=BypassSwitch(), depth_default=128, nfiles=2): # Ingest into class structure
    """Run growing selection test for specified variable from xarray and kerchunk datasets"""
    logger.info(f'{xv} : Starting growbox data tests for {step+1} - {depth_default}')

    if nfiles > 1: # Timestep matching not required if only one file
        kvariable, xvariable = match_timestamp(xobj[xv], kobj[xv], logger)
    else:
        kvariable = kobj[xv]
        xvariable = xobj[xv]

    # Attempt 128 divisions within selection - 128, 64, 32, 16, 8, 4, 2, 1
    return validate_selection(xvariable, kvariable, xv, depth_default, depth_default, logger, bypass=bypass)

def validate_timestep(args, xobj, kobj, step: int, nfiles: int, logger, concat_dims={}, index=0): # Ingest into class structure
    """Run all tests for a single file which may or may not equate to 1 timestep"""
    # Note: step indexed from 0

    # Run Variable and Shape validation

    if 'virtual' in concat_dims:
        # Assume virtual dimension is first?
        logger.info("Filtering out virtual dimension for testing")
        virtual = {concat_dims['virtual']:index}
        logger.debug(f'Kerchunk index: {index}')
        kobj = kobj.isel(**virtual)

    xvars = set(xobj.variables)
    kvars = set(kobj.variables)
    if xvars&kvars != xvars: # Overlap of sets - all xvars should be in kvars
        missing = (xvars^kvars)&xvars 
        raise VariableMismatchError(missing=missing)
    else:
        logger.info(f'Passed Variable tests - all required variables are present')
        print()
        for xv in xvars:
            validate_shapes(xobj, kobj, nfiles, xv, logger,
                            bypass_shape=args.bypass.skip_xkshape,
                            proj_dir=args.proj_dir,
                            concat_dims=concat_dims)
            logger.info(f'{xv} : Passed Shape test')
        logger.info(f'Passed all Shape tests')
        print()
        for xv in xvars:
            validate_data(xobj, kobj, xv, step, logger, bypass=args.bypass, nfiles=nfiles)
            logger.info(f'{xv} : Passed Data test')

def run_successful(args, logger): # Ingest into class structure
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
    version_no = 'kr1.0'
    detailfile = f'{args.proj_dir}/detail-cfg.json'
    if os.path.isfile(detailfile):
        with open(detailfile) as f:
            details = json.load(f)
        if 'version_no' in details:
            version_no = details['version_no']
            logger.info(f'Found version {version_no} in detail-cfg')
    else:
        logger.warning('detail-cfg.json file missing or unreachable - using default version number')

    newfile = f'{complete_dir}/{args.proj_code}_{version_no}.json'
    if args.dryrun:
        logger.info(f'DRYRUN: mv {kfile} {newfile}')
    else:
        os.system(f'mv {kfile} {newfile}')
        os.system(f'touch {kfile}.complete')

def run_backtrack(workdir: str, groupID: str, proj_code: str,logger,  quality=False): # Ingest into class structure
    """Backtrack progress on all output files. If quality is specified as well, files are removed rather than backtracked"""

    if groupID:
        complete_dir = f'{workdir}/complete/{groupID}'
    else:
        complete_dir = f'{workdir}/complete/single_runs'

    proj_dir = get_proj_dir(proj_code, workdir, groupID)

    logger.info("Removing 'complete' indicator file")

    for f in glob.glob(f'{proj_dir}/*complete*'):
        os.remove(f)
    if quality:
        logger.info("Removing Kerchunk file")
        for f in glob.glob(f'{complete_dir}/{proj_code}*'):
            os.remove(f)
    else:
        logger.info("Backtracking Kerchunk file")
        for x, file in enumerate(glob.glob(f'{complete_dir}/{proj_code}*')):
            os.rename(file, f'{proj_dir}/kerchunk-1{list("abcde")[x]}.json')

    logger.info(f'{proj_code} Successfully backtracked to pre-validation')
    
def attempt_timestep(args, xobj, kobj, step, nfiles, logger, concat_dims, fullset=False): # Ingest into class structure
    """Handler for attempting processing on a timestep multiple times.
    - Handles error conditions"""
    try:
        validate_timestep(args, xobj, kobj, step, nfiles, logger, concat_dims=concat_dims)
    except ShapeMismatchError as err:
        if fullset:
            raise TrueShapeValidationError
        else:
            return True
    except Exception as err:
        raise err

def validate_dataset(args, logger, fh=None, logid=None, **kwargs): # Ingest into class structure - main function to use.
    """
    Perform validation steps for specific dataset defined here, sets up a local dask cluster to limit
    memory usage, retrieves the set of xarray objects and the kerchunk dataset, then runs validation
    on each of the selected indexes (subset of total number of objects).

    :param args:        (obj) Set of command line arguments supplied by argparse.

    :param logger:      (obj) Logging object for info/debug/error messages. Will create a new 
                        logger object if not given one.

    :param fh:          (str) Path to file for logger I/O when defining new logger.

    :param logid:       (str) If creating a new logger, will need an id to distinguish this logger
                        from other single processes (typically n of N total processes.)
    """
    if not logger:
        logger = init_logger(args.verbose, args.mode,'validate', fh=fh, logid=logid)
    logger.info(f'Starting tests for {args.proj_code}')

    # Removed for doing local connection tests.
    #logger.info('Testing Connection to the CEDA Archive')
    #if not verify_connection(logger):
       # raise ArchiveConnectError

    
    # Experimenting with a local dask cluster for memory limit
    #cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_target_fraction=0.95, memory_limit=str(args.memory + 'B'))

    # Removed Backtrack for now
    #if hasattr(args, 'backtrack'):
        #if args.backtrack:
            #run_backtrack(args, logger)
            #return None

    if not args.proj_dir:
        args.proj_dir = get_proj_dir(args.proj_code, args.workdir, args.groupID)

    xobjs, indexes, xfiles = open_netcdfs(args, logger, thorough=args.quality)
    nfiles = len(xfiles)
    if len(xobjs) == 0:
        raise NoValidTimeSlicesError(message='Xarray/NetCDF')
    if indexes == None:
        args.quality = True

    detailfile = f'{args.proj_dir}/detail-cfg.json'
    with open(detailfile) as f:
        details = json.load(f)

    ## Open kerchunk file
    kobj, _v = locate_kerchunk(args, logger)
    if not kobj:
        raise MissingKerchunkError

    virtual = False
    if 'virtual_concat' in details:
        virtual = details['virtual_concat']
    
    if virtual:
        concat_dims = {'virtual': details['combine_kwargs']['concat_dims'][0]}
        # Perform virtual attempt
        logger.info(f"Attempting file subset validation: {len(indexes)}/{nfiles} (virtual dimension)")
        for step, index in enumerate(indexes):
            xobj = xobjs[step]
            logger.info(f'Running tests for selected file: {index} ({step+1}/{len(indexes)})')
            attempt_timestep(args, xobj, kobj, step, nfiles, logger, concat_dims)
    else:
        ## Set up loop variables
        fullset     = bool(args.quality)
        concat_dims = get_concat_dims(xobjs, args.proj_dir)
        if not fullset:
            logger.info(f"Attempting file subset validation: {len(indexes)}/{nfiles}")
            for step, index in enumerate(indexes):
                xobj = xobjs[step]
                logger.info(f'Running tests for selected file: {index} ({step+1}/{len(indexes)})')
                fullset = attempt_timestep(args, xobj, kobj, step, nfiles, logger, concat_dims)
                if fullset:
                    break

        if fullset:
            print()
            logger.info(f"Attempting total validation")
            xobjs, indexes, nfiles = open_netcdfs(args, logger, thorough=True)
            attempt_timestep(args, xobjs, kobj, 0, 1, logger, concat_dims, fullset=True)
    
    logger.info('All tests passed successfully')
    print()
    run_successful(args, logger)

def verify_connection(logger):
    """
    Verify connection to the CEDA archive by opening a test file in Kerchunk and
    comparing to a known value."""

    kfile = 'https://dap.ceda.ac.uk/badc/cmip6/metadata/kerchunk/pipeline1/CMIP/AS-RCEC/TaiESM1/kr1.0/CMIP6_CMIP_AS-RCEC_TaiESM1_historical_r1i1p1f1_3hr_clt_gn_v20201013_kr1.0.json'
    validated = False
    tries = 0
    while not validated and tries < 5:
        try:
            mapper = fsspec.get_mapper('reference://',fo=kfile, backend_kwargs={'compression':None}, remote_protocol='https')
            ds = xr.open_zarr(mapper, consolidated=False, decode_times=True)
            value = ds['clt'].sel(lat=slice(51,59), lon=slice(-15,7)).isel(time=slice(0,5)).mean().compute()
            validated = bool(f"{value:.2f}" == '72.45')
            if not validated:
                tries += 1
                logger.warning(f'Failed data collection')
        except Exception as err:
            try:
                erstr = str(err)
            except:
                erstr = 'Unknown'
            logger.warning(f'Failed once with {erstr}')
            tries += 0.5
    return validated

if __name__ == "__main__":
    print('Validation Process for Kerchunk Pipeline - run with single_run.py')