__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import xarray as xr
import json
from datetime import datetime
import fsspec
from fsspec.implementations.reference import ReferenceNotReachable
import random
import numpy as np
import glob
import logging
import math
import re
from functools import reduce
from itertools import groupby

from padocc.core.errors import (
    ShapeMismatchError,
    TrueShapeValidationError,
    NoValidTimeSlicesError,
    MissingKerchunkError,
    ArchiveConnectError,
    FullsetRequiredError,
    NoOverwriteError,
    SourceNotFoundError,
    ChunkDataError,
    ValidationError,
    XKShapeToleranceError,
    NaNComparisonError,
    SoftfailBypassError,
    VariableMismatchError
)
from padocc.core import BypassSwitch, FalseLogger
from padocc.core.utils import open_kerchunk

SUFFIXES = []
SUFFIX_LIST = []

from padocc.core import ProjectOperation, LoggedOperation

### Public Validation methods visible across PADOCC

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

def get_vslice(
        dimensions: list, 
        dtypes: list, 
        lengths: list, 
        divisions : list
    ) -> list:
    """
    Assemble dataset slice given the shape of the array and dimensions involved.
    
    :param shape:       (list) The dimension names for an array currently being assessed.

    :param dtypes:      (list) A list of the datatypes corresponding to each dimension for an array.

    :param lengths:     (list) The lengths of each dimension for an array.

    :param divisions:   (list) The number of divisions for each of the dimensions for an array.

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
    return vslice
       
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

## 2. Hypercube Validation

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
    logger.debug(f'Starting data comparison for {vname}')

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
            v1 = xbox[index]
            v2 = kerchunk_box[index]
            if v1 != v2:
                logger.error(f'X: {v1}, K: {v2}, idx: {index}')
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

def _validate_shapes(xobj, kobj, nfiles: int, xv: str, logger, bypass_shape=False, proj_dir=None, concat_dims={}) -> None: # Ingest into class structure
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
    logger.debug(f'Checking nan values for {label}: dtype: {str(box.dtype)}')

    if not ('float' in str(box.dtype) or 'int' in str(box.dtype)):
        # Non numeric arrays cannot have NaN values.
        return False
    
    arr = np.array(box)

    def handle_boxissue(err):
        if isinstance(err, TypeError):
            return False
        else:
            if bypass.skip_boxfail:
                logger.warning(f'{err} - Uncaught error bypassed')
                return False
            else:
                raise err

    if arr.size == 1:
        try:
            isnan = np.isnan(arr)
        except Exception as err:
            isnan = handle_boxissue(err)
    else:
        try:
            isnan = np.all(arr!=arr)
        except Exception as err:
            isnan = handle_boxissue(err)
        
        if not isnan and arr.size >= 1:
            try:
                isnan = np.all(arr == np.mean(arr))
            except Exception as err:
                isnan = handle_boxissue(err)

    return isnan

def get_slice(shape, division):
    vslice = []
    for s in shape:
        vslice.append(slice(0, math.ceil(s/division)))
    return vslice

def _count_duplicates(arr: list, source_num: int = None):
    """
    Count the number of duplicates in a list
    compared to the source number - return the values
    that are not present in all source arrays.
    """

    freq_items = {}
    for item in arr:
        if item in freq_items:
            freq_items[item] += 1
        else:
            freq_items[item] = 1

    if source_num is None:
        return freq_items
    else:
        missing = []
        for item, value in freq_items.items():
            if value < source_num:
                missing.append(item)
        return missing
    

class ValidateDatasets(LoggedOperation):
    """
    ValidateDatasets object for performing validations between two
    pseudo-identical Xarray Dataset objects
    """

    def __init__(
            self, 
            datasets: list,
            identifier: str,
            logger = None,
            label: str = None,
            fh: str = None,
            logid: str = None,
            verbose: bool = None,
        ):
        """
        Initiator for the ValidateDataset Class.
        Given a list of xarray.Dataset objects, all methods applied to 
        all datasets should give the same values as an output - the 
        outputs should be equivalent.
        
        These dataset objects should be identical, just from different sources.
        """

        self._identifier = identifier
        self._datasets   = datasets

        if len(self._datasets) > 2:
            raise NotImplementedError(
                'Simultaneous Validation of multiple datasets is not supported.'
            )

        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose
        )

    def __str__(self):
        return f'<PADOCC Validator: {self._identifier}>'

    def validate_metadata(self, allowances: dict = None):
        """
        Run all validation steps on this set of datasets.
        """

        allowances = allowances or {}
        ignore_vars, ignore_dims, ignore_globals = None, None, None

        # Validate global attributes
        if 'ignore_global_attrs' in allowances:
            ignore_globals = {'ignore': allowances['ignore_global_attrs']}

        self.validate_global_attrs(allowances=ignore_globals)

        if 'ignore_variables' in allowances:
            ignore_vars = {'ignore': allowances['ignore_variables']}
        if 'ignore_dimensions' in allowances:
            ignore_dims = {'ignore': allowances['ignore_dimensions']}

        # Validate variables/dimensions
        self._validate_variables(allowances=ignore_vars)
        self._validate_dimensions(allowances=ignore_dims)

    def _validate_variables(self, allowances: dict = None):
        """
        Validate variables public method
        """
        self._validate_selector(allowances=allowances, selector='variables')

    def _validate_dimensions(self, allowances: dict = None):
        """
        Validate dimensions public method
        """
        self._validate_selector(allowances=allowances, selector='dimensions')

    def _validate_selector(self, allowances: dict = None, selector: str = 'variables'):
        """
        Ensure all variables/dimensions are consistent across all datasets.
        Allowances dict contains configurations for skipping some variables
        in the case for example of a virtual dimension.

        allowances:
          ignore: [list to ignore]
        """
        ignore_vars = []

        allowances = allowances or {}
        if f'ignore' in allowances:
            ignore_vars = allowances['ignore']

        compare_vars = [[] for d in len(self._datasets)]
        total_list = []
        for index, d in enumerate(self._datasets):
            
            vset = getattr(d, selector)
            
            for var in vset:
                if var in ignore_vars:
                    continue
                compare_vars[index].append(var)
            total_list.extend(compare_vars[index])

        # Check each list has the same number of variables.
        if len(total_list) != len(compare_vars[0])*len(compare_vars):
            raise VariableMismatchError(
                f'The number of {selector} between datasets does not match: '
                f'Datasets have {[len(c) for c in compare_vars]} {selector} '
                'respectively.'
            )

        # Check all variables are present in all datasets.
        missing = _count_duplicates(total_list, source_num=len(self._datasets))
        if missing:
            raise VariableMismatchError(
                f'Inconsistent {selector} between datasets - {selector} '
                f'not present in all files: {missing}'
            )
        
        # Check variables appear in the same order in all datasets
        in_order = True
        for vset in zip(*compare_vars):
            vars = groupby(vset)
            is_equal = next(vars, True) and not next(vars, False)
            in_order = in_order and is_equal

        # Warning for different ordering only.
        if not in_order:
            self.logger.warning(
                f'{selector} present in a different order between datasets'
            )

    def validate_global_attrs(self, allowances: dict = None):
        """
        Validate the set of global attributes across all datasets
        """

        allowances = allowances or {}
        ignore = []
        if 'ignore' in allowances:
            ignore = allowances['ignore']

        attrset = []
        for d in self._datasets:
            attrset.append(d.attrs)

        self._validate_attrs(attrset, source='global.', ignore=ignore)

    def _validate_attrs(self, attrset: list, source: str = '', ignore: list = None):
        """
        Ensure all values across the sets of attributes are consistent
        """

        ignore = ignore or []
        for attr in attrset[0].keys():

            # Try extracting this attribute from all attribute sets.
            try:
                set_of_values = [a[attr] for a in attrset]
            except IndexError:
                if attr not in ignore:
                    raise ValueError(
                        f'Attribute {source}{attr} not present in all datasets'
                    )
                
            for s in set_of_values[1:]:
                if not np.all(s == set_of_values[0]):
                    raise ValueError(
                        f'Attribute {source}{attr} is not equal across all datasets:'
                        f'Found values: {set_of_values}'
                    )

    def _validate_shapes(self, ignore: list = None):
        """
        Ensure all variable shapes are consistent across all datasets.
        Allowances dict contains configurations for skipping some shape tests
        in the case for example of a virtual dimension.
        """
        ignore = ignore or []

        vset = self._datasets[0].variables

        for v in vset:
            test = self._datasets[0][v]
            control = self._datasets[1][v]

            testshape, controlshape = {}, {}

            for i in len(control.dims):
                if test.dims[i] not in ignore:
                    testshape[test.dims[i]] = test.shape[i]
                if control.dims[i] not in ignore:
                    controlshape[control.dims[i]] = control.shape[i]

            for dim in set(testshape.keys()) | set(controlshape.keys()):
                try:
                    ts = testshape[dim]
                except IndexError:
                    raise ShapeMismatchError(
                        f'"{dim}" dimension not present for {v} in dataset 0'
                    )
                
                try:
                    cs = controlshape[dim]
                except IndexError:
                    raise ShapeMismatchError(
                        f'"{dim}" dimension not present for {v} in dataset 1'
                    )

                if int(ts) != int(cs):
                    raise ShapeMismatchError(
                        f'Shape mismatch for {v} with dimension {dim} ({ts} != {cs})'
                    )

    def validate_data(self, allowances: dict = None):
        """
        Perform data validations using the growbox method for all datasets.
        """
        pass
        # Validate selection for all DataArrays in the Dataset

    def _validate_selection(
            self,
            test: xr.DataArray,
            control: xr.DataArray,
            current : int,
            name    : str,
            recursion_limit : int = 10, 
        ) -> bool:
        """
        General purpose validation for a specific variable from multiple sources.
        Both inputs are expected to be xarray DataArray objects but the control could
        instead by a NetCDF4 Dataset object. We expect both objects to be of the same size.
        """
        if test.size != control.size:
            raise ValueError(
                'Validation could not be completed for these objects due to differing'
                f'sizes - "{test.size}" and "{control.size}"'
            )

        if current >= recursion_limit:
            logger.debug('Maximum recursion depth reached')
            logger.info(f'Validation for {name} not performed')
            return None
        
        vslice = get_slice(test.shape, current)
        tbox   = test[vslice]
        cbox   = control[vslice]

        if check_for_nan(cbox):
            return self._validate_selection(test, control, current+1, name, recursion_limit=recursion_limit)
        else:
            return self._compare_data(name, tbox, cbox)

    def _compare_data(
        self, 
        vname: str, 
        control: xr.DataArray, 
        test: xr.DataArray
        ) -> None:
        """Compare a NetCDF-derived ND array to a Kerchunk-derived one. This function takes a 
        netcdf selection box array of n-dimensions and an equally sized test array and
        tests for elementwise equality within selection. If possible, tests max/mean/min calculations 
        for the selection to ensure cached values are the same.

        Expect TypeErrors later from summations which are bypassed. Other errors will exit the run.

        :param vname:           (str) The name of the variable described by this box selection

        :param control:            (obj) The native dataset selection

        :param test:    (obj) The cloud-format (Kerchunk) dataset selection

        :param logger:          (obj) Logging object for info/debug/error messages.

        :param bypass:          (bool) Single value flag for bypassing numeric data errors (in the
                                case of values which cannot be added).

        :returns:   None but will raise error if data comparison fails.
        """
        self.logger.debug(f'Starting data comparison for {vname}')

        self.logger.debug('1. Flattening Arrays')
        t1 = datetime.now()

        control         = np.array(control).flatten()
        test = np.array(test).flatten()

        self.logger.debug(f'2. Calculating Tolerance - {(datetime.now()-t1).total_seconds():.2f}s')
        try: # Tolerance 0.1% of mean value for xarray set
            tolerance = np.abs(np.nanmean(test))/1000
        except TypeError: # Type cannot be summed so skip all summations
            tolerance = None

        self.logger.debug(f'3. Comparing with array_equal - {(datetime.now()-t1).total_seconds():.2f}s')
        testpass = True
        try:
            equality = np.array_equal(control, test, equal_nan=True)
        except TypeError as err:
            equality = np.array_equal(control, test)

        if not equality:
            self.logger.debug(f'3a. Comparing directly - {(datetime.now()-t1).total_seconds():.2f}s')
            equality = False
            for index in range(control.size):
                v1 = control[index]
                v2 = test[index]
                if v1 != v2:
                    logger.error(f'X: {v1}, K: {v2}, idx: {index}')
            raise ValidationError
                
        self.logger.debug(f'4. Comparing Max values - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            if np.abs(np.nanmax(test) - np.nanmax(control)) > tolerance:
                self.logger.warning(f'Failed maximum comparison for {vname}')
                self.logger.debug('K ' + str(np.nanmax(test)) + ' N ' + str(np.nanmax(control)))
                testpass = False
        except TypeError as err:
            if bypass:
                self.logger.warning(f'Max comparison skipped for non-summable values in {vname}')
            else:
                raise err
        self.logger.debug(f'5. Comparing Min values - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            if np.abs(np.nanmin(test) - np.nanmin(control)) > tolerance:
                self.logger.warning(f'Failed minimum comparison for {vname}')
                self.logger.debug('K ' + str(np.nanmin(test)) + ' N ' + str(np.nanmin(control)))
                testpass = False
        except TypeError as err:
            if bypass:
                self.logger.warning(f'Min comparison skipped for non-summable values in {vname}')
            else:
                raise err
        self.logger.debug(f'6. Comparing Mean values - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            if np.abs(np.nanmean(test) - np.nanmean(control)) > tolerance:
                self.logger.warning(f'Failed mean comparison for {vname}')
                self.logger.debug('K ' + str(np.nanmean(test)) + ' N ' + str(np.nanmean(control)))
                testpass = False
        except TypeError as err:
            if bypass:
                self.logger.warning(f'Mean comparison skipped for non-summable values in {vname}')
            else:
                raise err
        if not testpass:
            self.logger.error('Validation Error')
            raise ValidationError


class ValidateOperation(ProjectOperation):
    """
    Encapsulate all validation testing into a single class. Instantiate for a specific project,
    the object could then contain all project info (from detail-cfg) opened only once. Also a 
    copy of the total datasets (from native and cloud sources). Subselections can be passed
    between class methods along with a variable index (class variables: variable list, dimension list etc.)

    Class logger attribute so this doesn't need to be passed between functions.
    Bypass switch contained here with all switches.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.concat_dims = None

    def _run(self):
        # Replaces validate timestep

        if self.detail_cfg.get('cfa'):
            # CFA-enabled validation
            self._perform_cfa_validation()
        else:
            self._perform_source_validation()

    def _open_cfa(self):
        """
        Open the CFA dataset for this project
        """

        return xr.open_dataset(self.cfa_path, engine='CFA', cfa_options=None)

    def _open_product(self):
        """
        Configuration to open object wrappers in the appropriate way so actions
        can be applied to all. Any products not usable with Xarray should have 
        an xarray-wrapper to allow the application of typical methods for comparison.
        """

        if self.cloud_format == 'kerchunk':
            # Kerchunk opening sequence
            return open_kerchunk(
                self.outfile, 
                self.logger,
                isparq = self.isparq,
                retry = True,
                attempt = 3
            )

    def _open_source(self, )

    def _perform_cfa_validation(self):
        """
        Perform validation for the selected dataset against the CFA-netCDF version.
        """

        Validator = ValidateDatasets(
            [cfa_ds, product_ds],
            f'{self.proj_code}-validator'
            logger = self.logger)

        # Perform metadata validation
        Validator.validate_metadata()

        # Perform data validation
        Validator.validate_data()

    def _perform_source_validation(self):
        """
        Perform validation by comparison to the source material
        """



    def _open_product(self):
        """
        Configuration to open object wrappers in the appropriate way so actions
        can be applied to all. Any products not usable with Xarray should have 
        an xarray-wrapper to allow the application of typical methods for comparison.
        """
        pass

    def _open_base(self):
        pass

    def _verify_connection(self):
        pass
