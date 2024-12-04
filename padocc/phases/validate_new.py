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
    
def slice_all_dims(data_arr: xr.DataArray, intval: int):
    """
    Slice all dimensions for the DataArray according 
    to the integer value."""
    shape = tuple(data_arr.shape)

    for d in shape:
        if d < 8:
            continue

        mid = int(d/2)
        step = int(d/(intval*2))
        data_arr = data_arr[mid-step:mid+step]
    return data_arr

def default_preslice(data_arr: xr.DataArray):
    """
    Default preslice performs no operations on the
    data array.
    """
    return data_arr

class ValidateDatasets(LoggedOperation):
    """
    ValidateDatasets object for performing validations between two
    pseudo-identical Xarray Dataset objects.
    """

    def __init__(
            self, 
            datasets: list,
            identifier: str,
            preslice_fn: list = None, # Preslice each dataset's DataArrays to make equivalent.
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

        self.variables = None
        self.dimensions = None

        self._preslice_fn = preslice_fn or [default_preslice for d in datasets]

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
    
    def test_dataset_var(self, var):
        """
        Get a variable DataArray from the test dataset, 
        performing preslice functions.
        """
        return self._dataset_var(var, 0)
    
    def control_dataset_var(self, var):
        """
        Get a variable DataArray from the control dataset, 
        performing preslice functions.
        """
        return self._dataset_var(var, 1)

    def _dataset_var(self, var, id):
        """
        Perform preslice functions on the requested DataArray
        """
        return self._preslice_fn[id](self._datasets[id][var])

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
        
        setattr(self, selector, set(total_list))

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
            test = self.test_dataset_var(v)
            control = self.control_dataset_var(v)

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

    def validate_data(self):
        """
        Perform data validations using the growbox method for all variable DataArrays.
        """

        if self.variables is None:
            self.logger.error(
                'Unable to validate data, please ensure metadata has been validated first.'
                'Use `validate_metadata()` method.'
            )
            return None

        for var in self.variables:
            self.logger.info('Validating selection for ')
            testvar = self.test_dataset_var(var)
            controlvar = self.control_dataset_var(var)

            # Check access to the source data somehow here
            # Initiate growbox method - recursive increasing box size.
            self._validate_selection(var, testvar, controlvar)

    def _validate_selection(
            self,
            var: str,
            test: xr.DataArray,
            control: xr.DataArray,
            current : int = 1,
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
            self.logger.debug('Maximum recursion depth reached')
            self.logger.info(f'Validation for {var} not performed')
            return None
        
        tbox = slice_all_dims(test, current)
        cbox = slice_all_dims(control, current)

        if check_for_nan(cbox):
            return self._validate_selection(test, control, current+1, var, recursion_limit=recursion_limit)
        else:
            return self._compare_data(var, tbox, cbox)

    def _compare_data(
        self, 
        vname: str, 
        test: xr.DataArray, 
        control: xr.DataArray
        ) -> None:
        """
        Compare a NetCDF-derived ND array to a Kerchunk-derived one. This function takes a 
        netcdf selection box array of n-dimensions and an equally sized test array and
        tests for elementwise equality within selection. If possible, tests max/mean/min calculations 
        for the selection to ensure cached values are the same.

        Expect TypeErrors later from summations which are bypassed. Other errors will exit the run.

        :param vname:           (str) The name of the variable described by this box selection

        :param test:            (obj) The cloud-format (Kerchunk) dataset selection

        :param control:         (obj) The native dataset selection

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
                    self.logger.error(f'X: {v1}, K: {v2}, idx: {index}')
            raise ValidationError
                
        self.logger.debug(f'4. Comparing Max values - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            if np.abs(np.nanmax(test) - np.nanmax(control)) > tolerance:
                self.logger.warning(f'Failed maximum comparison for {vname}')
                self.logger.debug('K ' + str(np.nanmax(test)) + ' N ' + str(np.nanmax(control)))
                testpass = False
        except TypeError as err:
            if self.bypass:
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
            if self.bypass:
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
            if self.bypass:
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

        # test = open_product()

        if self.detail_cfg.get('cfa'):
            # CFA-enabled validation
            # control = open_cfa()
            pass
        else:
            # Use default 
            # Rethink preslice_fn - will need this to be dynamic.
            pass

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
