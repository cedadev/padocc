__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import json
import random
from datetime import datetime
from typing import Optional, Union

import numpy as np
import xarray as xr

from padocc.core import BypassSwitch, LoggedOperation, ProjectOperation
from padocc.core.errors import worst_error, ValidationError, AggregationError
from padocc.core.filehandlers import JSONFileHandler
from padocc.core.utils import format_tuple, timestamp, extract_json

SUFFIXES = []
SUFFIX_LIST = []
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
            
    def get_origin(arr):
        if len(arr.shape) > 1:
            return get_origin(arr[0])
        else:
            return arr[0]

    if arr.size == 1:
        try:
            isnan = np.isnan(arr)
        except Exception as err:
            isnan = handle_boxissue(err)
    else:
        try:
            isnan = np.all(arr == np.nan)
        except Exception as err:
            isnan = handle_boxissue(err)

    return isnan

def slice_all_dims(data_arr: xr.DataArray, intval: int, dim_mid: Union[dict[int,None],None] = None):
    """
    Slice all dimensions for the DataArray according 
    to the integer value."""
    shape = tuple(data_arr.shape)
    dims  = tuple(data_arr.dims)

    dim_mid = dim_mid or {}

    slice_applied = []
    for dim, d in zip(dims, shape):
        if d < 8:
            slice_applied.append(slice(0,d))
            continue

        mid = int(d/2)
        if dim_mid.get(dim,None) is not None:
            mid = dim_mid[dim]

        step = int(d/(intval*2))
        # Rounding issue solve - bug 20/05/25
        if step < 0.5:
            step = 0.51
        slice_applied.append(slice(int(mid-step),int(mid+step)))
    return tuple(slice_applied)

def format_slice(slice: list[slice]) -> str:
    starts = []
    ends = []
    for s in slice:
        starts.append(str(s.start))
        ends.append(str(s.stop))
    return "(%s)" % ','.join(starts), "(%s)" % ','.join(ends)

def _recursive_set(source: dict, keyset: list, value):
    """
    Method for recursively setting values in a dictionary.
    """
    if len(keyset) > 1:

        # Preserve existing values
        current = {}
        if keyset[0] in source:
            current = source[keyset[0]]

        source[keyset[0]] = _recursive_set(current,keyset[1:], value)
    else:
        try:
            _ = json.dumps(value)
            current = value
        except TypeError:
            current = 'N/A'
        source[keyset[0]] = value
    return source

class PresliceSet:
    """
    Preslice Object for handling slices applied to datasets.
    """

    def __init__(self, logger):
        self._preslice_set = {}
        self.logger = logger

    def add_preslice(self, preslice: dict[slice], var: str):
        self._preslice_set[var] = preslice

    def apply(self, data_arr: xr.DataArray, var: str) -> xr.DataArray:
        """
        Apply a preslice operation to a data array"""

        squeeze_dims=[]
        if var not in self._preslice_set:
            return self._default_preslice(data_arr)
        else:
            for dim, dslice in self._preslice_set[var].items():
                if isinstance(dslice,tuple):
                    squeeze_dims.append(dslice[1])
                    self._preslice_set[var][dim] = dslice[0]

            self.logger.debug(self._preslice_set[var])
            self.logger.debug(squeeze_dims)
            da = data_arr.isel(**self._preslice_set[var])
            if len(squeeze_dims) > 0:
                da = da.squeeze(dim=squeeze_dims, drop=True)
            return da

    def _default_preslice(self, data_arr: xr.DataArray) -> xr.DataArray:
        """
        Default preslice performs no operations on the
        data array.
        """
        return data_arr

class Report:
    """
    Special report class, capable of utilising recursive
    dictionary value-setting."""
    description = 'Special report class for recursive dictionary value-setting'

    def __init__(self, fh: Union[dict,object,None] = None, bypass: dict = None):
        
        self._value = fh or {}
        self._bypass = bypass or None

    def __setitem__(self, index, value):
        nest = index.split(',')

        if len(nest) == 1:
            self._value[nest[0]] = value
            return

        current = {}
        if nest[0] in self._value:
            current = self._value[nest[0]]
        self._value[nest[0]] = _recursive_set(current, nest[1:], value)

    @property
    def value(self):
        return self._clean_report()

    def _clean_report(self):
        """
        Recursive report cleaning function.
        
        Removes non-serialisable elements."""
        def __clean_report(rep):
            
            for k, v in rep.items():
                if isinstance(v, dict):
                    v = __clean_report(v)
                else:
                    try:
                        _ = json.dumps(v)
                    except TypeError:
                        v = 'N/A'
                rep[k] = v
            return rep

        return __clean_report(self._value)

    def export(self):
        # Taking into account bypass control
        return self.value

    def __bool__(self):
        return bool(self.value)

    def __dict__(self):
        return self.value.get()
    
    def __repr__(self):
        return json.dumps(self.value)
    
    def __str__(self):
        return json.dumps(self.value, indent=2)

class ValidateDatasets(LoggedOperation):
    """
    ValidateDatasets object for performing validations between two
    pseudo-identical Xarray Dataset objects.

    4th Dec Note:
    Validate metadata using single NetCDF(Xarray) vs Kerchunk
    Validate data using combined NetCDF or CFA vs Kerchunk
    (for best performance)
    """

    def __init__(
            self, 
            datasets: list,
            id: str,
            filehandlers: Optional[Union[list[JSONFileHandler], list[dict]]] = None,
            dataset_labels: Union[list,None] = None,
            preslice_fns: Union[list,None] = None, # Preslice each dataset's DataArrays to make equivalent.
            error_bypass: Union[dict, str, None] = None,
            logger = None,
            label: Union[str,None] = None,
            fh: Union[str,None] = None,
            logid: Union[str,None] = None,
            verbose: int = 0,
            bypass_vars: Union[list,None] = None,
            concat_dims: Union[list,None] = None
        ):
        """
        Initiator for the ValidateDataset Class.
        Given a list of xarray.Dataset objects, all methods applied to 
        all datasets should give the same values as an output - the 
        outputs should be equivalent.
        
        These dataset objects should be identical, just from different sources.
        """

        # Bypass checking specific variables if requested
        self.bypass_vars = bypass_vars or []

        self.concat_dims = concat_dims or []

        # Bypass considering fatal/warnings from entire report
        self.error_bypass = error_bypass or {}
        if isinstance(self.error_bypass, str):
            self.error_bypass = extract_json(self.error_bypass)

        self._id = id
        self._datasets   = datasets

        self._labels    = dataset_labels or [str(i) for i in range(len(datasets))]

        self.variables  = None
        self.dimensions = None

        self.decoded_times = True

        self.fhs = filehandlers or [{},{}]

        self._metadata_report = None
        self._data_report = None

        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose
        )

        self._preslice_fns = preslice_fns or [PresliceSet(self.logger) for d in datasets]

        if len(self._datasets) > 2:
            raise NotImplementedError(
                'Simultaneous Validation of multiple datasets is not supported.'
            )

    def __str__(self):
        return f'<PADOCC Validator: {self._id}>'
    
    def pass_fail(self, err: str):
        if 'Warn' in err:
            return 'Warning'
        elif 'Fatal' in err:
            return 'Fatal'
        return 'Success'

    
    @property
    def data_report(self):
        """Read-only data report"""
        return self._data_report
    
    @property
    def metadata_report(self):
        """Read-only metadata report"""
        return self._metadata_report

    @property
    def report(self):
        if self._metadata_report is None:
            return None
        
        if self._data_report is None:
            return {
                'metadata': self._metadata_report.export()
            }
        
        return {
            'report':{
                'metadata': self._metadata_report.export(),
                'data': self._data_report.export()
            }
        }

    def save_report(self, filehandler=None):
        """
        Formulate report such that it notes bypasses, and determine
        the worst error.
        
        This is in addition to saving the report content.
        """

        err, report = worst_error(self.report, bypass = self.error_bypass)

        if filehandler is not None:
            filehandler.set(report)
            filehandler.save()
            return err
        
        if isinstance(self.fhs[0], JSONFileHandler):
            self.fhs[0].set(report.get('metadata',{}))
            self.fhs[0].save()

            self.fhs[1].set(report.get('data',{}))
            self.fhs[1].save()
            return err
        
        raise ValueError(
            'Filehandler not provided to save report'
        )

    def replace_dataset(
            self, 
            new_ds: xr.Dataset, 
            label: str = None, 
            index: int = None, 
            dstype: str = None
        ) -> None:
        """
        Replace dataset by type, label or index.
        """

        if label is not None:
            index = self._labels.index(label)

        if dstype is not None:
            types = ['test','control']
            index = types.index(dstype)

        if index is not None:
            self._datasets[index] = new_ds
    
    def replace_preslice(
            self, 
            new_preslice: xr.Dataset, 
            label: str = None, 
            index: int = None, 
            dstype: str = None
        ) -> None:
        """
        Replace dataset by type, label or index.
        """

        if label is not None:
            index = self._labels.index(label)

        if dstype is not None:
            types = ['test','control']
            index = types.index(dstype)

        if index is not None:
            self._preslice_fns[index] = new_preslice

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
        return self._preslice_fns[id].apply(self._datasets[id][var], var)

    def decode_times_ok(self):
        """
        Determine if test and sample datasets have matching encodings.
        """

        self.logger.info('Checking matching time encoding/dtype')
        
        if not hasattr(self._datasets[0],'time'):
            return False

        dtypes = {}
        for ds in self._datasets:
            dtypes[ds.time.dtype] = 1

        if len(dtypes.keys()) > 1:
            self.logger.warning('Time encoding mismatch - recorded')
            self.decoded_times = False
            return False
        return True

    def validate_metadata(self, allowances: dict = None) -> dict:
        """
        Run all validation steps on this set of datasets.
        """

        # Reset for new report run
        self._metadata_report = Report()
        self.logger.info('Initialised metadata report')

        self.concat_dims=['time']

        aggregation_errors = []
        for dim in self.concat_dims:
            testdim = self.test_dataset_var(dim)

            # Increasing across aggregation dim
            fwd  = np.all(np.array(testdim[1:]) > np.array(testdim[:-1]))

            # Decreasing across aggregation dim
            back = np.all(np.array(testdim[1:]) < np.array(testdim[:-1]))

            if not fwd and not back:
                aggregation_errors.append(dim)
            
        if len(aggregation_errors) > 0:
            raise AggregationError(aggregation_errors)

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
        self.logger.info('Performing validation checks: Variables')
        self._validate_selector(allowances=allowances, selector='variables')

    def _validate_dimensions(self, allowances: dict = None):
        """
        Validate dimensions public method
        """
        self.logger.info('Performing validation checks: Dimensions')
        self._validate_selector(allowances=allowances, selector='dims')

    def _validate_selector(self, allowances: dict = None, selector: str = 'variables'):
        """
        Ensure all variables/dimensions are consistent across all datasets.
        Allowances dict contains configurations for skipping some variables
        in the case for example of a virtual dimension.

        allowances:
          ignore: [list to ignore]
        """
        ignore_sels = []
        # Determine the set of selectors
        # Only able to compare two datasets at once
        test_set    = list(getattr(self._datasets[0], selector))
        control_set = list(getattr(self._datasets[1], selector))
        all_selectors = set(control_set) | set(test_set)

        ignore_attrs = {all_s:[] for all_s in all_selectors}

        # Collect ignored selectors and attributes
        allowances = allowances or {}
        if 'ignore' in allowances:
            ignore_sels = allowances['ignore']
        if 'attributes' in allowances:
            for scode in allowances['attributes']:
                s, attr = scode.split('.')
                ignore_attrs[s].append(attr)
            
        misordered = False
        if len(test_set) != len(control_set):
            misordered = True

        self.logger.info(f'Checking {selector}:')
        for order, s in enumerate(control_set):
            self.logger.info(f' - {s}')

            # Check for ignored selectors
            if s in ignore_sels:
                self._metadata_report[f'{selector},{s}'] = {
                    'type': 'ignored'
                }
                continue
                
            # Check for correct selector order
            if not misordered:
                if test_set[order] != s:
                    misordered = True

            # Find selectors missing from the test set
            try:
                test_s = test_set[test_set.index(s)]
            except ValueError:
                # Selector missing from test set
                self._metadata_report[f'{selector},{s}'] = {
                    'type':'missing',
                    'info':f'missing from {self._labels[0]}'
                }
                continue

            # Check equality for attributes
            self._validate_attrs(
                [
                    self._datasets[0][s].attrs,
                    self._datasets[1][s].attrs
                ], 
                source=s, ignore=ignore_attrs[s]
            )

        # Find selectors missing from the control set
        missing_from_control = set(test_set).difference(control_set)
        for mc in missing_from_control:
            self._metadata_report[f'{selector},{mc}'] = {
                'type':'missing',
                'info':f'missing from {self._labels[1]}'
            }
        
        setattr(self, selector, set(all_selectors))

        # Set the selector here for further operations.

        if misordered:
            self._metadata_report[f'{selector},all_{selector}'] = {
                'type': 'order'
            }

            self.logger.warning(
                f'{s} present in a different order between datasets - this has been recorded'
            )

    def validate_global_attrs(self, allowances: dict = None):
        """
        Validate the set of global attributes across all datasets
        """

        allowances = allowances or {}
        ignore = []
        if 'ignore' in allowances:
            ignore = allowances['ignore']

        ignore.append('aggregation')

        attrset = []
        for d in self._datasets:
            attrset.append(d.attrs)

        self.logger.info('Checking global attributes:')
        self._validate_attrs(attrset, source='global', ignore=ignore)

    def _validate_attrs(self, attrset: list[dict], source: str = '', ignore: list = None):
        """
        Ensure all values across the sets of attributes are consistent - add results
        to the metadata report.
        """

        ignore = ignore or []
        for attr in attrset[0].keys():

            self.logger.debug(f'  > {attr}')
            # Check for ignored attributes
            if attr in ignore:
                self._metadata_report[f'attributes,{source},{attr}'] = {
                    'type': 'ignored'
                }
                continue

            # Check for missing attributes in any of the sets
            try:
                set_of_values = []
                for index, aset in enumerate(attrset):
                    set_of_values.append(aset[attr])
            except KeyError:
                self._metadata_report[f'attributes,{source},{attr}'] = {
                    'type': 'missing',
                    'info': f'missing from {self._labels[index]}'
                }
                continue
                
            # Check for non-equal attributes
            s = set_of_values[0]
            if not np.all(s == np.array(set_of_values[1:])):
                self._metadata_report[f'attributes,{source},{attr}'] = {
                    'type': 'not_equal'
                }

    def validate_data(self, dim_mid: Union[dict,None] = None):
        """
        Perform data validations using the growbox method for all variable DataArrays.
        """

        if self.variables is None:
            raise ValueError(
                'Unable to validate data, please ensure metadata has been validated first.'
                'Use `validate_metadata()` method.'
            )
        
        # Reset for new report run
        self._data_report = Report()
        self.logger.info('Initialised data report')

        if not self.decoded_times:
            self.logger.warning('Validating without decoding times.')
            self._data_report['time_encoding_mismatch'] = True

        for dim in self.dims:
            self.logger.debug(f'Validating size of {dim}')

            try:
                testdim = self.test_dataset_var(dim)
            except KeyError:
                self.logger.warning(f'{dim} could not be validated for data content')
                continue

            try:
                controldim = self.control_dataset_var(dim)
            except KeyError:
                self.logger.warning(f'{dim} could not be validated for data content')
                continue

            self._validate_dimlens(
                dim,
                testdim.size,
                controldim.size
            )

            self._validate_dimvalues(
                dim,
                testdim,
                controldim
            )

        for var in self.variables:

            if var in self.bypass_vars:
                self._data_report[f'variables,skipped,{var}'] = {
                    'type':'singled-out'
                }
                continue

            self.logger.debug(f'Validating shapes for {var}')
            try:
                testvar = self.test_dataset_var(var)
            except KeyError:
                self.logger.warning(f'{var} could not be validated for data content')
                continue

            try:
                controlvar = self.control_dataset_var(var)
            except KeyError:
                self.logger.warning(f'{var} could not be validated for data content')
                continue

            self._validate_shapes(var, testvar, controlvar)

            # Check access to the source data somehow here
            # Initiate growbox method - recursive increasing box size.
            self.logger.debug(f'Validating data for {var}')
            
            if len(testvar.dims) == 0:
                # Single attempt allowed
                current = 2
            else:
                # 100 or less if the largest dimension is smaller than this limit.
                # Means we don't try growboxes of equal size too many times.
                current = min(100, max([testvar[d].size for d in testvar.dims])/2)
            
            self._validate_selection(var, testvar, controlvar, dim_mid=dim_mid, current=current)

    def _validate_shapes(self, var: str, test, control, ignore=None):
        """
        Ensure all variable shapes are consistent across all datasets.

        Allowances dict contains configurations for skipping some shape tests
        in the case for example of a virtual dimension.
        """
        ignore = ignore or []

        # Check sizes against ignore list
        if 'size' not in ignore:
            if test.size != control.size:
                # Size error
                self._data_report[f'variables,size_errors,{var}'] = {
                    self._labels[0]: test.size,
                    self._labels[1]: control.size
                }

        if 'dtype' not in ignore:
            if test.dtype != control.dtype:
                # Dtype issue - possibly due to conversion
                self._data_report[f'variables,dtype/precision,{var}'] = {
                    self._labels[0]: test.dtype,
                    self._labels[1]: control.dtype
                }

        # Check dimensions individually

        ignore_dims = []
        if 'dims' in ignore:
            ignore_dims = ignore['dims']

        test_dr, control_dr = [],[]
        dim_error = False

        # Check for consistency of number of dimensions
        if len(test.dims) != len(control.dims):
            self._data_report[f'variables,dim_errors,{var}'] = {
                    self._labels[0]: test.dims,
                    self._labels[1]: control.dims
                }
            self.logger.warning(
                f'Dimensions inconsistent for {var} - this has been reported'
            )
            return

        # Check each dimension, filter for ignores or matching sizes
        for i in range(len(control.sizes)):
            if i in ignore_dims:
                test_dr.append('i')
                control_dr.append('i')
                continue

            if test.shape[i] != control.shape[i]:
                test_dr.append(str(test.shape[i]))
                control_dr.append(str(control.shape[i]))
                dim_error = True
            else:
                test_dr.append(':')
                control_dr.append(':')
        
        # Record error if present
        if dim_error:
            self._data_report[f'variables,dim_size_errors,{var}'] = {
                    self._labels[0]: ','.join(test_dr),
                    self._labels[1]: ','.join(control_dr)
                }

    def _validate_dimvalues(self, dim: str, testdim: xr.DataArray, controldim: xr.DataArray, ignore=None):
        """
        Validate that the first and last values of the dimension arrays are equal.

        :param dim:         (str) The name of the current dimension.

        :param testdim:        (obj) The cloud-format dimension array - as of 1.4.3 
            validation applies to the entire array.

        :param controldim:     (obj) The native-format dimension array - as of 1.4.3 
            validation applies to the entire array.

        :param ignore:      (bool) Option to ignore specific dimension.
        """
        if ignore:
            self.logger.debug(f'Skipped {dim}')
            return
        
        try:
            equal = np.array_equal(testdim.compute(), controldim.compute(),equal_nan=True)
        except TypeError:
            try: 
                equal = np.array_equal(testdim.compute(), controldim.compute())
            except TypeError:
                equal = False
        # Compare array values.
        if not equal:

            self._data_report[f'dimensions,data_errors,{dim}'] = {
                    self._labels[0]: testdim.compute()[0],
                    self._labels[1]: controldim.compute()[0]
                }            

    def _validate_dimlens(self, dim: str, test, control, ignore=None):
        """
        Validate dimension lengths are consistent

        :param dim:     (str) The name of the current dimension.

        :param test:        (obj) The cloud-format (Kerchunk) dataset selection

        :param control:     (obj) The native dataset selection

        :param ignore:      (bool) Option to ignore specific dimension.
        """
        if ignore:
            self.logger.debug(f'Skipped {dim}')
            return
        
        if test != control:
            self._data_report[f'dimensions,size_errors,{dim}'] = {
                    self._labels[0]: test,
                    self._labels[1]: control
                }
        
    def _validate_selection(
            self,
            var: str,
            test: xr.DataArray,
            control: xr.DataArray,
            current : int = 100,
            recursion_limit : int = 1, 
            dim_mid: Union[dict,None] = None,
        ) -> bool:
        """
        General purpose validation for a specific variable from multiple sources.
        Both inputs are expected to be xarray DataArray objects but the control could
        instead by a NetCDF4 Dataset object. We expect both objects to be of the same size.

        :param var:           (str) The name of the variable described by this box selection

        :param test:            (obj) The cloud-format (Kerchunk) dataset selection

        :param control:         (obj) The native dataset selection
        """
        if test.size != control.size:
            self.logger.error(
                'Validation could not be completed for these objects due to differing '
                f'sizes - "{test.size}" and "{control.size}"'
            )
            return

        if current <= recursion_limit:
            self.logger.debug('Maximum recursion depth reached')
            self.logger.info(f'Validation for {var} not performed')

            self._data_report[f'variables,data_errors,{var}'] = {
                'type':'grow_box_exceeded'
            }
            return None
        
        slice_applied = slice_all_dims(test, current, dim_mid=dim_mid)
        self.logger.debug(f'Applying slice {slice_applied} to {var}')
        tbox = test[slice_applied]
        cbox = control[slice_applied]

        if check_for_nan(cbox, BypassSwitch(), self.logger, label=var):
            return self._validate_selection(var, test, control, current-1, recursion_limit=recursion_limit, dim_mid=dim_mid)
        else:
            return self._compare_data(var, slice_applied, tbox, cbox)

    def _compare_data(
        self, 
        vname: str, 
        slice_applied: list[slice],
        test: xr.DataArray, 
        control: xr.DataArray,
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
        self.logger.info(f'Starting data comparison for {vname}')

        self.logger.debug('1. Flattening Arrays')
        t1 = datetime.now()

        data_errors, bypassed = [], []

        ### --- Array Flattening --- ##
        try:
            control   = np.array(control).flatten()
            test      = np.array(test).flatten()
        except Exception as err:
            self.logger.error('Failed to flatten numpy arrays')
            raise err

        if len(slice_applied) == 0:
            slice_applied = [slice(0, len(control))]
        start, stop = format_slice(slice_applied)

        ### --- Tolerance Calculation --- ###

        self.logger.debug(f'2. Calculating Tolerance - {(datetime.now()-t1).total_seconds():.2f}s')
        try: # Tolerance 0.1% of mean value for xarray set
            tolerance = np.abs(np.nanmean(test))/1000
        except TypeError: # Type cannot be summed so skip all summations
            tolerance = None

        ### --- Equality Comparison: with tolerance --- ###

        self.logger.debug(f'3a. Comparing with all_close - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            is_close = np.allclose(control, test, atol=tolerance, equal_nan=True)
        except TypeError as err:
            try:
                is_close = np.allclose(control, test, atol=tolerance)
            except TypeError:
                self._data_report[f'variables,bypassed,{vname}'] = 'non-comparable'
                self.logger.info(f'Data validation skipped for {vname} - non-comparable')
                return
            
        ### --- Equality Comparison: without tolerance --- ###

        self.logger.debug(f'3b. Comparing with array_equal - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            equality = np.allclose(control, test, atol=tolerance, equal_nan=True)
        except TypeError as err:
            try:
                equality = np.allclose(control, test, atol=tolerance)
            except TypeError:
                self._data_report[f'variables,bypassed,{vname}'] = 'non-comparable'
                self.logger.info(f'Data validation skipped for {vname} - non-comparable')
                return

        if not equality:
            if not is_close:
                data_errors.append('not_equal')
            else:
                data_errors.append('precision_error')

        ### --- Max/Min/Mean Comparisons --- ###
        
        self.logger.debug(f'4. Comparing Max values - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            if np.abs(np.nanmax(test) - np.nanmax(control)) > tolerance:
                self.logger.warning(f'Failed maximum comparison for {vname}')
                self.logger.debug('K ' + str(np.nanmax(test)) + ' N ' + str(np.nanmax(control)))
                data_errors.append('max_not_equal')
        except TypeError as err:
            self.logger.warning(f'Max comparison skipped for non-summable values in {vname}')
            bypassed.append('max')

        self.logger.debug(f'5. Comparing Min values - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            if np.abs(np.nanmin(test) - np.nanmin(control)) > tolerance:
                self.logger.warning(f'Failed minimum comparison for {vname}')
                self.logger.debug('K ' + str(np.nanmin(test)) + ' N ' + str(np.nanmin(control)))
                data_errors.append('min_not_equal')
        except TypeError as err:
            self.logger.warning(f'Min comparison skipped for non-summable values in {vname}')
            bypassed.append('min')

        self.logger.debug(f'6. Comparing Mean values - {(datetime.now()-t1).total_seconds():.2f}s')
        try:
            if np.abs(np.nanmean(test) - np.nanmean(control)) > tolerance:
                self.logger.warning(f'Failed mean comparison for {vname}')
                self.logger.debug('K ' + str(np.nanmean(test)) + ' N ' + str(np.nanmean(control)))
                data_errors.append('mean_not_equal')
        except TypeError as err:
            self.logger.warning(f'Mean comparison skipped for non-summable values in {vname}')
            bypassed.append('mean')

        if data_errors:
            # 1.3.5 Error bypass
            if test.size == 1:
                self.logger.warning(f'1.3.5 Warning: 1-dimensional value difference for {vname} - skipped')
                self._data_report[f'variables,bypassed,{vname}'] = '1D-nan'
            else:
                self._data_report[f'variables,data_errors,{vname}'] = {
                    'type':','.join(data_errors),
                    'topleft':start,
                    'bottomright':stop,
                }
        if bypassed:
            self._data_report[f'variables,bypassed,{vname}'] = ','.join(bypassed)

        self.logger.info(f'Data validation complete for {vname}')

class ValidateOperation(ProjectOperation):
    """
    Encapsulate all validation testing into a single class. Instantiate for a specific project,
    the object could then contain all project info (from detail-cfg) opened only once. Also a 
    copy of the total datasets (from native and cloud sources). Subselections can be passed
    between class methods along with a variable index (class variables: variable list, dimension list etc.)

    Class logger attribute so this doesn't need to be passed between functions.
    Bypass switch contained here with all switches.
    """
    def __init__(
            self, 
            proj_code,
            workdir,
            parallel: bool = False,
            **kwargs):
        """
        No current validate-specific parameters
        """

        self.phase = 'validate'
        super().__init__(proj_code, workdir, **kwargs)
        if parallel:
            self.update_status(self.phase, 'Pending',jobid=self._logid)

    def _run(
            self,
            mode: str = 'kerchunk',
            dim_mid: Union[dict,None] = None,
            error_bypass: Union[dict,str,None] = None,
            **kwargs
        ) -> None:
        """
        Run hook for project operation run method

        :param mode:    (str) Cloud format to use, overriding the known cloud format from 
            previous steps.
        """
        self.set_last_run(self.phase, timestamp())
        self.logger.info("Starting validation")

        self.logger.debug(f"Error bypass: {bool(error_bypass)}")

        if mode != self.cloud_format and mode is not None:
            self.cloud_format = mode

        test       = self.dataset.open_dataset()
        sample, rf = self._open_sample()

        self.logger.info(f'Using sample {rf} along aggregated dimension')

        meta_fh = JSONFileHandler(self.dir, 'metadata_report',logger=self.logger, **self.fh_kwargs)
        data_fh = JSONFileHandler(self.dir, 'data_report',logger=self.logger, **self.fh_kwargs)

        bypass_vars = self.base_cfg.get('validation',{}).get('bypass',[])

        concat_dims = self.detail_cfg.get('kwargs',{}).get('combine_kwargs',{}).get('concat_dims',None)

        vd = ValidateDatasets(
            [test,sample],
            f'validator-padocc-{self.proj_code}',
            dataset_labels=[self.cloud_format, self.source_format], 
            filehandlers=[meta_fh, data_fh],
            logger=self.logger,
            bypass_vars=bypass_vars,
            error_bypass=error_bypass,
            concat_dims=concat_dims)

        # Run metadata testing
        vd.validate_metadata()

        # Data Validation Selection

        if self.cfa_enabled and self.cloud_format != 'CFA':
            self.logger.info('CFA-enabled validation')
            # CFA now opens with decoded times (2025.8.4)
            try:
                control = self._open_cfa()
                vd.replace_dataset(control, label=self.source_format)
            except:
                # CFA has failed for some reason - file must be deleted.
                self.cfa_enabled = False

        if self.cfa_enabled and self.cloud_format != 'CFA':
            # Run single validation attempt
            vd.validate_data(dim_mid=dim_mid)
        else:

            filetests = [0]
            nfiles = len(self.allfiles.get())
            if self.allfiles[0] != self.allfiles[-1]:
                filetests.append(nfiles-1)
            if nfiles > 2:
                # Get random file
                filetests.append(None)

            checks = len(filetests)
            for check, rf in enumerate(filetests):
                vd = self._run_data_validation(test, rf, check, checks, vd, dim_mid)

        err = self.get_agg_shorthand() + (vd.save_report() or 'Success')

        self.update_status('validate', err, jobid=self._logid)
        
        return vd.pass_fail(err)
    
    def _run_data_validation(self, test: xr.Dataset, rf: int, check: int, checks: int, vd: ValidateDatasets, dim_mid):
        """
        Prepare and run for a single validation attempt.
        """

         # Open a random file or as specified above.
        sample, rfnum = self._open_sample(rf=rf)
        vd.replace_dataset(sample, label=self.source_format)

        decode_times = vd.decode_times_ok()

        # Time encoding mismatch
        if not decode_times:
            test   = self.dataset.open_dataset(decode_times=decode_times)
            vd.replace_dataset(test, label=self.cloud_format)
            sample, rf = self._open_sample(decode_times=decode_times)
            vd.replace_dataset(sample, label=self.source_format)

        self.logger.info(f'Source-slice validation: {check+1}/{checks} using file {rfnum}')
        preslice = self._get_preslice(test, sample, test.variables, rf=rf)
        vd.replace_preslice(preslice, label=self.cloud_format)

        self.logger.debug('Slicing using preslice selections:')
        slice_set = "\n -".join(preslice._preslice_set)
        self.logger.debug(f' - {slice_set}')

        vd.validate_data(dim_mid=dim_mid)

        return vd

    def _open_sample(self, rf: Union[int,None] = None, **kwargs) -> tuple:
        """
        Open a random sample dataset for validation checking.
        """
        if rf is not None:
            file = self.allfiles[rf]
            randomfile = rf
        else:
            randomfile = random.randint(0,len(self.allfiles)-1)
            file = self.allfiles[randomfile]

        xarray_kwargs = self._xarray_kwargs | kwargs
        return xr.open_dataset(file, **xarray_kwargs), randomfile

    def _open_cfa(self, **kwargs):
        """
        Open the CFA dataset for this project
        """
        return self.cfa_dataset.open_dataset(**kwargs)

    def _get_preslice(self, test, sample, variables, rf:int = 0):
        """Match timestamp of xarray object to kerchunk object.
        
        :param test:     (obj) An xarray dataset representing the cloud product.
        
        :param sample:   (obj) An xarray dataset representing the source file(s).
        
        :returns:   A slice object to apply to the test dataset to map directly
            to the sample dataset.
        """

        virtual = self.detail_cfg.get('virtual_concat',False)
        # Use rf to squeeze non-present dimensions

        preslice = PresliceSet(self.logger)
        for var in variables:
            preslice_var = {}

            if virtual:
                if var == 'file_number':
                    # Skip the virtual dimension
                    continue

            dim_diffs = set(test[var].dims) - set(sample[var].dims)
            self.logger.debug(f'Test dims: {set(test[var].dims)}')
            self.logger.debug(f'Sample dims: {set(sample[var].dims)}')

            for dim in sample[var].dims:

                if len(sample[dim]) < 2:
                    # Non-coordinate dimensions with no axis.

                    dim_array = np.array(test[dim])
                    index = np.where(dim_array == np.array(sample[dim][0]))[0][0]
                    stop = index + 1
                    pos0 = index #np.array(test[dim][index], dtype=test[dim].dtype)
                    end = stop # np.array(test[dim][stop], dtype=test[dim].dtype)

                else:
                    # Source Slice validation for coodinate dimensions

                    # Index of the 0th sample dim value in the test dim array
                    matching_value = np.where(test[dim] == sample[dim][0])[0]
                    if matching_value.size == 0:
                        raise ValidationError(
                            'Fatal dimension mismatch - '
                            f'cannot align sample section with test dataset for {dim}')
                    index0 = matching_value[0]

                    pos0 = index0
                    end  = index0 + len(sample[dim])

                slice_dim = slice(
                    pos0,
                    end
                )
                
                preslice_var[dim] = slice_dim

            # Covers virtual dimensions
            if virtual and 'file_number' in test[var].dims:
                preslice_var['file_number'] = slice(rf,rf+1)

            # Covers missing dimensions (inc. virtual)
            for dim in dim_diffs:
                preslice_var[dim] = (slice(rf,rf+1), dim)
            
            preslice.add_preslice(preslice_var, var)

        return preslice
