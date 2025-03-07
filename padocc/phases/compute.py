__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import base64
import json
import logging
import os
from datetime import datetime
from typing import Optional, Union

import fsspec
import numpy as np
import rechunker
import xarray as xr

from padocc.core import FalseLogger, LoggedOperation, ProjectOperation
from padocc.core.errors import (ComputeError, ConcatFatalError,
                                KerchunkDriverFatalError, PartialDriverError,
                                SourceNotFoundError)
from padocc.core.filehandlers import JSONFileHandler, ZarrStore
from padocc.core.utils import find_closest, make_tuple, timestamp
from padocc.phases.validate import ValidateDatasets

CONCAT_MSG = 'See individual files for more details'    

class KerchunkConverter(LoggedOperation):
    """Class for converting a single file to a Kerchunk reference object. Handles known
    or unknown file types (NetCDF3/4 versions)."""

    description = 'Single-file Kerchunk converter class.'
    def __init__(
            self,
            logger=None, 
            bypass_driver=False,  
            verbose=1,
            label=None,
            fh=None,
            logid=None) -> None:

        self.success       = True
        self._bypass_driver = bypass_driver
        self.loaded_refs   = False

        self.ctype = None

        self.drivers = {
            'ncf3': self._ncf3_to_zarr,
            'hdf5': self._hdf5_to_zarr,
            'tif' : self._tiff_to_zarr,
            'grib': self._grib_to_zarr,
        }

        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose
        )

    def run(self, nfile: str, filehandler=None, extension=None, **kwargs) -> dict:
        """
        Safe creation allows for known issues and tries multiple drivers

        :returns:   dictionary of Kerchunk references if successful, raises error
                    otherwise if unsuccessful.
        """

        if not os.path.isfile(nfile):
            raise SourceNotFoundError(sfile=nfile)

        supported_extensions = [ext for ext in list(self.drivers.keys()) if ext != extension]

        tdict = None
        if extension:
            tdict = self._convert_kerchunk(nfile, extension, **kwargs)
            ctype = extension

        if not tdict:
            for ctype in supported_extensions:
                tdict = self._convert_kerchunk(nfile, ctype, **kwargs)
                if tdict:
                    self.logger.debug(f'Scan successful with {ctype} driver')
                    break

        if not tdict:
            self.logger.error('Scanning failed for all drivers, file type is not Kerchunkable')
            raise KerchunkDriverFatalError
        
        if filehandler:
            filehandler.set(tdict)
            filehandler.close()

        return tdict, ctype

    def _convert_kerchunk(self, nfile: str, ctype, **kwargs) -> None:
        """
        Perform conversion to zarr with exceptions for bypassing driver errors.

        :param nfile:           (str) Path to a local native file of an appropriate
                                type to be converted.

        :param ctype:           (str) File extension relating to file type if known.
                                All extensions/drivers will be tried first, subsequent
                                files in the same dataset will use whatever extension
                                worked for the first file as a starting point.

        :returns:               The output of performing a driver if successful, None
                                if the driver is unsuccessful. Errors will be bypassed
                                if the bypass_driver option is selected for this class.
        """
        
        self.logger.debug(f'Attempting conversion using "{ctype}" driver')
        try:
            if ctype in self.drivers:
                ref = self.drivers[ctype](nfile, **kwargs)
                return ref
            else:
                self.logger.debug(f'Extension {ctype} not valid')
                return None
        except Exception as err:
            if self._bypass_driver:
                return None
            else:
                raise err
                    
    def _hdf5_to_zarr(self, nfile: str, **kwargs) -> dict:
        """Wrapper for converting NetCDF4/HDF5 type files to Kerchunk"""
        from kerchunk.hdf import SingleHdf5ToZarr
        return SingleHdf5ToZarr(nfile,**kwargs).translate()

    def _ncf3_to_zarr(self, nfile: str, **kwargs) -> dict:
        """Wrapper for converting NetCDF3 type files to Kerchunk"""
        from kerchunk.netCDF3 import NetCDF3ToZarr
        return NetCDF3ToZarr(nfile, **kwargs).translate()

    def _tiff_to_zarr(self, tfile: str, **kwargs) -> dict:
        """Wrapper for converting GeoTiff type files to Kerchunk"""
        from kerchunk.tiff import TiffToZarr
        return TiffToZarr(tfile, **kwargs).translate()
    
    def _grib_to_zarr(self, gfile: str, **kwargs) -> dict:
        """Wrapper for converting GRIB type files to Kerchunk"""
        from kerchunk.grib2 import GribToZarr
        return GribToZarr(gfile, **kwargs).translate()

class ComputeOperation(ProjectOperation):
    """
    PADOCC Dataset Processor Class, capable of processing a single
    dataset's worth of input files into a single aggregated file/store.
    """
    
    def __init__(
            self, 
            proj_code : str, 
            workdir   : str,
            groupID   : str = None,
            stage     : str = 'in_progress',
            thorough    : bool = None,
            concat_msg  : str = CONCAT_MSG,
            limiter     : int = None, 
            skip_concat : bool = False, 
            label : str = 'compute',
            is_trial: bool = False,
            **kwargs
        ) -> None:
        """
        Initialise KerchunkDSProcessor for this dataset, set all variables and prepare 
        for computation.
        
        :param proj_code:       (str) The project code in string format (DOI)

        :param workdir:         (str) Path to the current working directory.

        :param groupID:         (str) GroupID of the parent group.

        :param thorough:        (bool) From args.quality - if True will create all files 
            from scratch, otherwise saved refs from previous runs will be loaded.
        
        :param concat_msg:      (str) Value displayed as global attribute for any attributes 
            that differ across the set of files, instead of a list of the differences,
            this message will be used, default can be found above.

        :param limiter:         (int) Number of files to process from the whole set of files. 
            Default value of None will mean all files are processed. Any non-None value will 
            limit the number of files for processing - utilised in 'scan' phase.

        :param skip_concat:     (bool) Internal parameter for skipping concat - used for parallel 
            construction which requires a more complex job allocation.

        :param new_version:

        :returns: None

        """
        self.phase = 'compute'

        super().__init__(
            proj_code, 
            workdir, 
            groupID=groupID,
            thorough=thorough,
            label=label,
            **kwargs)
        
        self._is_trial = is_trial

        self.logger.debug('Starting variable definitions')

        self.concat_msg  = concat_msg
        self.skip_concat = skip_concat

        self.stage = stage
        self.mode = self.cloud_format

        self.validate_time = None
        self.concat_time   = None
        self.convert_time  = None

        self._data_properties = None

        self.updates, self.removals = False, False

        self.loaded_refs      = False
        self.quality_required = False

        num_files = len(self.allfiles)

        self.partial = (limiter and num_files != limiter)

        # Perform this later
        #self._determine_version()

        self.limiter = limiter
        if not self.limiter:
            self.limiter = num_files

        self._setup_cache(self.dir)

        self.temp_zattrs = JSONFileHandler(
            self.cache, 
            'temp_zattrs',
            logger=self.logger,
            dryrun=self._dryrun,
            forceful=self._forceful
        )

        if thorough:
            self.temp_zattrs.set({})

        self.combine_kwargs = {} # Now using agg_dims and identical dims finders.
        self.create_kwargs  = {'inline_threshold':0}
        self.pre_kwargs     = {}

        self.special_attrs = {}
        self.var_shapes    = {}

        self.logger.debug('Finished all setup steps')

    def help(self, fn=print):
        super().help(fn=fn)
        fn('')
        fn('Compute Options:')
        fn(' > project.run() - Run compute for this project')

    @property
    def extra_properties(self):
        return self._data_properties
    
    @property
    def extra_kwargs(self):
        return {
            'combine_kwargs': self.combine_kwargs,
            'create_kwargs': self.create_kwargs,
            'pre_kwargs': self.pre_kwargs,
        }

    def _run(
            self, 
            mode: str = 'CFA',
            file_limit: Union[int,None] = None,
        ) -> str:
        """
        Default _run hook for compute operations. A user should aim to use the
        configuration options to use the Kerchunk or Zarr DS classes rather than
        the bare parent class. Override this class with a _run parameter for new 
        DS classes (COG etc.)
        
        This class now defaults to create the CFA dataset,
        rather than having a separate class for this.
        """
        if mode != 'CFA':
            raise ValueError(
                '`ComputeOperation` default run option uses CFA - you '
                f'have specified to use {mode} which requires the use '
                'of a different `DS` operator (Kerchunk/Zarr supported).'
            )
        
        if file_limit == len(self.allfiles):
            file_limit = None

        results = self._run_cfa(file_limit=file_limit)

        if results is None:
            return 'Fatal'
        
        self.base_cfg['data_properties'] = results
        self.base_cfg.close()

        # Check results values
        success = len(results.keys()) > 0
        for s in results.values():
            if s == 'Unknown':
                success = False
        
        if success:
            self.detail_cfg['CFA'] = True
            self.detail_cfg.close()
            return 'Success'
        
        return 'Fatal'

    def _run_cfa(
            instance: type[ProjectOperation], 
            file_limit: Union[int,None] = None,
        ) -> Union[dict,None]:

        """
        Handle the creation of a CFA-netCDF file using the CFAPyX package

        :param instance:    (obj) The reference instance of ProjectOperation 
            from which to pull project-specific info.

        :param file_limit:  (obj) The file limit to apply to a set of files.
        """
        try:
            from cfapyx import CFANetCDF

        except ImportError:
            return False

        try:

            instance.logger.info("Starting CFA Computation")

            files = instance.allfiles.get()
            if file_limit is not None:
                files = files[:file_limit]

            cfa = CFANetCDF(files) # Add instance logger here.

            cfa.create()
            if file_limit is None:
                cfa.write(instance.cfa_path + '.nca')

            return {
                'aggregated_dims': make_tuple(cfa.agg_dims),
                'pure_dims': make_tuple(cfa.pure_dims),
                'coord_dims': make_tuple(cfa.coord_dims),
                'aggregated_vars': make_tuple(cfa.aggregated_vars),
                'scalar_vars': make_tuple(cfa.scalar_vars),
                'identical_vars': make_tuple(cfa.identical_vars)
            }

        except Exception as err:
            instance.logger.error(
                f'Aggregation via CFA failed - {err} - report at https://github.com/cedadev/CFAPyX/issues'
            )
            return None

    def _run_with_timings(self, func, **kwargs) -> str:
        """
        Configure all required steps for Kerchunk processing.


        Check if output files already exist and configure 
        timings post-run.
        """

        # Timed func call
        t1 = datetime.now()
        func(**kwargs)
        compute_time = (datetime.now()-t1).total_seconds()

        timings      = self._get_timings()
        detail       = self.detail_cfg.get()

        if not detail.get('timings',None):
            detail['timings'] = {}

        if timings:
            self.logger.info('Export timings for this process - all refs created from scratch.')
            detail['timings']['convert_actual'] = timings['convert_actual']
            
            if 'concat_actual' in timings:
                detail['timings']['concat_actual']  = timings['concat_actual']
            detail['timings']['compute_actual'] = compute_time

        self.detail_cfg.set(detail)
        self.detail_cfg.close()
        return 'Success'

    def save_files(self):
        super().save_files()
        self.temp_zattrs.close()

    @property
    def filelist(self):
        """
        Quick function for obtaining a subset of the whole fileset. Originally
        used to open all the files using Xarray for concatenation later.
        """
        if self.limiter < len(self.allfiles):
            self.logger.debug(f'Opening a limited set of {self.limiter} files')

        return self.allfiles[:self.limiter]

    def _determine_version(self):
        if self._forceful:
            return
        
        found_space = False
        while not found_space:

            if os.path.isfile(self.outpath) or os.path.isdir(self.outpath):
                if False:
                    self.minor_version_increment()
                else:
                    raise ValueError(
                        'Output product already exists and there is no plan to overwrite or create new version'
                    )
            else:
                found_space = True

    def _get_timings(self) -> dict:
        """
        Export timed values if refs were all created from scratch.
        Ref loading invalidates timings so returns None if any refs were loaded
        not created - common class method for all conversion types.

        :returns:   Dictionary of timing values if successful and refs were not loaded. 
                    If refs were loaded, timings are invalid so returns None.
        """
        timings = None
        if not self.loaded_refs:
            timings = {
                'convert_actual': self.convert_time,
                'concat_actual' : self.concat_time
            }
        return timings

    def _collect_details(self) -> dict:
        """
        Collect kwargs for combining and any special attributes - save to detail file.
        Common class method for all conversion types.
        """
        detail = self.detail_cfg.get()
        detail['combine_kwargs'] = self.combine_kwargs
        if self.special_attrs:
            detail['special_attrs'] = list(self.special_attrs.keys())

        detail['quality_required'] = self.quality_required
        self.detail_cfg.set(detail)

    def _clean_attr_array(self, allzattrs: dict) -> dict:
        """
        Collect global attributes from all refs:
        - Determine which differ between refs and apply changes

        This Class method is common to all zarr-like conversion types.
        """

        base = json.loads(allzattrs[0])

        self.logger.debug('Correcting time attributes')
        # Sort out time metadata here
        times = {}
        all_values = {}

        # Global attributes with 'time' in the name i.e start_datetime
        for k in base.keys():
            if 'time' in k:
                times[k] = [base[k]]
            all_values[k] = []

        nonequal = {}
        # Compare other attribute sets to a starting set 0
        for ref in allzattrs[1:]:
            zattrs = json.loads(ref)
            for attr in zattrs.keys():
                # Compare each attribute.
                if attr in all_values:
                    all_values[attr].append(zattrs[attr])
                else:
                    all_values[attr] = [zattrs[attr]]
                if attr in times:
                    times[attr].append(zattrs[attr])
                elif attr not in base:
                    nonequal[attr] = False
                else:
                    if base[attr] != zattrs[attr]:
                        nonequal[attr] = False

        # Requires something special for start and end times
        base = {**base, **self._check_time_attributes(times)}
        self.logger.debug('Comparing similar keys')

        for attr in nonequal.keys():
            if len(set(all_values[attr])) == 1:
                base[attr] = all_values[attr][0]
            else:
                base[attr] = self.concat_msg
                self.special_attrs[attr] = 0

        self.logger.debug('Finished checking similar keys')
        return base

    def _clean_attrs(self, zattrs: dict) -> dict:
        """
        Ammend any saved attributes post-combining
        - Not currently implemented, may be unnecessary

        This Class method is common to all zarr-like conversion types.
        """
        self.logger.warning('Attribute cleaning post-loading from temp is not implemented')
        return zattrs

    def _check_time_attributes(self, times: dict) -> dict:
        """
        Takes dict of time attributes with lists of values
        - Sort time arrays
        - Assume time_coverage_start, time_coverage_end, duration (2 or 3 variables)

        This Class method is common to all zarr-like conversion types.
        """
        combined = {}
        for k in times.keys():
            if 'start' in k:
                combined[k] = sorted(times[k])[0]
            elif 'end' in k or 'stop' in k:
                combined[k]   = sorted(times[k])[-1]
            elif 'duration' in k:
                pass
            else:
                # Unrecognised time variable
                # Check to see if all the same value
                if len(set(times[k])) == len(self.allfiles):
                    combined[k] = 'See individual files for details'
                elif len(set(times[k])) == 1:
                    combined[k] = times[k][0]
                else:
                    combined[k] = list(set(times[k]))

        self.logger.debug('Finished time corrections')
        return combined

    def _correct_metadata(self, allzattrs: dict) -> dict:
        """
        General function for correcting metadata
        - Combine all existing metadata in standard way (cleaning arrays)
        - Add updates and remove removals specified by configuration

        This Class method is common to all zarr-like conversion types.
        """

        self.logger.debug('Starting metadata corrections')
        if type(allzattrs) == list:
            zattrs = self._clean_attr_array(allzattrs)
        else:
            zattrs = self._clean_attrs(allzattrs)
        self.logger.debug('Applying config info on updates and removals')

        if self.updates:
            for update in self.updates.keys():
                zattrs[update] = self.updates[update]
        new_zattrs = {}
        if self.removals:
            for key in zattrs:
                if key not in self.removals:
                    new_zattrs[key] = zattrs[key]
        else:
            new_zattrs = zattrs # No removals required

        self.logger.debug('Finished metadata corrections')
        if not new_zattrs:
            self.logger.error('Lost zattrs at correction phase')
            raise ValueError
        return new_zattrs

    def _dims_via_cfa(self):
        """
        Obtain dimension info from the report generated on running
        the CFA process.
        """
    
        report = {k: tuple(v) for k, v in self.base_cfg['data_properties'].items()} 

        concat_dims = report['aggregated_dims']
        identical_dims = tuple(c for c in report['coord_dims'] if c not in concat_dims)

        identicals = tuple(set(report['identical_vars'] + \
                         report['scalar_vars'] + \
                         identical_dims + \
                         report['pure_dims']))
    
        return concat_dims, identicals, report

    def _dims_via_validator(self) -> tuple[list[str]]:
        """
        Determine identical/concat dims using the validator
        """
        self.logger.info('Starting dimension determination - using Validator')

        test_files = [self.allfiles[0], self.allfiles[-1]]
        datasets   = [xr.open_dataset(t) for t in test_files]
        dimensions = datasets[0].dims

        vd = ValidateDatasets(
            datasets,
            'scan-dim-check',
            dataset_labels=('first','last'),
            logger=self.logger
        )

        vd.validate_metadata()
        vd.validate_data()

        vd.save_report(
            JSONFileHandler(
                self.dir,
                'potential_issues',
                logger=self.logger
            )
        )

        dims = vd.report['report']['data'].get('dimensions',{})

        # Non identical variables identifiable by either data errors (the data changes between files)
        # Or size errors (the array size is different - data must be different in this case.)
        derrs    = set(vd.report['report']['data'].get('data_errors',{}))
        var_errs = vd.report['report']['data'].get('variables',{})
        sizerrs  = set(var_errs.get('size_errors',{}))

        vars = derrs | sizerrs

        concat_dims = []
        if 'data_errors' in dims:
            concat_dims = [dim for dim in dims['data_errors'].keys()]

        # Concat dims will vary across files, identicals will not.

        identical_dims = [dim for dim in dimensions if dim not in vars]
        
        
        return concat_dims, identical_dims

    def _determine_dim_specs(self) -> None:
        """
        Perform identification of identical_dims and concat_dims here.
        """

        # Calculate Partial Validation Estimate here
        t1 = datetime.now()
        self.logger.info("Starting dimension specs determination")

        if 'CFA' in self.detail_cfg:
            self.logger.info('Extracting dim info from CFA report')
            concat_dims, identical_dims, report = self._dims_via_cfa()
            self._data_properties = report

        else:
            self.logger.info('CFA Determination failed - defaulting to Validator-based checks')
            concat_dims, identical_dims = self._dims_via_validator()

            self._data_properties = {
                'aggregated_dims': tuple(concat_dims),
                'coord_dims': tuple(set(concat_dims) | set(identical_dims))
            }

        self.combine_kwargs['concat_dims'] = concat_dims
        self.combine_kwargs['identical_dims'] = identical_dims

        if self.combine_kwargs['concat_dims'] == []:
            self.logger.info("No concatenation dimensions available - virtual dimension will be constructed.")
        else:
            self.logger.info(f"Found {self.combine_kwargs['concat_dims']} concatenation dimensions.")

        # Identical (Variables) Dimensions
        self.logger.info(f"Found {self.combine_kwargs['identical_dims']} identical variables.")

        # This one only happens for two files so don't need to take a mean
        self.validate_time = (datetime.now()-t1).total_seconds()

class KerchunkDS(ComputeOperation):

    def __init__(
            self, 
            proj_code,
            workdir,
            stage = 'in_progress',
            **kwargs):

        super().__init__(proj_code, workdir, stage=stage, **kwargs)
        
    def _run(
            self,
            check_dimensions: bool = False,
            ctype: Union[str,None] = None,
            **kwargs
        ) -> str:
        """
        ``_run`` hook method called from the ``ProjectOperation.run`` 
        which this subclass inherits. The kwargs capture the ``mode``
        parameter from ``ProjectOperation.run`` which is not needed 
        because we already know we're running for ``Kerchunk``.
        """

        # Run CFA in super class.
        _ = super()._run(file_limit=self.limiter)

        status = self._run_with_timings(
            self.create_refs, 
            check_refs=check_dimensions,
            ctype=ctype
        )
        
        if ctype is None:
            self.detail_cfg['driver'] = '/'.join(set(self.ctypes))

        self.update_status('compute',status,jobid=self._logid)

        return status

    def create_refs(
            self, 
            check_refs : bool = False,
            ctype: Union[str,None] = None
        ) -> None:
        """Organise creation and loading of refs
        - Load existing cached refs
        - Create new refs
        - Combine metadata and global attributes into a single set
        - Coordinate combining and saving of data"""

        self.logger.info(f'Starting computation for components of {self.proj_code}')

        refs, allzattrs = [], []
        partials = []
        ctypes = []

        ctype = self.source_format or ctype

        converter = KerchunkConverter(logger=self.logger, 
                                      bypass_driver=self._bypass.skip_driver)
        
        listfiles = self.allfiles.get()

        t1 = datetime.now()
        create_mode = False

        for x, nfile in enumerate(listfiles[:self.limiter]):

            ref = None
            ## Default Converter Type if not set.
            if ctype is None:
                ctype = list(converter.drivers.keys())[0]

            ## Connect to Cache File
            CacheFile = JSONFileHandler(self.cache, f'{x}', 
                                            dryrun=self._dryrun, forceful=self._forceful,
                                            logger=self.logger)
            
            ## Attempt to load the cache file
            if not self._thorough:
                self.logger.info(f'Attempting cache file load: {x+1}/{self.limiter}')
                ref = CacheFile.get()
                if ref:
                    self.logger.info(f' > Loaded ref')
                    create_mode = False

            ## Create cache file from scratch if needed
            if not ref:
                if not create_mode:
                    self.logger.info(' > Cache file not found: Switching to create mode')
                    create_mode = True

                self.logger.info(f'Creating refs: {x+1}/{self.limiter}')
                try:
                    ref, ctype = converter.run(nfile, extension=ctype, **self.create_kwargs)
                except KerchunkDriverFatalError as err:
                    if len(refs) == 0:
                        raise err
                    else:
                        partials.append(x)

            if not ref:
                continue
            
            allzattrs.append(ref['refs']['.zattrs'])
            refs.append(ref)

            if check_refs:
                # Perform any and all checks here if required
                refs = self._perform_shape_checks(ref)

            CacheFile.set(ref)
            CacheFile.close()
            ctypes.append(ctype)

        self.success = converter.success
        self.ctypes = ctypes

        # Compute mean conversion time for this set.
        self.convert_time = (datetime.now()-t1).total_seconds()/self.limiter

        self.loaded_refs = converter.loaded_refs

        if len(partials) > 0:
            raise PartialDriverError(filenums=partials)

        if not self.temp_zattrs.get():
            self.temp_zattrs.set(
                self._correct_metadata(allzattrs)
            )

        try:
            if self.success and not self.skip_concat:
                self._combine_and_save(refs)
        except Exception as err:
            # Any additional parts here.
            raise err

    def _combine_and_save(self, refs: dict) -> None:
        """
        Concatenation of refs data for different kerchunk schemes.

        :param refs:    (dict) The set of generated
        """

        self.logger.info('Starting concatenation of refs')
        if len(refs) > 1:
            self._determine_dim_specs()

        t1 = datetime.now()  
        if self.file_type == 'parq':
            self.logger.info('Concatenating to Parquet format Kerchunk store')
            self._data_to_parq(refs)
        else:
            self.logger.info('Concatenating to JSON format Kerchunk file')
            self._data_to_json(refs)
        self.concat_time = (datetime.now()-t1).total_seconds()/self.limiter

        if not self._dryrun:
            self._collect_details() # Zarr might want this too.
            self.logger.info("Details updated in detail-cfg.json")

    def _construct_virtual_dim(self, refs: dict) -> None:
        """
        Construct a Virtual dimension for stacking multiple files 
        where no suitable concatenation dimension is present.
        """
        # For now this just means creating a list of numbers 0 to N files
        vdim = 'file_number'

        for idx in range(len(refs)):
            ref = refs[idx]
            zarray = json.dumps({
                "chunks": [1],
                "compressor": None,
                "dtype":"<i8",
                "fill_value": 4611686018427387904,
                "filters": None,
                "order": "C",
                "shape": [1],
                "zarr_format": 2
            })
            zattrs = json.dumps({
                "_ARRAY_DIMENSIONS": [vdim],
                "axis": "F",
                "long_name": vdim,
                "standard_name": vdim
            })
            values = b"base64:" + base64.b64encode(np.array([idx]).tobytes())

            if 'refs' in ref:
                ref['refs'][f'{vdim}/.zarray'] = zarray
                ref['refs'][f'{vdim}/.zattrs'] = zattrs
                ref['refs'][f'{vdim}/0'] = values
            else:
                ref[f'{vdim}/.zarray'] = zarray
                ref[f'{vdim}/.zattrs'] = zattrs
                ref[f'{vdim}/0'] = values
        return refs, vdim

    def _data_to_parq(self, refs: dict) -> None:
        """
        Concatenating to Parquet-format Kerchunk store
        """

        from fsspec import filesystem
        from fsspec.implementations.reference import LazyReferenceMapper
        from kerchunk.combine import MultiZarrToZarr

        self.logger.debug('Starting parquet-write process')

        out = LazyReferenceMapper.create(self.record_size, str(self.kstore), fs = filesystem("file"), **self.pre_kwargs)

        out_dict = MultiZarrToZarr(
            refs,
            out=out,
            remote_protocol='file',
            **self.combine_kwargs
        ).translate()
        
        if self.partial:
            self.logger.info(f'Skipped writing to parquet store - {self.kstore}')
        else:
            out.flush()
            self.logger.info(f'Written to parquet store - {self.kstore}')

    def _data_to_json(self, refs: dict) -> None:
        """
        Concatenating to JSON-format Kerchunk file
        """
        from kerchunk.combine import MultiZarrToZarr

        self.logger.debug('Starting JSON-write process')

        # Already have default options saved to class variables
        if len(refs) > 1:
            self.logger.debug('Concatenating refs using MultiZarrToZarr')
            
            if self.detail_cfg['virtual_concat']:
                refs, vdim = self._construct_virtual_dim(refs)
                self.combine_kwargs['concat_dims'] = [vdim]

            try:
                mzz = MultiZarrToZarr(list(refs), **self.combine_kwargs).translate()
            except ValueError as err:
                if 'chunk size mismatch' in str(err):
                    raise ConcatFatalError
                else:
                    raise err

            zattrs = self.temp_zattrs.get()
            if zattrs is not None:
                mzz['refs']['.zattrs'] = json.dumps(zattrs)

            self.kfile.set(mzz)
        else:
            self.logger.debug('Found single ref to save')
            self.kfile.set(refs[0])

        self.kfile.close()

    def _perform_shape_checks(self, ref: dict) -> dict:
        """
        Check the shape of each variable for inconsistencies which will
        require a thorough validation process.
        """

        if self.source_format not in ['ncf3','hdf5']:
            self.logger.warning(
                'Skipped reference checks, source file not compatible.'
            )

        # Identify variables to be checked
        if self.base_cfg['data_properties']['aggregated_vars'] != 'Unknown':
            variables = self.base_cfg['data_properties']['aggregated_vars']
            checklist = [f'{v}/.zarray' for v in variables]
        else:
            checklist = [r for r in ref['refs'].keys() if '.zarray' in r]

        # Determine correct values from a single source file
        ## - Test opening a netcdf file and extracting the dimensions
        ## - Already checked the files are of netcdf type.

        # Perform corrections
        for key in checklist:
            zarray = json.loads(ref['refs'][key])
            if key not in self.var_shapes:
                self.var_shapes[key] = zarray['shape']

            if self.var_shapes[key] != zarray['shape']:
                self.logger.debug(
                    f'Reference Correction: {zarray["shape"]} to '
                )

class ZarrDS(ComputeOperation):

    def __init__(
            self, 
            proj_code,
            workdir,
            groupID: Union[str,None] = None,
            stage : str = 'in_progress',
            mem_allowed : str = '100MB',
            preferences = None,
            **kwargs,
        ) -> None:
        
        super().__init__(proj_code, workdir, groupID=groupID, stage=stage, **kwargs)

        self.tempstore   = ZarrStore(self.dir, "zarrcache", logger=self.logger, **self.fh_kwargs)
        self.preferences = preferences

        if self._thorough or self._forceful:
            self.tempstore.clear()

        self.mem_allowed = mem_allowed

    def _run(self, **kwargs) -> str:
        """
        Recommended way of running an operation - includes timers etc.
        """
        self.set_last_run(self.phase, timestamp())
        # Run CFA in super class.
        _ = super()._run(file_limit=self.limiter)

        status = self._run_with_timings(self.create_store)
        self.update_status('compute',status,jobid=self._logid)
        return status

    def create_store(
            self, 
            file_limit: int = None):
        """
        Create the Zarr Store
        """

        kwargs = self.detail_cfg.get('kwargs', {})
        self.combine_kwargs = kwargs.get('combine_kwargs',{})

        # Open all files for this process (depending on limiter)
        self.logger.debug('Starting timed section for estimation of whole process')
        t1 = datetime.now()

        self.logger.info(f"Retrieved required xarray dataset objects - {(datetime.now()-t1).total_seconds():.2f}s")

        if len(self.allfiles) > 1:
            
            # Determine concatenation dimensions
            if self.base_cfg['data_properties']['aggregated_vars'] == 'Unknown':
                # Determine dimension specs for concatenation.
                self._determine_dim_specs([
                    xr.open_dataset(self.allfiles[0]),
                    xr.open_dataset(self.allfiles[1])
                ])
            if not self.combine_kwargs['concat_dims']:
                self.logger.error('No concatenation dimensions - unsupported for zarr conversion')
                raise NotImplementedError

            # Perform Concatenation
            self.logger.info(f'Concatenating xarray objects across dimensions ({self.combine_kwargs["concat_dims"]})')

            if file_limit:
                fileset = self.allfiles[:file_limit]
            else:
                fileset = self.allfiles.get()

            combined_ds = xr.open_mfdataset(
                fileset, 
                combine='nested', 
                concat_dim=self.combine_kwargs['concat_dims'],
                data_vars='minimal')
            
        else:
            combined_ds = xr.open_dataset(self.allfiles[0])
        
        # Assessment values
        self.std_vars = list(combined_ds.variables)

        self.logger.info(f'Concluded object concatenation - {(datetime.now()-t1).total_seconds():.2f}s')

        concat_dim_rechunk, dim_sizes, cpf, volm = self._get_rechunk_scheme(combined_ds)
        self.cpf  = [cpf]
        self.volm = [volm]
        self.logger.info(f'Determined appropriate rechunking scheme - {(datetime.now()-t1).total_seconds():.2f}s')
        self.logger.debug(f'Sizes        : {dim_sizes}')
        self.logger.debug(f'Chunk Scheme : {concat_dim_rechunk}')
        self.logger.debug(f'CPF: {self.cpf[0]}, VPF: {self.volm[0]}, num_vars: {len(self.std_vars)}')

        self.concat_time = (datetime.now()-t1).total_seconds()/self.limiter

        for store in [self.tempstore, self.zstore]:
            if not store.is_empty:
                if self._forceful or self._thorough:
                    store.clear()
                else:
                    raise ValueError(
                        'Unable to rechunk to zarr - store already exists '
                        'and no overwrite plan has been given. Use '
                        '-f or -Q on the commandline to clear or overwrite'
                        'existing store'
                    )
        if self.base_cfg.get('rechunk',False):
    
            # Perform Rechunking
            self.logger.info(f'Starting Rechunking - {(datetime.now()-t1).total_seconds():.2f}s')
            if not self._dryrun:
                t1 = datetime.now()

                rechunker.rechunk(
                    combined_ds, 
                    concat_dim_rechunk, 
                    self.mem_allowed, 
                    self.zstore.store,
                    temp_store=self.tempstore.store_path).execute()
                
                self.convert_time = (datetime.now()-t1).total_seconds()/self.limiter
                self.logger.info(f'Concluded Rechunking - {(datetime.now()-t1).total_seconds():.2f}s')
            else:
                self.logger.info('Skipped rechunking step.')
        else:
            self.logger.info(f'Starting Zarr Conversion')
            if not self._dryrun:
                t1 = datetime.now()

                combined_ds.to_zarr(self.zstore.store)
                
                self.convert_time = (datetime.now()-t1).total_seconds()/self.limiter
                self.logger.info(f'Concluded Conversion - {(datetime.now()-t1).total_seconds():.2f}s')
            else:
                self.logger.info('Skipped conversion writing')


    def _get_rechunk_scheme(self, ds):
        """
        Determine Rechunking Scheme appropriate
         - Figure out which variable has the largest total size.
         - Rechunk all dimensions for that variable to sensible values.
         - Rechunk all other dimensions to 1.
        """

        dims               = ds.dims
        concat_dim_rechunk = {}
        dim_sizes          = {d: ds[d].size for d in dims}
        total              = sum(dim_sizes.values())

        for index, cd in enumerate(dims):
            dsize = dim_sizes[cd]
            pref = None
            if self.preferences:
                pref = self.preferences[index]

            if pref:
                # Where a preference is specified
                concat_dim_rechunk[cd] = find_closest(dsize, pref)
            elif total > 20000:
                # For standard sized dimensions.
                concat_dim_rechunk[cd] = find_closest(dsize, 10000*(dsize/total))
            else:
                # For very small dimensions
                concat_dim_rechunk[cd] = find_closest(dsize, dsize/10)

        cpf = 0
        volume = 0
        for var in self.std_vars:
            shape = ds[var].shape
            chunks = []
            for x, dim in enumerate(ds[var].dims):
                chunks.append(shape[x]/concat_dim_rechunk[dim])

            cpf    += sum(chunks)
            volume += ds[var].nbytes

        return concat_dim_rechunk, dim_sizes, cpf/self.limiter, volume/self.limiter

if __name__ == '__main__':
    print('Serial Processor for Kerchunk Pipeline')
    
