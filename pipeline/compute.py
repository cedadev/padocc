__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import os
import json
import sys
import logging
from datetime import datetime
import fsspec
import xarray as xr
import numpy as np
import base64

from pipeline.logs import init_logger, FalseLogger
from pipeline.utils import BypassSwitch, open_kerchunk, get_proj_file, set_proj_file
from pipeline.errors import *
from pipeline.validate import validate_selection

WORKDIR = None
CONCAT_MSG = 'See individual files for more details'

class KerchunkConverter:
    """Class for converting a single file to a Kerchunk reference object"""

    def __init__(self, clogger=None, bypass_driver=False, ctype=None, verbose=1) -> None:
        if not clogger:
            clogger = init_logger(verbose,0,'convert-trial')

        self.logger        = clogger
        self.ctype         = ctype
        self.success       = True
        self.bypass_driver = bypass_driver
        self.loaded_refs   = False

        self.drivers = {
            'ncf3': self.ncf3_to_zarr,
            'hdf5': self.hdf5_to_zarr,
            'tif' : self.tiff_to_zarr,
            'grib': self.grib_to_zarr,
        }

    def convert_to_zarr(self, nfile: str, extension=False, **kwargs) -> None:
        """
        Perform conversion to zarr with exceptions for bypassing driver errors.

        :param nfile:           (str) Path to a local native file of an appropriate
                                type to be converted.

        :param extension:       (str) File extension relating to file type if known.
                                All extensions/drivers will be tried first, subsequent
                                files in the same dataset will use whatever extension
                                worked for the first file as a starting point.

        :returns:               The output of performing a driver if successful, None
                                if the driver is unsuccessful. Errors will be bypassed
                                if the bypass_driver option is selected for this class.
        """

        if extension:
            self.ctype=extension
        try:
            if self.ctype in self.drivers:
                ref = self.drivers[self.ctype](nfile, **kwargs)
                return ref
            else:
                self.logger.debug(f'Extension {self.ctype} not valid')
                return None
        except Exception as err:
            if self.bypass_driver:
                return None
            else:
                raise err
            
    def try_all_drivers(self, nfile: str, **kwargs) -> dict:
        """
        Safe creation allows for known issues and tries multiple drivers

        :returns:   dictionary of Kerchunk references if successful, raises error
                    otherwise if unsuccessful.
        """

        extension = False
        supported_extensions = list(self.drivers.keys())

        self.logger.debug(f'Attempting conversion for 1 {self.ctype} extension')

        if not self.ctype:
            self.ctype = supported_extensions[0]

        tdict = self.convert_to_zarr(nfile, **kwargs)
        ext_index = 0
        while not tdict and ext_index < len(supported_extensions)-1:
            # Try the other ones
            extension = supported_extensions[ext_index]
            self.logger.debug(f'Attempting conversion for {extension} extension')
            if extension != self.ctype:
                tdict = self.convert_to_zarr(nfile, extension, **kwargs)
            ext_index += 1

        if not tdict:
            self.logger.error('Scanning failed for all drivers, file type is not Kerchunkable')
            raise KerchunkDriverFatalError
        else:
            if extension:
                self.ctype = extension
            self.logger.debug(f'Scan successful with {self.ctype} driver')
            return tdict
            
    def save_individual_ref(self, ref: dict, cache_ref: str, forceful=False) -> None:
        """
        Save each individual set of refs created for each file immediately to reduce
        loss of progress in the event of a failure somewhere in processing.
        """
        if ref and (not os.path.isfile(cache_ref) or forceful):
            with open(cache_ref,'w') as f:
                f.write(json.dumps(ref))

    def load_individual_ref(self, cache_ref: str) -> dict:
        """
        Wrapper for getting proj_file cache_ref contents
        
        :returns:   Dictionary of refs if successful, None or raised error otherwise.
        """
        ref = get_proj_file(cache_ref, None)
        if ref:
            self.loaded_refs = True
        return ref

    def hdf5_to_zarr(self, nfile: str, **kwargs) -> dict:
        """Wrapper for converting NetCDF4/HDF5 type files to Kerchunk"""
        from kerchunk.hdf import SingleHdf5ToZarr
        return SingleHdf5ToZarr(nfile, **kwargs).translate()

    def ncf3_to_zarr(self, nfile: str, **kwargs) -> dict:
        """Wrapper for converting NetCDF3 type files to Kerchunk"""
        from kerchunk.netCDF3 import NetCDF3ToZarr
        return NetCDF3ToZarr(nfile, **kwargs).translate()

    def tiff_to_zarr(self, tfile: str, **kwargs) -> dict:
        """Wrapper for converting GeoTiff type files to Kerchunk"""
        from kerchunk.tiff import TiffToZarr
        return TiffToZarr(tfile, **kwargs).translate()
    
    def grib_to_zarr(self, gfile: str, **kwargs) -> dict:
        """Wrapper for converting GRIB type files to Kerchunk"""
        from kerchunk.grib2 import GribToZarr
        return GribToZarr(gfile, **kwargs).translate()
    
class KerchunkDSProcessor(KerchunkConverter):
    def __init__(self, 
                 proj_code, 
                 workdir=WORKDIR, thorough=False, forceful=False, 
                 verb=0, mode=None, version_no='trial-', concat_msg=CONCAT_MSG, bypass=BypassSwitch(), 
                 groupID=None, limiter=None, dryrun=True, ctype=None, fh=None, logid=None, 
                 skip_concat=False, logger=None, **kwargs) -> None:
        """
        Initialise KerchunkDSProcessor for this dataset, set all variables and prepare for computation.
        
        :param proj_code:           (str) The project code in string format (DOI)

        :param workdir:             (str) Path to the current working directory.

        :param thorough:            (bool) From args.quality - if True will create all files from scratch,
                                    otherwise saved refs from previous runs will be loaded.

        :param forceful:            (bool) Continue with processing even if final output file already exists.

        :param verb:                (int) From args.verbose - Level of verboseness (see logs.init_logger).

        :param mode:                (str) Unused parameter for different logging output mechanisms.

        :param version_no:          (str) Kerchunk revision number/identifier. Default is trial - used for 
                                    'scan' phase, will be overridden with specific revision in 'compute'
                                    actual phase.
        
        :param concat_msg:          (str) Value displayed as global attribute for any attributes that 
                                    differ across the set of files, instead of a list of the differences,
                                    this message will be used, default can be found above.

        :param bypass:              (BypassSwitch) instance of BypassSwitch class containing multiple
                                    bypass/skip options for specific events. See utils.BypassSwitch.

        :param groupID:             (str) Name of current dataset group.

        :param limiter:             (int) Number of files to process from the whole set of files. Default
                                    value of None will mean all files are processed. Any non-None value
                                    will limit the number of files for processing - utilised in 'scan' phase.

        :param dryrun:              (bool) From args.dryrun - if True will prevent output files being generated
                                    or updated and instead will demonstrate commands that would otherwise happen.

        :param ctype:               (str) Extension/filetype of the set of files to be processed if already known.

        :param fh:                  (str) Path to logfile for logger object generated in this specific process.

        :param logid:               (str) ID of the process within a subset, which is then added to the name
                                    of the logger - prevents multiple processes with different logfiles getting
                                    loggers confused.

        :param skip_concat:         (bool) Internal parameter for skipping concat - used for parallel construction 
                                    which requires a more complex job allocation.

        :returns: None

        """
        if not logger:
            logger = init_logger(verb, mode, 'compute-serial', fh=fh, logid=logid)
        super().__init__(caselogger=logger, bypass_driver=bypass.skip_driver, ctype=ctype)

        self.logger.debug('Starting variable definitions')

        self.bypass     = bypass
        self.limiter    = limiter
        self.workdir    = workdir
        self.proj_code  = proj_code
        self.version_no = version_no
        self.concat_msg = concat_msg
        self.thorough   = thorough
        self.forceful   = forceful
        self.skip_concat= skip_concat

        self.validate_time = None
        self.concat_time   = None
        self.convert_time  = None

        self.dryrun      = dryrun
        self.updates, self.removals, self.load_refs = False, False, False

        if groupID:
            self.proj_dir = f'{self.workdir}/in_progress/{groupID}/{self.proj_code}'
        else:
            self.proj_dir = f'{self.workdir}/in_progress/{self.proj_code}'

        self.logger.debug('Loading config information')
        self.cfg = get_proj_file(self.proj_dir, 'base-cfg.json')

        self.detail = get_proj_file(self.proj_dir, 'detail-cfg.json')
        if not self.detail:
            self.detail={}

        with open(f'{self.proj_dir}/allfiles.txt')as f:
            self.num_files = len(list(f.readlines()))

        self.partial = (self.limiter and self.num_files != self.limiter)

        if 'virtual_concat' not in self.detail:
            self.detail['virtual_concat'] = False

        if version_no != 'trial-':
            if 'version_no' in self.detail:
                self.version_no = self.detail['version_no']

        if 'update' in self.cfg:
            try:
                self.updates = dict(self.cfg['update'])
            except ValueError:
                self.logger.warning('Updates attribute not read')
                self.updates = {}
        if 'remove' in self.cfg:
            try:
                self.removals = dict(self.cfg['remove'])
            except ValueError:
                self.logger.warning('Removal attribute not read')
                self.removals = {}

        if 'type' in self.detail:
            self.use_json = (self.detail['type'] == 'JSON')
        else:
            self.use_json = True

        self.outfile     = f'{self.proj_dir}/kerchunk-{version_no}a.json'
        self.outstore    = f'{self.proj_dir}/kerchunk-{version_no}a.parq'
        self.record_size = 167 # Default

        self.filelist = f'{self.proj_dir}/allfiles.txt'

        self.cache    = f'{self.proj_dir}/cache/'
        if os.path.isfile(f'{self.cache}/temp_zattrs.json') and not thorough:
            # Load data instead of create from scratch
            self.load_refs = True
            self.logger.debug('Found cached data from previous run, loading cache')
        

        if not os.path.isdir(self.cache):
            os.makedirs(self.cache) 
        if thorough:
            os.system(f'rm -rf {self.cache}/*')

        self.combine_kwargs = {} # Now using concat_dims and identical dims finders.
        self.create_kwargs  = {'inline_threshold':1000}
        self.pre_kwargs     = {}

        self.special_attrs = {}

        self.set_filelist()
        self.logger.debug('Finished all setup steps')

    def collect_details(self) -> dict:
        """
        Collect kwargs for combining and any special attributes - save to detail file.
        """
        self.detail['combine_kwargs'] = self.combine_kwargs
        if self.special_attrs:
            self.detail['special_attrs'] = list(self.special_attrs.keys())
        return self.detail
    
    def get_timings(self) -> dict:
        """
        Export timed values if refs were all created from scratch.
        Ref loading invalidates timings so returns None if any refs were loaded
        not created.

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

    def set_filelist(self) -> None:
        """
        Get the list of files from the filelist for this dataset and set
        to 'filelist' list.
        """
        with open(self.filelist) as f:
            self.listfiles = [r.strip() for r in f.readlines()]
        if not self.limiter:
            self.limiter = len(self.listfiles)

    def add_download_link(self, refs: dict) -> dict:
        """
        Add the download link to each of the Kerchunk references
        """
        for key in refs.keys():
            if len(refs[key]) == 3:
                if refs[key][0][0] == '/':
                    refs[key][0] = 'https://dap.ceda.ac.uk' + refs[key][0]
        return refs

    def add_kerchunk_history(self, attrs: dict) -> dict:
        """
        Add kerchunk variables to the metadata for this dataset, including 
        creation/update date and version/revision number.
        """

        from datetime import datetime

        # Get current time
        # Format for different uses
        now = datetime.now()
        if 'history' in attrs:
            if type(attrs['history']) == str:
                hist = attrs['history'].split('\n')
            else:
                hist = attrs['history']

            if 'Kerchunk' in hist[-1]:
                hist[-1] = 'Kerchunk file updated on ' + now.strftime("%D")
            else:
                hist.append('Kerchunk file created on ' + now.strftime("%D"))
            attrs['history'] = '\n'.join(hist)
        else:
            attrs['history'] = 'Kerchunk file created on ' + now.strftime("%D") + '\n'
        
        attrs['kerchunk_revision'] = self.version_no
        attrs['kerchunk_creation_date'] = now.strftime("%d%m%yT%H%M%S")
        return attrs
    
    def find_concat_dims(self, ds_examples: list) -> None:
        """Find dimensions to use when combining for concatenation
        - Dimensions which change over the set of files must be concatenated together
        - Dimensions which do not change (typically lat/lon) are instead identified as identical_dims
        """
        concat_dims = []
        for dim in ds_examples[0].dims:
            try:
                validate_selection(ds_examples[0][dim], ds_examples[1][dim], dim, 128, 128, self.logger, bypass=self.bypass)          
            except ValidationError:
                self.logger.debug(f'Non-identical dimension: {dim} - if this dimension should be identical across the files, please inspect.')
                concat_dims.append(dim)
            except SoftfailBypassError as err:
                self.logger.error(f'Found Empty dimension {dim} across example files - assuming non-stackable')
                raise err
            except Exception as err:
                self.logger.warning('Non validation error is present')
                raise err
        if len(concat_dims) == 0:
            self.detail['virtual_concat'] = True
        self.combine_kwargs['concat_dims'] = concat_dims

    def find_identical_dims(self, ds_examples: list) -> None:
        """
        Find dimensions and variables that are identical across the set of files.
        - Variables which do not change (typically lat/lon) are identified as identical_dims and not concatenated over the set of files.
        - Variables which do change are concatenated as usual.
        """
        identical_dims = []
        normal_dims = []
        for var in ds_examples[0].variables:
            identical_check = True
            for dim in self.combine_kwargs['concat_dims']:
                if dim in ds_examples[0][var].dims:
                    identical_check = False
            if identical_check:
                try:
                    validate_selection(ds_examples[0][var], ds_examples[1][var], var, 128, 128, self.logger, bypass=self.bypass)
                    identical_dims.append(var)
                except ValidationError:
                    self.logger.debug(f'Non-identical variable: {var} - if this variable should be identical across the files, please rerun.')
                    normal_dims.append(var)
                except SoftfailBypassError as err:
                    self.logger.warning(f'Found Empty variable {var} across example files - assuming non-identical')
                    normal_dims.append(var)
                except Exception as err:
                    self.logger.warning('Unexpected error in checking identical dims')
                    raise err
            else:
                normal_dims.append(var)
        if len(identical_dims) == len(ds_examples[0].variables):
            raise IdenticalVariablesError
        self.combine_kwargs['identical_dims'] = identical_dims
        if self.combine_kwargs["concat_dims"] == []:
            self.logger.info(f'No concatenation dimensions identified - {normal_dims} will be concatenated using a virtual dimension')  
        else:
            self.logger.debug(f'Found {normal_dims} that vary over concatenation_dimensions: {self.combine_kwargs["concat_dims"]}')          

    def combine_and_save(self, refs: dict, zattrs: dict) -> None:
        """
        Concatenation of ref data for different kerchunk schemes
        """
        self.logger.info('Starting concatenation of refs')
        if not (len(refs) == 1 or type(refs) == dict):
            # Pick 2 refs to use when determining dimension info.
            # Concatenation Dimensions
            if 'combine_kwargs' in self.detail:
                self.combine_kwargs = self.detail['combine_kwargs']
            else:
                # Calculate Partial Validation Estimate here
                t1 = datetime.now()
                self.logger.info("Determining concatenation dimensions")
                print()
                self.find_concat_dims([
                    open_kerchunk(refs[0], FalseLogger(), remote_protocol=None),
                    open_kerchunk(refs[-1], FalseLogger(), remote_protocol=None)
                ])
                if self.combine_kwargs['concat_dims'] == []:
                    self.logger.info(f"No concatenation dimensions available - virtual dimension will be constructed.")
                else:
                    self.logger.info(f"Found {self.combine_kwargs['concat_dims']} concatenation dimensions.")
                print()

                # Identical (Variables) Dimensions
                self.logger.info("Determining identical variables")
                print()
                self.find_identical_dims([
                    open_kerchunk(refs[0], FalseLogger(), remote_protocol=None),
                    open_kerchunk(refs[-1], FalseLogger(), remote_protocol=None)
                ])
                self.logger.info(f"Found {self.combine_kwargs['identical_dims']} identical variables.")
                print()

                # This one only happens for two files so don't need to take a mean
                self.validate_time = (datetime.now()-t1).total_seconds()

        t1 = datetime.now()  
        if self.use_json:
            self.logger.info('Concatenating to JSON format Kerchunk file')
            self.data_to_json(refs, zattrs)
        else:
            self.logger.info('Concatenating to Parquet format Kerchunk store')
            self.data_to_parq(refs)
        self.concat_time = (datetime.now()-t1).total_seconds()/self.limiter

        if not self.dryrun:
            self.collect_details()
            with open(self.detailfile,'w') as f:
                f.write(json.dumps(self.detail))
            self.logger.info("Details updated in detail-cfg.json")

    def construct_virtual_dim(self, refs: dict) -> None:
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

    def data_to_parq(self, refs: dict) -> None:
        """
        Concatenating to Parquet-format Kerchunk store
        """

        from kerchunk.combine import MultiZarrToZarr
        from fsspec import filesystem
        from fsspec.implementations.reference import LazyReferenceMapper

        self.logger.debug('Starting parquet-write process')

        if not os.path.isdir(self.outstore):
            os.makedirs(self.outstore)
        out = LazyReferenceMapper.create(self.record_size, self.outstore, fs = filesystem("file"), **self.pre_kwargs)

        out_dict = MultiZarrToZarr(
            refs,
            out=out,
            remote_protocol='file',
            **self.combine_kwargs
        ).translate()
        
        if self.partial:
            self.logger.info(f'Skipped writing to parquet store - {self.outstore}')
        else:
            out.flush()
            self.logger.info(f'Written to parquet store - {self.outstore}')

    def data_to_json(self, refs: dict, zattrs: dict) -> None:
        """
        Concatenating to JSON-format Kerchunk file
        """
        from kerchunk.combine import MultiZarrToZarr

        self.logger.debug('Starting JSON-write process')

        # Already have default options saved to class variables
        if len(refs) > 1:
            self.logger.debug('Concatenating refs using MultiZarrToZarr')
            if self.detail['virtual_concat']:
                refs, vdim = self.construct_virtual_dim(refs)
                self.combine_kwargs['concat_dims'] = [vdim]
            mzz = MultiZarrToZarr(list(refs), **self.combine_kwargs).translate()
            if zattrs:
                zattrs = self.add_kerchunk_history(zattrs)
            else:
                self.logger.debug(zattrs)
                raise ValueError
            mzz['refs']['.zattrs'] = json.dumps(zattrs)
        else:
            self.logger.debug('Found single ref to save')
            mzz = refs[0]
        # Override global attributes
        mzz['refs'] = self.add_download_link(mzz['refs'])

        if not self.dryrun and not self.partial:
            with open(self.outfile,'w') as f:
                f.write(json.dumps(mzz))
            self.logger.info(f'Written to JSON file - {self.outfile}')
        else:
            self.logger.info(f'Skipped writing to JSON file - {self.outfile}')

    def correct_metadata(self, allzattrs: dict) -> dict:
        """
        General function for correcting metadata
        - Combine all existing metadata in standard way (cleaning arrays)
        - Add updates and remove removals specified by configuration
        """

        self.logger.debug('Starting metadata corrections')
        if type(allzattrs) == list:
            zattrs = self.clean_attr_array(allzattrs)
        else:
            zattrs = self.clean_attrs(allzattrs)
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
        
    def clean_attr_array(self, allzattrs: dict) -> dict:
        """
        Collect global attributes from all refs:
        - Determine which differ between refs and apply changes
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
        base = {**base, **self.check_time_attributes(times)}
        self.logger.debug('Comparing similar keys')

        for attr in nonequal.keys():
            if len(set(all_values[attr])) == 1:
                base[attr] = all_values[attr][0]
            else:
                base[attr] = self.concat_msg
                self.special_attrs[attr] = 0

        self.logger.debug('Finished checking similar keys')
        return base

    def clean_attrs(self, zattrs: dict) -> dict:
        """
        Ammend any saved attributes post-combining
        - Not currently implemented, may be unnecessary
        """
        self.logger.warning('Attribute cleaning post-loading from temp is not implemented')
        return zattrs

    def check_time_attributes(self, times: dict) -> dict:
        """
        Takes dict of time attributes with lists of values
        - Sort time arrays
        - Assume time_coverage_start, time_coverage_end, duration (2 or 3 variables)
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
                if len(set(times[k])) == len(self.listfiles):
                    combined[k] = 'See individual files for details'
                elif len(set(times[k])) == 1:
                    combined[k] = times[k][0]
                else:
                    combined[k] = list(set(times[k]))

        self.logger.debug('Finished time corrections')
        return combined

    def save_metadata(self,zattrs: dict) -> dict:
        """
        Cache metadata global attributes in a temporary file.
        """

        if not self.dryrun:
            with open(f'{self.cache}/temp_zattrs.json','w') as f:
                f.write(json.dumps(zattrs))
            self.logger.debug('Saved global attribute cache')
        else:
            self.logger.debug('Skipped saving global attribute cache')
    
    def load_temp_zattrs(self) -> dict:
        """
        Load global attributes from a 'temporary' cache file.
        """

        self.logger.debug(f'Loading attributes')
        try:
            with open(f'{self.cache}/temp_zattrs.json') as f:
                zattrs = json.load(f)
        except FileNotFoundError:
            zattrs = None
        if not zattrs:
            self.logger.debug('No attributes loaded from temp store')
            return None
        return zattrs

    def create_refs(self) -> None:
        """Organise creation and loading of refs
        - Load existing cached refs
        - Create new refs
        - Combine metadata and global attributes into a single set
        - Coordinate combining and saving of data"""
        self.logger.info(f'Starting computation for components of {self.proj_code}')
        refs, allzattrs = [], []
        partials = []
        zattrs = None
        use_temp_zattrs = True

        # Attempt to load existing file - create if not exists already
        t1 = datetime.now()
        for x, nfile in enumerate(self.listfiles[:self.limiter]):
            cache_ref = f'{self.cache}/{x}.json'
            ref = None
            if not self.thorough:
                ref = self.load_individual_ref(cache_ref)
                if ref:
                    self.logger.info(f'Loaded refs: {x+1}/{self.limiter}')
            if not ref:
                self.logger.info(f'Creating refs: {x+1}/{self.limiter}')
                try:
                    ref = self.try_all_drivers(nfile, **self.create_kwargs)
                except KerchunkDriverFatalError as err:
                    if len(refs) == 0:
                        raise err
                    else:
                        partials.append(x)
                use_temp_zattrs = False
            if ref:
                allzattrs.append(ref['refs']['.zattrs'])
                refs.append(ref)
                cache_ref = f'{self.cache}/{x}.json'
                self.save_individual_ref(ref, cache_ref, forceful=self.forceful)
        # Compute mean conversion time for this set.
        self.convert_time = (datetime.now()-t1).total_seconds()/self.limiter

        if len(partials) > 0:
            raise PartialDriverError(filenums=partials)

        if use_temp_zattrs:
            zattrs = self.load_temp_zattrs()
        if not zattrs:
            zattrs = self.correct_metadata(allzattrs)

        try:
            if self.success and not self.skip_concat:
                self.combine_and_save(refs, zattrs)
        except Exception as err:
            # Any additional parts here.
            raise err

class ZarrRechunker(KerchunkConverter):
    """
    Rechunk input data types directly into zarr using Pangeo Rechunker.
    - If refs already exist from previous Kerchunk runs, can use these to inform rechunker.
    - Otherwise will have to start from scratch.
    """
    def __init__(self):
        raise NotImplementedError

def configure_kerchunk(args, logger, fh=None, logid=None):
    """
    Configure all required steps for Kerchunk processing.
    - Check if output files already exist.
    - Configure timings post-run.
    """
    version_no = 1
    complete, escape = False, False
    while not (complete or escape):
        out_json = f'{args.proj_dir}/kerchunk-{version_no}a.json'
        out_parq = f'{args.proj_dir}/kerchunk-{version_no}a.parq'

        if os.path.isfile(out_json) or os.path.isfile(out_parq):
            if args.forceful:
                complete = True
            elif args.new_version:
                version_no += 1
            else:
                escape = True
        else:
            complete = True

    concat_msg = '' # CMIP and CCI may be different?

    if complete and not escape:

        t1 = datetime.now()
        ds = KerchunkDSProcessor(args.proj_code,
                workdir=args.workdir,thorough=args.quality, forceful=args.forceful,
                verb=args.verbose, mode=args.mode,
                version_no=version_no, concat_msg=concat_msg, bypass=args.bypass, groupID=args.groupID, 
                dryrun=args.dryrun, fh=fh, logid=logid)
        ds.create_refs()

        compute_time = (datetime.now()-t1).total_seconds()

        detail = get_proj_file(args.proj_dir, 'detail-cfg.json')
        if 'timings' not in detail:
            detail['timings'] = {}

        timings = ds.get_timings()
        if timings:
            logger.info('Export timings for this process - all refs created from scratch.')
            detail['timings']['convert_actual'] = timings['convert_actual']
            detail['timings']['concat_actual']  = timings['concat_actual']
            detail['timings']['compute_actual'] = compute_time
        set_proj_file(args.proj_dir, 'detail-cfg.json', detail, logger)

    else:
        logger.error('Output file already exists and there is no plan to overwrite')
        return None

def configure_zarr(args, logger):
    raise NotImplementedError

def compute_config(args, fh=None, logid=None, **kwargs) -> None:
    """
    Will serve as main point of configuration for processing runs.
    Must be able to assess between using Zarr/Kerchunk.
    """

    logger = init_logger(args.verbose, args.mode, 'compute-serial', fh=fh, logid=logid)

    logger.info(f'Starting computation step for {args.proj_code}')

    cfg_file = f'{args.proj_dir}/base-cfg.json'
    detail_file = f'{args.proj_dir}/detail-cfg.json'

    # Preliminary checks
    if not os.path.isfile(cfg_file):
        logger.error(f'cfg file missing or not provided - {cfg_file}')
        raise FileNotFoundError(cfg_file)
    
    if not os.path.isfile(detail_file):
        logger.error(f'cfg file missing or not provided - {detail_file}')
        raise FileNotFoundError(detail_file)
    
    # Open the detailfile to check type.
    detail = get_proj_file(args.proj_dir, 'detail-cfg.json')
    if detail['type'] == 'Zarr':
        configure_zarr(args, logger)
    else:
        configure_kerchunk(args, logger)

if __name__ == '__main__':
    print('Serial Processor for Kerchunk Pipeline - run with single_run.py')
    