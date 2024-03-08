# Borrows from kerchunk tools but with more automation
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
from pipeline.utils import BypassSwitch
from pipeline.errors import *
from pipeline.validate import validate_data, open_kerchunk, validate_selection

class KerchunkDriverFatalError(Exception):

    def __init__(self, message="All drivers failed when performing conversion"):
        self.message = message
        super().__init__(self.message)

WORKDIR = None
CONCAT_MSG = 'See individual files for more details'

class Converter:
    """Class for converting a single file to a Kerchunk reference object"""

    def __init__(self, clogger, bypass_driver=False, ctype=None):
        self.logger = clogger
        self.ctype = ctype
        self.success = True
        self.bypass_driver = bypass_driver

    def convert_to_zarr(self, nfile, extension=False, **kwargs):
        """Perform conversion to zarr with exceptions for bypassing driver errors."""
        drivers = {
            'ncf3': self.ncf3_to_zarr,
            'hdf5': self.hdf5_to_zarr,
            'tif' : self.tiff_to_zarr
        }
        if extension:
            self.ctype=extension
        try:
            if self.ctype in drivers:
                return drivers[self.ctype](nfile, **kwargs)
            else:
                self.logger.debug(f'Extension {self.ctype} not valid')
                return None
        except Exception as err:
            if self.bypass_driver:
                pass
            else:
                raise err

    def hdf5_to_zarr(self, nfile, **kwargs):
        """Converter for HDF5 type files"""
        from kerchunk.hdf import SingleHdf5ToZarr
        return SingleHdf5ToZarr(nfile, **kwargs).translate()

    def ncf3_to_zarr(self, nfile, **kwargs):
        """Converter for NetCDF3 type files"""
        from kerchunk.netCDF3 import NetCDF3ToZarr
        return NetCDF3ToZarr(nfile, **kwargs).translate()

    def tiff_to_zarr(self, tfile, **kwargs):
        """Converter for Tiff type files"""
        self.logger.error('Tiff conversion not yet implemented - aborting')
        self.success = False
        return None
    
class Indexer(Converter):
    def __init__(self, 
                 proj_code, 
                 cfg_file=None, detail_file=None, workdir=WORKDIR, 
                 issave_meta=False, thorough=False, forceful=False, 
                 verb=0, mode=None, version_no='trial-',
                 concat_msg=CONCAT_MSG, bypass=BypassSwitch(), 
                 groupID=None, limiter=None, dryrun=True, ctype=None, fh=None, logid=None, **kwargs):
        """Initialise indexer for this dataset, set all variables and prepare for computation"""
        super().__init__(init_logger(verb, mode, 'compute-serial', fh=fh, logid=logid), bypass_driver=bypass.skip_driver, ctype=ctype)

        self.logger.debug('Starting variable definitions')

        self.bypass     = bypass
        self.limiter    = limiter
        self.workdir    = workdir
        self.proj_code  = proj_code
        self.version_no = version_no
        self.concat_msg = concat_msg
        self.thorough   = thorough
        self.forceful   = forceful

        self.dryrun      = dryrun
        self.issave_meta = issave_meta
        self.updates, self.removals, self.load_refs = False, False, False

        self.logger.debug('Loading config information')
        with open(cfg_file) as f:
            cfg = json.load(f)

        self.detailfile = detail_file
        with open(detail_file) as f:
            self.detail = json.load(f)

        if 'virtual_concat' not in self.detail:
            self.detail['virtual_concat'] = False

        if version_no != 'trial-':
            if 'version_no' in self.detail:
                self.version_no = self.detail['version_no']

        if groupID:
            self.proj_dir = f'{self.workdir}/in_progress/{groupID}/{self.proj_code}'
        else:
            self.proj_dir = f'{self.workdir}/in_progress/{self.proj_code}'

        if 'update' in cfg:
            try:
                self.updates = dict(cfg['update'])
            except ValueError:
                self.logger.warning('Updates attribute not read')
                self.updates = {}
        if 'remove' in cfg:
            try:
                self.removals = dict(cfg['remove'])
            except ValueError:
                self.logger.warning('Removal attribute not read')
                self.removals = {}

        if 'type' in self.detail:
            self.use_json = (self.detail['type'] == 'JSON')
        else:
            self.use_json = True

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

    def collect_details(self):
        """Collect kwargs for combining and any special attributes - save to detail file."""
        self.detail['combine_kwargs'] = self.combine_kwargs
        if self.special_attrs:
            self.detail['special_attrs'] = list(self.special_attrs.keys())
        return self.detail

    def set_filelist(self):
        """Get the list of files from the filelist for this dataset"""
        with open(self.filelist) as f:
            self.listfiles = [r.strip() for r in f.readlines()]
        if not self.limiter:
            self.limiter = len(self.listfiles)

    def add_download_link(self, refs):
        """Add the download link to the Kerchunk references"""
        for key in refs.keys():
            if len(refs[key]) == 3:
                if refs[key][0][0] == '/':
                    refs[key][0] = 'https://dap.ceda.ac.uk' + refs[key][0]
        return refs

    def add_kerchunk_history(self, attrs):
        """Add kerchunk variables to the metadata for this dataset"""

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
    
    def find_concat_dims(self, ds_examples):
        """Find dimensions to use when combining for concatenation
        - Dimensions which change over the set of files must be concatenated together
        - Dimensions which do not change (typically lat/lon) are instead identified as identical_dims"""
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

    def find_identical_dims(self, ds_examples):
        """Find dimensions and variables that are identical across the set of files.
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

    def combine_and_save(self, refs, zattrs):
        """Concatenation of ref data for different kerchunk schemes"""
        self.logger.info('Starting concatenation of refs')
        if len(refs) > 1:
            # Pick 2 refs to use when determining dimension info.
            if len(refs) == 1 or type(refs) == dict:
                pass
            else:
                # Concatenation Dimensions
                if 'combine_kwargs' in self.detail:
                    self.combine_kwargs = self.detail['combine_kwargs']
                else:
                    self.logger.info("Determining concatenation dimensions")
                    print()
                    self.find_concat_dims([
                        open_kerchunk(refs[0], FalseLogger()),
                        open_kerchunk(refs[-1], FalseLogger())
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
                        open_kerchunk(refs[0], FalseLogger()),
                        open_kerchunk(refs[-1], FalseLogger())
                    ])
                    self.logger.info(f"Found {self.combine_kwargs['identical_dims']} identical variables.")
                    print()
                
        if self.use_json:
            self.logger.info('Concatenating to JSON format Kerchunk file')
            self.data_to_json(refs, zattrs)
        else:
            self.logger.info('Concatenating to Parquet format Kerchunk store')
            self.data_to_parq(refs)

        if not self.dryrun:
            self.collect_details()
            with open(self.detailfile,'w') as f:
                f.write(json.dumps(self.detail))
            self.logger.info("Details updated in detail-cfg.json")

    def construct_virtual_dim(self, refs):
        """Construct a Virtual dimension for stacking multiple files where no suitable concatenation dimension is present."""
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

    def data_to_parq(self, refs):
        """Concatenating to Parquet format Kerchunk store"""
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
            concat_dims=['time'],
            **self.combine_kwargs
        ).translate()
        
        out.flush()
        self.logger.info(f'Written to parquet store - {self.proj_code}/kerchunk-1a.parq')

    def data_to_json(self, refs, zattrs):
        """Concatenating to JSON format Kerchunk file"""
        from kerchunk.combine import MultiZarrToZarr

        self.logger.debug('Starting JSON-write process')

        # Already have default options saved to class variables
        if len(refs) > 1:
            self.logger.debug('Concatenating refs using MultiZarrToZarr')
            if self.detail['virtual_concat']:
                refs, vdim = self.construct_virtual_dim(refs)
                self.combine_kwargs['concat_dims'] = [vdim]
            print(self.combine_kwargs)
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

        if not self.dryrun:
            with open(self.outfile,'w') as f:
                f.write(json.dumps(mzz))
            self.logger.info(f'Written to JSON file - {self.outfile}')
        else:
            self.logger.info(f'Skipped writing to JSON file - {self.outfile}')

    def correct_metadata(self, allzattrs):
        """General function for correcting metadata
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
        
    def clean_attr_array(self, allzattrs):
        """Collect global attributes from all refs:
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

    def clean_attrs(self, zattrs):
        """Ammend any saved attributes post-combining
        - Not currently implemented, may be unnecessary
        """
        self.logger.warning('Attribute cleaning post-loading from temp is not implemented')
        return zattrs

    def check_time_attributes(self, times):
        """Takes dict of time attributes with lists of values
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

        duration = '' # Need to compare start/end
        self.logger.debug('Finished time corrections')
        return combined

    def save_metadata(self,zattrs):
        """Cache metadata global attributes in a temporary file"""
        if not self.dryrun:
            with open(f'{self.cache}/temp_zattrs.json','w') as f:
                f.write(json.dumps(zattrs))
            self.logger.debug('Saved global attribute cache')
        else:
            self.logger.debug('Skipped saving global attribute cache')

    def save_individual_ref(self, ref, cache_ref):
        if not os.path.isfile(cache_ref) or self.forceful:
            with open(cache_ref,'w') as f:
                f.write(json.dumps(ref))

    def try_all_drivers(self, nfile, **kwargs):
        """Safe creation allows for known issues and tries multiple drivers"""

        extension = False
        supported_extensions = ['ncf3','hdf5','tif']

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
    
    def load_temp_zattrs(self):
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

    def create_refs(self):
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
        for x, nfile in enumerate(self.listfiles[:self.limiter]):
            cache_ref = f'{self.cache}/{x}.json'
            ref = None
            if os.path.isfile(cache_ref) and not self.thorough:
                self.logger.info(f'Loading refs: {x+1}/{len(self.listfiles)}')
                if os.path.isfile(cache_ref):
                    with open(cache_ref) as f:
                        ref = json.load(f)
            if not ref:
                self.logger.info(f'Creating refs: {x+1}/{len(self.listfiles)}')
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
                self.save_individual_ref(ref, cache_ref)

        if len(partials) > 0:
            raise PartialDriverError(filenums=partials)

        if use_temp_zattrs:
            zattrs = self.load_temp_zattrs()
        if not zattrs:
            zattrs = self.correct_metadata(allzattrs)

        try:
            if self.success:
                self.combine_and_save(refs, zattrs)
        except Exception as err:
            # Any additional parts here.
            raise err

if __name__ == '__main__':
    print('Serial Processor for Kerchunk Pipeline - run with single_run.py')
    