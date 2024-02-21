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

from pipeline.logs import init_logger, BypassSwitch, FalseLogger
from pipeline.errors import IdenticalVariablesError, ValidationError, ConcatenationError, SoftfailBypassError
from pipeline.validate import validate_data, open_kerchunk, validate_selection

class KerchunkDriverFatalError(Exception):

    def __init__(self, message="All drivers failed when performing conversion"):
        self.message = message
        super().__init__(self.message)

WORKDIR = None
CONCAT_MSG = 'See individual files for more details'

class Converter:
    def __init__(self, clogger, bypass_driver=False):
        self.logger = clogger
        self.success = True
        self.bypass_driver = bypass_driver

    def convert_to_zarr(self, nfile, ctype, **kwargs):
        #ctype = 'hdf5'
        try:
            if ctype == 'ncf3':
                return self.ncf3_to_zarr(nfile, **kwargs)
            elif ctype == 'hdf5':
                return self.hdf5_to_zarr(nfile, **kwargs)
            elif ctype == 'tif':
                return self.tiff_to_zarr(nfile, **kwargs)
            else:
                self.logger.debug(f'Extension {ctype} not valid')
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
                 groupID=None, limiter=None, dryrun=True):
        """Initialise indexer for this dataset, set all variables and prepare for computation"""
        super().__init__(init_logger(verb, mode, 'compute-serial'), bypass_driver=bypass.skip_driver)

        self.logger.debug('Starting variable definitions')
        self.bypass     = bypass
        self.limiter    = limiter
        self.workdir    = workdir
        self.proj_code  = proj_code
        self.version_no = version_no
        self.concat_msg = concat_msg

        self.dryrun = dryrun
        self.issave_meta = issave_meta
        self.updates, self.removals, self.load_refs = False, False, False

        self.verb = verb
        self.mode = mode
        if mode != 'std':
            self.log = ''
        else:
            self.log = None

        self.logger.debug('Loading config information')
        with open(cfg_file) as f:
            cfg = json.load(f)

        with open(detail_file) as f:
            self.detail = json.load(f)

        if 'virtual_concat' not in self.detail:
            self.detail['virtual_concat'] = False

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

        self.thorough = thorough

        self.combine_kwargs = {'concat_dims':['time']} # Always try time initially
        self.create_kwargs  = {'inline_threshold':1000}
        self.pre_kwargs     = {}

        self.set_filelist()
        self.logger.debug('Finished all setup steps')

    def collect_details(self):
        self.detail['combine_kwargs'] = self.combine_kwargs
        return self.detail

    def set_filelist(self):
        """Get the list of files from the filelist for this dataset"""
        with open(self.filelist) as f:
            self.listfiles = [r.strip() for r in f.readlines()]
        if not self.limiter:
            self.limiter = len(self.listfiles)

    def add_download_link(self, refs):
        timecount = 0
        total = len(list(refs.keys()))
        t1 = datetime.now()
        for key in refs.keys():
            if len(refs[key]) == 3:
                if refs[key][0][0] == '/':
                    refs[key][0] = 'https://dap.ceda.ac.uk' + refs[key][0]
            if timecount == 100:
                end_time = (datetime.now()-t1).total_seconds()*(total/6000)
                self.logger.debug(f'Expected time remaining: {end_time} mins')
            timecount += 1
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
        concat_dims = []
        for dim in ds_examples[0].dims:
            try:
                validate_selection(ds_examples[0][dim], ds_examples[1][dim], dim, 1, 1, self.logger, bypass=self.bypass)          
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
        identical_dims = []
        normal_dims = []
        for var in ds_examples[0].variables:
            identical_check = True
            for dim in self.combine_kwargs['concat_dims']:
                if dim in ds_examples[0][var].dims:
                    identical_check = False
            if identical_check:
                try:
                    validate_selection(ds_examples[0][var], ds_examples[1][var], var, 1, 1, self.logger, bypass=self.bypass)
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

    def construct_virtual_dim(self, refs):
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

    def data_to_parq(self, create_refs):
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
        # General function for correcting metadata
        # - Combine all existing metadata in standard way
        # - Add updates and remove removals specified by configuration

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
        # Collect attributes from all files, 
        # determine which are always equal, which have differences
        base = json.loads(allzattrs[0])

        self.logger.debug('Correcting time attributes')
        # Sort out time metadata here
        times = {}
        all_values = {}
        for k in base.keys():
            if 'time' in k:
                times[k] = [base[k]]
            all_values[k] = []

        nonequal = {}
        for ref in allzattrs[1:]:
            zattrs = json.loads(ref)
            for attr in zattrs.keys():
                if attr in all_values:
                    all_values[attr].append(zattrs[attr])
                else:
                    all_values[attr] = zattrs[attr]
                if attr in times:
                    times[attr].append(zattrs[attr])
                elif attr not in base:
                    nonequal[attr] = False
                else:
                    if base[attr] != zattrs[attr]:
                        nonequal[attr] = False

        base = {**base, **self.check_time_attributes(times)}
        self.logger.debug('Comparing similar keys')

        for attr in nonequal.keys():
            if len(set(all_values[attr])) == 1:
                base[attr] = all_values[attr][0]
            else:
                base[attr] = self.concat_msg

        self.logger.debug('Finished checking similar keys')
        return base

    def clean_attrs(self, zattrs):
        self.logger.warning('Attribute cleaning post-loading from temp is not implemented')
        return zattrs

    def check_time_attributes(self, times):
        # Takes dict of time attributes with lists of values
        # Sort time arrays
        # Assume time_coverage_start, time_coverage_end, duration (2 or 3 variables)
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

    def save_cache(self, refs, zattrs):
        """Cache reference data in temporary json reference files"""
        self.logger.info('Saving pre-concat cache')
        if not self.dryrun:
            for x, r in enumerate(refs):
                cache_ref = f'{self.cache}/{x}.json'
                with open(cache_ref,'w') as f:
                    f.write(json.dumps(r))
            self.logger.debug('Saved kerchunk data cache')
        else:
            self.logger.debug('Skipped saving metadata cache')
        self.save_metadata(zattrs)
        self.issave_meta = False
        # Save is only valid prior to concatenation
        # For a bizarre reason, the download link is applied to the whole cache on concatenation,
        # So cache saving MUST occur BEFORE concatenation.

    def try_all_drivers(self, nfile, **kwargs):
        """Safe creation allows for known issues and tries multiple drivers"""

        extension = False

        if '.' in nfile:
            ctype = f'.{nfile.split(".")[-1]}'
        else:
            ctype = '.nc'

        supported_extensions = ['ncf3','hdf5','tif']

        self.logger.debug(f'Attempting conversion for 1 {ctype} extension')

        tdict = self.convert_to_zarr(nfile, ctype, **kwargs)
        ext_index = 0
        while not tdict and ext_index < len(supported_extensions)-1:
            # Try the other ones
            extension = supported_extensions[ext_index]
            self.logger.debug(f'Attempting conversion for {extension} extension')
            if extension != ctype:
                tdict = self.convert_to_zarr(nfile, extension, **kwargs)
            ext_index += 1

        if not tdict:
            self.logger.error('Scanning failed for all drivers, file type is not Kerchunkable')
            raise KerchunkDriverFatalError
        else:
            if extension:
                self.logger.debug(f'Scan successful with {extension} driver')
            else:
                self.logger.debug(f'Scan successful with {ctype} driver')
            return tdict
    
    def convert_to_kerchunk(self):
        refs = []
        allzattrs = []
        for x, nfile in enumerate(self.listfiles[:self.limiter]):
            self.logger.info(f'Creating refs: {x+1}/{len(self.listfiles)}')
            zarr_content = self.try_all_drivers(nfile, **self.create_kwargs)
            if zarr_content:
                allzattrs.append(zarr_content['refs']['.zattrs'])
                refs.append(zarr_content)
        return allzattrs, refs
    
    def load_cache(self):
        refs = []
        for x, nfile in enumerate(self.listfiles[:self.limiter]):
            self.logger.info(f'Loading refs: {x+1}/{len(self.listfiles)}')
            cache_ref = f'{self.cache}/{x}.json'
            with open(cache_ref) as f:
                refs.append(json.load(f))

        self.logger.debug(f'Loading attributes')
        with open(f'{self.cache}/temp_zattrs.json') as f:
            zattrs = json.load(f)
        if not zattrs:
            self.logger.error('No attributes loaded from temp store')
            raise ValueError
        return zattrs, refs

    def create_refs(self):
        self.logger.info(f'Starting computation for components of {self.proj_code}')
        if not self.load_refs:
            allzattrs, refs = self.convert_to_kerchunk()
            zattrs = self.correct_metadata(allzattrs)
        else:
            zattrs, refs = self.load_cache()
            zattrs = self.correct_metadata(zattrs)
        try:
            if self.issave_meta and (not self.load_refs):
                self.save_cache(refs, zattrs)
            if self.success:
                self.combine_and_save(refs, zattrs)
        except Exception as err:
            if self.issave_meta and (not self.load_refs or self.thorough):
                self.save_cache(refs, zattrs)
            raise err

if __name__ == '__main__':
    print('Serial Processor for Kerchunk Pipeline - run with single_run.py')
    