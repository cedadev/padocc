# Borrows from kerchunk tools but with more automation
import os
import json
import sys
import logging

WORKDIR = None
CONCAT_MSG = 'See individual files for more details'

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

class Indexer:
    def __init__(self, 
                 proj_code, 
                 cfg_file=None, detail_file=None, workdir=WORKDIR, 
                 issave_meta=False, refresh='', forceful=False, 
                 verb=0, mode=None, version_no=1,
                 concat_msg=CONCAT_MSG):
        """Initialise indexer for this dataset, set all variables and prepare for computation"""

        self.logger = init_logger(verb, mode, 'compute-serial')

        self.logger.debug('Starting variable definitions')

        self.workdir   = workdir
        self.proj_code = proj_code

        self.issave_meta = issave_meta
        self.updates, self.removals, self.load_refs = False, False, False

        self.version_no = version_no

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
            detail = json.load(f)
        
        self.proj_dir = cfg['proj_dir']

        if 'update' in cfg:
            self.updates = cfg['update']
        if 'remove' in cfg:
            self.removals = cfg['remove']

        if 'type' in detail:
            self.use_json = (detail['type'] == 'JSON')
        else:
            self.use_json = True

        self.outfile     = f'{self.proj_dir}/kerchunk-{version_no}a.json'
        self.outstore    = f'{self.proj_dir}/kerchunk-{version_no}a.parq'
        self.record_size = 167 # Default

        self.filelist = f'{self.proj_dir}/allfiles.txt'

        self.cache    = f'{self.proj_dir}/cache/'
        if refresh == '' and os.path.isfile(f'{self.cache}/temp_zattrs.json'):
            # Load data instead of create from scratch
            self.load_refs = True
            self.logger.debug('Found cached data from previous run, loading cache')

        if not os.path.isdir(self.cache):
            os.makedirs(self.cache)

        self.success  = True

        self.combine_kwargs = {}
        self.create_kwargs  = {}
        self.pre_kwargs     = {}

        self.get_files()
        self.logger.info('Finished all setup steps')

    def get_files(self):
        """Get the list of files from the filelist for this dataset"""
        with open(self.filelist) as f:
            self.listfiles = [r.strip() for r in f.readlines()]
        self.limiter = len(self.listfiles)

    def hdf5_to_zarr(self, nfile, **kwargs):
        """Converter for HDF5 type files"""
        from kerchunk.hdf import SingleHdf5ToZarr
        try:
            return SingleHdf5ToZarr(nfile, **kwargs).translate()
        except Exception as err:
            self.logger.error(f'Issue with dataset {nfile} - {err}')
            self.success = False
            return None

    def ncf3_to_zarr(self, nfile, **kwargs):
        """Converter for NetCDF3 type files"""
        from kerchunk.netCDF3 import NetCDF3ToZarr
        try:
            return NetCDF3ToZarr(nfile, **kwargs).translate()
        except Exception as err:
            self.logger.error(f'Issue with dataset {nfile} - {err}')
            self.success = False
            return None
        
    def tiff_to_zarr(self, tfile, **kwargs):
        """Converter for Tiff type files"""
        self.logger.error('Tiff conversion not yet implemented - aborting')
        self.success = False
        return None

    def concat_data(self, refs, zattrs):
        """Concatenation of ref data for different kerchunk schemes"""
        if self.use_json:
            self.logger.info('Concatenating to JSON format Kerchunk file')
            self.data_to_json(refs, zattrs)
        else:
            self.logger.info('Concatenating to Parquet format Kerchunk store')
            self.data_to_parq(refs)

    def data_to_parq(self, refs):
        """Concatenating to Parquet format Kerchunk store"""
        from kerchunk.combine import MultiZarrToZarr
        from fsspec import filesystem
        from fsspec.implementations.reference import LazyReferenceMapper

        self.logger.info('Starting parquet-write process')

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
        self.logger.info('Written to parquet store')

    def data_to_json(self, refs, zattrs):
        """Concatenating to JSON format Kerchunk file"""
        from kerchunk.combine import MultiZarrToZarr

        self.logger.info('Starting JSON-write process')

        # Already have default options saved to class variables
        mzz = MultiZarrToZarr(refs, concat_dims=['time'], **self.combine_kwargs).translate()
        # Override global attributes

        # Needs but must be fixed
        zattrs = self.add_kerchunk_history(zattrs)
        mzz['refs']['.zattrs'] = json.dumps(zattrs)

        with open(self.outfile,'w') as f:
            f.write(json.dumps(mzz))

        self.logger.info('Written to JSON file')

    def add_kerchunk_history(self, attrs):
        """Add kerchunk variables to the metadata for this dataset"""

        from datetime import datetime

        # Get current time
        # Format for different uses
        now = datetime.now()
        hist = attrs['history'].split('\n')

        if 'Kerchunk' in hist[-1]:
            hist[-1] = 'Kerchunk file updated on ' + now.strftime("%D")
        else:
            hist.append('Kerchunk file created on ' + now.strftime("%D"))
        attrs['history'] = '\n'.join(hist)
        
        attrs['kerchunk_revision'] = self.version_no
        attrs['kerchunk_creation_date'] = now.strftime("%d%m%yT%H%M%S")
        return attrs

    def correct_metadata(self, allzattrs):
        # General function for correcting metadata
        # - Combine all existing metadata in standard way
        # - Add updates and remove removals specified by configuration

        self.logger.info('Starting metadata corrections')
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

        self.logger.info('Finished metadata corrections')
        return zattrs
        
    def clean_attr_array(self, allzattrs):
        # Collect attributes from all files, 
        # determine which are always equal, which have differences
        base = json.loads(allzattrs[0])

        self.logger.info('Correcting time attributes')
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
        if len(nonequal.keys()) > 0:
            self.success = False

        self.logger.info('Finished checking similar keys')
        return base

    def clean_attrs(self, zattrs):
        pass

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
        self.logger.info('Finished time corrections')
        return combined

    def save_metadata(self,zattrs):
        """Cache metadata global attributes in a temporary file"""
        with open(f'{self.cache}/temp_zattrs.json','w') as f:
            f.write(json.dumps(zattrs))
        self.logger.debug('Saved global attribute cache')

    def save_cache(self, refs, zattrs):
        """Cache reference data in temporary json reference files"""
        for x, r in enumerate(refs):
            cache_ref = f'{self.cache}/{x}.json'
            with open(cache_ref,'w') as f:
                f.write(json.dumps(r))
        self.save_metadata(zattrs)
        self.logger.debug('Saved metadata cache')
        # All file content saved for later reconcatenation

    def safe_create(self, nfile, **kwargs):
        """Safe creation allows for known issues and tries multiple drivers"""
        drivers = {
            'hdf5':self.hdf5_to_zarr,
            'ncf3':self.ncf3_to_zarr,
            'tiff':self.tiff_to_zarr
        }
        if 'kdriver' in kwargs:
            kdriver = drivers[kwargs['kdriver']]
            method  = kwargs['kdriver']
        else:
            kdriver = drivers['hdf5']
            method  = 'hdf5'

        zarr_content = kdriver(nfile, **kwargs)
        key = ''
        index = 0
        escape = False
        while not zarr_content or escape:
            key = list(drivers.keys())[index]
            if key != method:
                zarr_content = drivers[key](nfile, **kwargs)
            if index >= len(drivers.keys()):
                escape = True
            index += 1

        return zarr_content
    
    def get_kerchunk_data(self):
        refs = []
        allzattrs = []
        for x, nfile in enumerate(self.listfiles[:self.limiter]):
            self.logger.debug(f'Creating refs: {x+1}/{len(self.listfiles)}')
            zarr_content = self.safe_create(nfile, **self.create_kwargs)
            if zarr_content:
                allzattrs.append(zarr_content['refs']['.zattrs'])
                refs.append(zarr_content)
        return allzattrs, refs
    
    def load_kdata(self):
        refs = []
        for x, nfile in enumerate(self.listfiles[:self.limiter]):
            self.logger.debug(f'Loading refs: {x+1}/{len(self.listfiles)}')
            cache_ref = f'{self.cache}/{x}.json'
            with open(cache_ref) as f:
                refs.append(json.load(f))

        self.logger.debug(f'Loading attributes: {x+1}/{len(self.listfiles)}')
        with open(f'{self.cache}/temp_zattrs.json') as f:
            zattrs = json.load(f)
        return zattrs, refs

    def create_refs(self):
        self.logger.info('Starting computation')
        if not self.load_refs:
            allzattrs, refs = self.get_kerchunk_data()
            zattrs = self.correct_metadata(allzattrs)
        else:
            zattrs, refs = self.load_kdata()
            zattrs = self.correct_metadata(zattrs)

        try:
            if self.success:
                self.logger.info('Single conversions complete, starting concatenation')
                self.concat_data(refs, zattrs)
                if self.issave_meta:
                    self.save_meta(zattrs)
            else:
                self.logger.info('Issue with conversion unspecified - aborting process')
                self.save_cache(refs, zattrs)
        except TypeError as err:
            self.logger.error(f'Detected fatal error - {err}')
            raise err

if __name__ == '__main__':
    print('Serial Processor for Kerchunk Pipeline - run with single_run.py')
    