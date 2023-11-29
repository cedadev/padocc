# Borrows from kerchunk tools but with more automation
import os
import json
import sys

WORKDIR = None
CONCAT_MSG = 'See individual files for more details'

def output(msg,verb=True, mode=None, log=None, pref=0):
    prefixes = ['INFO','ERR']
    prefix = prefixes[pref]
    if verb:
        if mode == 'log':
            log += f'{prefix}: {msg}\n'
        else:
            print(f'>> {prefix}: {msg}')
    return log

class Indexer:

    def __init__(self, 
                 proj_code, 
                 cfg_file=None, detail_file=None, workdir=WORKDIR, 
                 issave_meta=False, refresh='', forceful=False, 
                 verb=False, mode=None, version_no=1,
                 concat_msg=CONCAT_MSG):
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

        self.log = output('Loading config information', verb=self.verb, mode=self.mode, log=self.log)
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

        self.outfile = f'{self.proj_dir}/kerchunk-{version_no}a.json'
        self.outstore = f'{self.proj_dir}/kerchunk-{version_no}a.parq'
        self.record_size = 167 # Default

        self.filelist = f'{self.proj_dir}/allfiles.txt'

        self.cache    = f'{self.proj_dir}/cache/'
        if refresh == '' and os.path.isfile(f'{self.cache}/temp_zattrs.json'):
            # Load data instead of create from scratch
            self.load_refs = True
        if not os.path.isdir(self.cache):
            os.makedirs(self.cache)

        self.success  = True

        self.combine_kwargs = {}
        self.create_kwargs  = {}
        self.pre_kwargs     = {}

        self.get_files()
        self.log = output('Finished all setup steps', verb=self.verb, mode=self.mode, log=self.log)

    def get_files(self):
        with open(self.filelist) as f:
            self.listfiles = [r.strip() for r in f.readlines()]

        self.limiter = len(self.listfiles)

    def hdf5_to_zarr(self, nfile, **kwargs):
        from kerchunk.hdf import SingleHdf5ToZarr
        try:
            return SingleHdf5ToZarr(nfile, **kwargs).translate()
        except:
            return None

    def ncf3_to_zarr(self, nfile, **kwargs):
        from kerchunk.netCDF3 import NetCDF3ToZarr
        try:
            return NetCDF3ToZarr(nfile, **kwargs).translate()
        except:
            return None
        
    def tiff_to_zarr(self, tfile, **kwargs):
        self.log = output('Tiff conversion not yet implemented - aborting', mode=self.mode, log=self.log, pref=1)
        raise

    def concat_data(self, refs, zattrs):
        if self.use_json:
            self.data_to_json(refs, zattrs)
        else:
            self.data_to_parq(refs)

    def data_to_parq(self, refs):
        from kerchunk.combine import MultiZarrToZarr
        from fsspec.implementations.reference import LazyReferenceMapper
        import fsspec

        self.log = output('Starting parquet-write process', verb=self.verb, mode=self.mode, log=self.log)

        if not os.path.isdir(self.outstore):
            os.makedirs(self.outstore)
        out = LazyReferenceMapper.create(self.record_size, self.outstore, fs = fsspec.filesystem("file"), **self.pre_kwargs)

        out_dict = MultiZarrToZarr(
            refs,
            out=out,
            remote_protocol='file',
            concat_dims=['time'],
            **self.combine_kwargs
        ).translate()
        
        out.flush()
        self.log = output('Written to parquet store', verb=self.verb, mode=self.mode, log=self.log)

    def add_kerchunk_history(self, attrs):
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

    def data_to_json(self, refs, zattrs):
        from kerchunk.combine import MultiZarrToZarr

        self.log = output('Starting JSON-write process', verb=self.verb, mode=self.mode, log=self.log)

        # Already have default options saved to class variables
        mzz = MultiZarrToZarr(refs, concat_dims=['time'], **self.combine_kwargs).translate()
        # Override global attributes

        # Needs but must be fixed
        zattrs = self.add_kerchunk_history(zattrs)
        mzz['refs']['.zattrs'] = json.dumps(zattrs)

        with open(self.outfile,'w') as f:
            f.write(json.dumps(mzz))

        self.log = output('Written to JSON file', verb=self.verb, mode=self.mode, log=self.log)

    def correct_meta(self, allzattrs):
        # General function for correcting metadata
        # - Combine all existing metadata in standard way
        # - Add updates and remove removals specified by configuration
        self.log = output('Starting metadata corrections', verb=self.verb, mode=self.mode, log=self.log)
        zattrs = self.combine_meta(allzattrs)
        self.log = output('Applying config info on updates and removals', verb=self.verb, mode=self.mode, log=self.log)

        if self.updates:
            for update in self.updates.keys():
                zattrs[update] = self.updates[update]
        new_zattrs = {}
        if self.removals:
            for key in zattrs:
                if key not in self.removals:
                    new_zattrs[key] = zattrs[key]
        self.log = output('Finished metadata corrections', verb=self.verb, mode=self.mode, log=self.log)
        return zattrs
        
    def combine_meta(self, allzattrs):
        # Collect attributes from all files, 
        # determine which are always equal, which have differences
        base = json.loads(allzattrs[0])

        self.log = output('Correcting time attributes', verb=self.verb, mode=self.mode, log=self.log)
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

        base = {**base, **self.adjust_time_meta(times)}
        self.log = output('Comparing nonequal keys', verb=self.verb, mode=self.mode, log=self.log)

        for attr in nonequal.keys():
            if len(set(all_values[attr])) == 1:
                base[attr] = all_values[attr][0]
            else:
                base[attr] = self.concat_msg
        if len(nonequal.keys()) > 0:
            self.success = False
        return base

    def adjust_time_meta(self, times):
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
        self.log = output('Finished time corrections', verb=self.verb, mode=self.mode, log=self.log)
        return combined

    def save_meta(self,zattrs):
        with open(f'{self.cache}/temp_zattrs.json','w') as f:
            f.write(json.dumps(zattrs))
        self.log = output('Saved global attribute cache', verb=self.verb, mode=self.mode, log=self.log)

    def save_singles(self, refs):
        for x, r in enumerate(refs):
            cache_ref = f'{self.cache}/{x}.json'
            with open(cache_ref,'w') as f:
                f.write(json.dumps(r))
        self.log = output('Saved metadata cache', verb=self.verb, mode=self.mode, log=self.log)
        # All file content saved for later reconcatenation

    def safe_create(self, nfile, **kwargs):
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
            self.log = output(f'Creating refs: {x+1}/{len(self.listfiles)}', verb=self.verb, mode=self.mode, log=self.log)
            zarr_content = self.safe_create(nfile, **self.create_kwargs)
            if zarr_content:
                allzattrs.append(zarr_content['refs']['.zattrs'])
                refs.append(zarr_content)
        return allzattrs, refs
    
    def load_kdata(self):
        refs = []
        for x, nfile in enumerate(self.listfiles[:self.limiter]):
            self.log = output(f'Loading refs: {x+1}/{len(self.listfiles)}', verb=self.verb, mode=self.mode, log=self.log)
            cache_ref = f'{self.cache}/{x}.json'
            with open(cache_ref) as f:
                refs.append(json.load(f))

        self.log = output(f'Loading attributes: {x+1}/{len(self.listfiles)}', verb=self.verb, mode=self.mode, log=self.log)
        with open(f'{self.cache}/temp_zattrs.json') as f:
            zattrs = json.load(f)
        return zattrs, refs

    def create_refs(self):
        self.log = output('Starting conversion', verb=self.verb, mode=self.mode, log=self.log)
        if not self.load_refs:
            allzattrs, refs = self.get_kerchunk_data()
            zattrs = self.correct_meta(allzattrs)
        else:
            zattrs, refs = self.load_kdata()
            zattrs = self.correct_meta(zattrs)

        try:
            if self.success:
                self.log = output('Single conversions complete, starting concatenation', verb=self.verb, mode=self.mode, log=self.log)
                self.concat_data(refs, zattrs)
                if self.issave_meta:
                    self.save_meta(zattrs)
            else:
                self.log = output('Issue with conversion unspecified - aborting process', mode=self.mode, log=self.log, pref=1)
                self.save_meta(zattrs)
                self.save_singles(refs)
        except TypeError as err:
            print(err)
            raise

if __name__ == '__main__':
    print('Serial Processor for Kerchunk Pipeline - run with single_run.py')
    