# Borrows from kerchunk tools but with more automation
import os
import json
import sys

WORKDIR = '/gws/nopw/j04/esacci_portal/kerchunk/pipeline'

def rundecode(cfgs):
    """
    cfgs - list of command inputs depending on user input to this program
    """
    flags = {
        '-w':'workdir'
    }
    kwargs = {}
    for x in range(0,int(len(cfgs)),2):
        try:
            flag = flags[cfgs[x]]
            kwargs[flag] = cfgs[x+1]
        except KeyError:
            print('Unrecognised cmdarg:',cfgs[x:x+1])

    return kwargs

class Indexer:

    def __init__(self, proj_code, workdir=WORKDIR, issave_meta=False):
        self.workdir   = workdir
        self.proj_code = proj_code

        self.issave_meta = issave_meta
        self.updates, self.removals = False, False

        cfg_file = f'{workdir}/in_progress/{proj_code}/base-cfg.json'
        if os.path.isfile(cfg_file):
            with open(cfg_file) as f:
                cfg = json.load(f)
        else:
            print(f'Error: cfg file missing or not provided - {cfg_file}')
            return None
        
        detail_file = f'{workdir}/in_progress/{proj_code}/detail-cfg.json'
        if os.path.isfile(detail_file):
            with open(detail_file) as f:
                detail = json.load(f)
        else:
            print(f'Error: cfg file missing or not provided - {detail_file}')
            return None
        
        self.proj_dir = cfg['proj_dir']

        if 'update' in cfg:
            self.updates = cfg['update']
        if 'remove' in cfg:
            self.removals = cfg['remove']

        if 'type' in detail:
            self.use_json = (detail['type'] == 'JSON')
        else:
            self.use_json = True

        if self.use_json:
            self.outfile = f'{self.proj_dir}/kerchunk-1a.json'
        else:
            self.outstore = f'{self.proj_dir}/kerchunk-1a.parq'
            self.record_size = 100 # Default

        self.filelist = f'{self.proj_dir}/allfiles.txt'
        self.cache    = f'{self.proj_dir}/cache/'
        if not os.path.isdir(self.cache):
            os.makedirs(self.cache)

        self.success  = True

        self.combine_kwargs = {}
        self.create_kwargs  = {}
        self.pre_kwargs     = {}

        self.get_files()

    def get_files(self):
        print('Opening files')
        with open(self.filelist) as f:
            self.listfiles = [r.strip() for r in f.readlines()]

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
        return None

    def concat_data(self, refs, zattrs):
        if self.use_json:
            self.data_to_parq(refs, zattrs)
        else:
            self.data_to_json(refs, zattrs)

    def data_to_parq(self, refs, zattrs):
        from kerchunk.combine import MultiZarrToZarr
        from fsspec.implementations.reference import LazyReferenceMapper
        import fsspec

        out = LazyReferenceMapper.create(self.record_size, self.outstore, fs = fsspec.filesystem("file"), **self.pre_kwargs)

        out_dict = MultiZarrToZarr(
            refs,
            out=out,
            **self.combine_kwargs
        ).translate()
        
        out.flush()

    def data_to_json(self, refs, zattrs):
        from kerchunk.combine import MultiZarrToZarr

        # Already have default options saved to class variables
        mzz = MultiZarrToZarr(refs, **self.combine_kwargs)
        # Override global attributes
        mzz['refs']['.zattrs'] = json.dumps(zattrs)
        with open(self.outfile,'w') as f:
            f.write(json.dumps(mzz))

    def correct_meta(self, allzattrs):
        # General function for correcting metadata
        # - Combine all existing metadata in standard way
        # - Add updates and remove removals specified by configuration
        zattrs = self.combine_meta(allzattrs)

        if self.updates:
            for update in self.updates.keys():
                zattrs[update] = self.updates[update]
        new_zattrs = {}
        if self.removals:
            for key in zattrs:
                if key not in self.removals:
                    new_zattrs[key] = zattrs[key]
        return zattrs

    def combine_meta(self, allzattrs):
        # Collect attributes from all files, 
        # determine which are always equal, which have differences
        base = json.loads(allzattrs[0])
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

        for attr in nonequal.keys():
            if len(set(all_values[attr])) == len(self.listfiles):
                base[attr] = 'See individual files for details'
            elif len(set(all_values[attr])) == 1:
                base[attr] = all_values[attr][0]
            else:
                base[attr] = list(set(all_values[attr]))
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
        return combined

    def save_meta(self,zattrs):
        with open(f'{self.cache}/temp_zattrs.json','w') as f:
            f.write(json.dumps(zattrs))

    def save_singles(self, refs):
        for x, r in enumerate(refs):
            cache_ref = f'{self.cache}/{x}.json'
            with open(cache_ref,'w') as f:
                f.write(json.dumps(r))
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
    
    def create_refs(self):
        refs = []
        allzattrs = []
        for x, nfile in enumerate(self.listfiles):
            print(f'Creating refs: {x+1}/{len(self.listfiles)}')
            zarr_content = self.safe_create(nfile, **self.create_kwargs)
            if zarr_content:
                allzattrs.append(zarr_content['refs']['.zattrs'])
                refs.append(zarr_content)

        # Concat dims
        zattrs = self.correct_meta(allzattrs)
        try:
            if self.success:
                self.concat_data(refs, zattrs)
                if self.issave_meta:
                    self.save_meta(zattrs)
            else:
                self.save_meta(zattrs)
                self.save_singles(refs)
        except:
            print('Something went wrong')
            self.save_singles(refs, zattrs)

if __name__ == '__main__':
    proj_code = sys.argv[1]
    kwargs = rundecode(sys.argv[2:])
    Indexer(proj_code, **kwargs).create_refs()
    