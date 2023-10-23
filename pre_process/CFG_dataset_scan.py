## Tool for scanning a netcdf file or set of netcdf files for kerchunkability

# Determine total number of netcdf chunks in first file
# Determine number of netcdf files

# Calculate total number of chunks and output

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr
import os, sys
from datetime import datetime
import glob
import math
import json

import numpy as np

# Quick open a netcdf file

WORKDIR = '/gws/nopw/j04/esacci_portal/kerchunk/pipeline'

VERBOSE = True

def vprint(msg):
    if VERBOSE:
        print(msg)

def get_refs(nfile):
    vprint(nfile)
    try:
        vprint('Using HDF5 reader')
        tdict = SingleHdf5ToZarr(nfile, inline_threshold=1).translate()
        return tdict['refs']
    except OSError:
        vprint('Switching to NetCDF3 reader')
        try:
            tdict = NetCDF3ToZarr(nfile, inline_threshold=1).translate()
            return tdict['refs']
        except:
            return False

def get_internals(testfile):
    refs = get_refs(testfile)
    sizes = []
    chunks = 0
    for chunkkey in refs.keys():
        try:
            sizes.append(int(refs[chunkkey][2]))
            chunks += 1
        except:
            pass
    return np.sum(sizes), chunks, True

def make_filelist(pattern, proj_code, workdir):
    proj_dir = f'{workdir}/in_progress/{proj_code}'
    if os.path.isdir(proj_dir):
        os.system(f'ls {pattern} > {proj_dir}/allfiles.txt')
    else:
        print(f'Error: Project Directory not located - {proj_dir}')

def main(files, proj_dir, proj_code):
    vprint('Assessment for ' + proj_code)
    success = False
    count = 0
    cpf = []
    volms = []
    while not success or len(cpf) < 5:
        print(f'Attempting file {count+1} (min 5, max 100)')
        # Add random file selector here
        try:
            volume, chunks_per_file, success = get_internals(files[count])
            cpf.append(chunks_per_file)
            volms.append(volume)
            print(f' > Data saved for file {count+1}')
        except:
            if count >= 100:
                success = True
        if len(cpf) >= 5:
            success = True
        count += 1
    if count > 100:
        print('Filecount Exceeded: No valid files in first 100 tried')
        return None
    
    avg_cpf = sum(cpf)/len(cpf)
    avg_vol = sum(volms)/len(volms)
    details = {
        'data_represented': f'{avg_vol*len(files):.1f}', 
        'chunks_per_file': f'{avg_cpf:.1f}',
        'num_files': str(len(files)),
        'total_chunks': str(avg_cpf * len(files)),
        'addition': f'{(avg_cpf * len(files) * 100)/os.stat(files[count]).st_size:.3f}',
        'type': 'JSON'}
    
    c2m = 1.67e-4 # Memory for each chunk in kerchunk in MB

    if avg_cpf * len(files) * c2m > 500e6:
        details['type':'parq']

    with open(f'{proj_dir}/detail-cfg.json','w') as f:
        # Replace with dumping dictionary
        f.write(json.dumps(details))
    vprint(f'Written config info to {proj_code}/detail-cfg.json')

def setup_main(proj_code, workdir=WORKDIR):
    cfg_file = f'{workdir}/in_progress/{proj_code}/base-cfg.json'
    if os.path.isfile(cfg_file):
        with open(cfg_file) as f:
            cfg = json.load(f)
    else:
        print(f'Error: cfg file missing or not provided - {cfg_file}')
        return None

    proj_code = cfg['proj_code']
    workdir   = cfg['workdir']
    proj_dir  = cfg['proj_dir']

    try:
        pattern   = cfg['pattern']
    except KeyError:
        pattern = None


    filelist = f'{proj_dir}/allfiles.txt'
    if pattern:
        make_filelist(pattern, proj_code, workdir)
    
    if not os.path.isfile(filelist):
        print('Error: No filelist detected - ',filelist)
        return None
    
    base_eval = '-b' in flags
    raw = '-r' in flags

    with open(filelist) as f:
        files = [r.strip() for r in f.readlines()]
        numfiles = len(files)

    main(files, proj_dir, proj_code)

# Assume deal with the first file in a directory

known_flags = {
    '-w': 'workdir'
}

if __name__ == '__main__':
    proj_code = None
    try:
        proj_code = sys.argv[1]
    except:
        print('Error: No project code given - exiting')

    if proj_code:
        flags = {}
        try:
            for x in range(len(sys.argv[2:])):
                if sys.argv[x] in known_flags:
                    flags[known_flags[x]] = sys.argv[x+1]
        except IndexError:
            # No Flags
            flags = {}

        if os.getenv('KERCHUNK_DIR'):
            flags['workdir'] = os.getenv('KERCHUNK_DIR')

        setup_main(proj_code, **flags)

    
