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

VERBOSE = True

def vprint(msg):
    if VERBOSE:
        print(msg)

def count_refs(nfile):
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
        return False

def get_base_size(nfile):
    return os.stat(nfile).st_size

def recursive_findfiles(path,d):
    vprint('Recursive file search level:',d)
    nfiles = []
    files = os.listdir(path)
    for f in files:
        fpath = os.path.join(path, f)
        if f.endswith('.nc'):
            nfiles.append(fpath)
        elif os.path.isdir(fpath):
            nfiles = nfiles + recursive_findfiles(fpath,d + 1, verbose=verbose)
        else:
            vprint('Ignoring path',fpath)
    return nfiles

def getsizes(nfiles, path):
    nsizes = []
    for f in nfiles:
        nsizes.append(os.stat(os.path.join(path,f)).st_size)
    return nsizes

def get_internals(path, nfiles):
    inner = False
    counter = 0
    while not inner:
        try:
            refs = count_refs(os.path.join(path, nfiles[counter]))
            inner = True
        except:
            pass
        if counter > 100:
            inner = True
    if counter > 100:
        vprint('Kerchunk Aborted - Files not kerchunkable')
        return None
    sizes = []
    chunks = 0
    for chunkkey in refs.keys():
        try:
            sizes.append(int(refs[chunkkey][2]))
            chunks += 1
        except:
            pass
    return sizes, chunks


def scan_path(path, base_eval=False):
    if '*' in path:
        nfiles = sorted(glob.glob(path))
    else:
        if os.path.isfile(path):
            vprint('PathError: Please put single quotes around patterns with "*" wildcards')
            return None
        nfiles = recursive_findfiles(path, 1)

    if not len(nfiles) > 0:
        vprint('FileError: .nc file count is zero for ',path)
        return None

    sizes, chunks_per_file = get_internals(path, nfiles)
    filecount = len(nfiles)
    total_chunk_size = np.sum(sizes)

    return chunks_per_file, filecount, total_chunk_size


def main(directory, outdir, nickname):
    vprint('Assessment for '+directory)
    chunks_per_file, numfiles, total_chunk_size = scan_path(directory, base_eval=base_eval)
    if numfiles:

        if nickname:
            dsname = nickname
        else:
            dsname = '-'.join(directory.split('/')[1:3])
        with open(f'{outdir}/configs/{dsname}-cfg.txt','w') as f:
            f.write(f'{chunks_per_file},{numfiles},{total_chunk_size}')
        vprint(f'Written config info to {dsname}-cfg.txt')

# Assume deal with the first file in a directory
if __name__ == '__main__':
    kstore_name = sys.argv[-1]
    pattern = sys.argv[-2] #'/badc/cmip6/data/CMIP6/ScenarioMIP/NCC//NorESM2-MM/ssp585/r1i1p1f1/Amon/va/gn/latest/'
    try:
        outdir = os.environ['OUTDIR']
    except KeyError:
        outdir = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/test_parqs'

    os.system(f'ls {pattern} > {outdir}/filelists/{kstore_name}.txt')

    nickname = None
    if '-n' in sys.argv:
        for x in range(len(sys.argv)):
            if sys.argv[x] == '-n':
                nickname = sys.argv[x+1]

    base_eval = '-b' in sys.argv

    raw = '-r' in sys.argv

    if '-f' in sys.argv:
        vprint('[INFO] Identifying file containing patterns')
        f = open(pattern,'r')
        dirs = [q.replace('\n','') for q in f.readlines()]
        f.close()
        for d in dirs:
            main(d, outdir, kstore_name)
    else:
        main(pattern, outdir, kstore_name)
