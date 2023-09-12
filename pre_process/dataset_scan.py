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

VERBOSE = '-v' in sys.argv

def vprint(msg):
    if VERBOSE:
        vprint(msg)

def count_refs(nfile):
    vprint(nfile)
    try:
        vprint('Using HDF5 reader')
        tdict = SingleHdf5ToZarr(nfile, inline_threshold=1).translate()
        return len(json.dumps(tdict['refs']))/1000, len(tdict['refs'].keys())
    except OSError:
        vprint('Switching to NetCDF3 reader')
        try:
            tdict = NetCDF3ToZarr(nfile, inline_threshold=1).translate()
            return len(json.dumps(tdict['refs']))/1000, len(tdict['refs'].keys())
        except:
            return False
        return False

def get_base_size(nfile):
    return os.stat(nfile).st_size

def recursive_findfiles(path,d, verbose=False):
    if verbose:
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
            if verbose:
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
        inner, refs = count_refs(os.path.join(path, nfiles[counter]))
        if not inner:
            vprint('Kerchunking failed on file', counter)
        counter += 1
        if counter > 100:
            inner = True
    if counter > 100:
        vprint('Kerchunk Aborted - Files not kerchunkable')
        return None
    return inner, refs


def scan_path(path, verbose, base_eval=False):
    if '*' in path:
        nfiles = sorted(glob.glob(path))
    else:
        if os.path.isfile(path):
            vprint('PathError: Please put single quotes around patterns with "*" wildcards')
            return None
        nfiles = recursive_findfiles(path, 1, verbose=verbose)

    if not len(nfiles) > 0:
        vprint('FileError: .nc file count is zero for ',path)
        return None

    bytetotal, refs = get_internals(path, nfiles)
    
    filecount = len(nfiles)
    nsizes    = getsizes(nfiles, path)
    refcount  = int(refs)*int(filecount) # Estimate total refs
    msize     = int(bytetotal)*int(filecount)*(np.mean(nsizes)/nsizes[0])/1000 # Memory in MB

    # Calculate errors
    err = np.std(nsizes)/(math.sqrt(len(nsizes))*np.mean(nsizes))

    if msize < 1:
        krating = 'Good'
    elif msize >= 1 and msize < 100:
        krating = 'Average'
    else:
        krating = 'Poor'
    bytecount = f'{bytetotal*1000/refs:.3f}'
    memsize   = f'{msize:.3f}'

    vprint(ASSESS.format(filecount, refs, bytecount, refcount, krating, memsize))

    if base_eval:
        meansize = f'{np.mean(sizes)/1000000:.3f}'
        maxsize  = f'{np.max(sizes)/1000000:.3f}'
        total    = f'{np.sum(sizes)/1000000000:.3f}'

        vprint(BASE.format(meansize, maxsize, total))

    multiplier = np.mean(nsizes)/nsizes[0]
    return filecount, multiplier, err
    # 30,000,000 ~ 5000 MB
    # Suitability - Good (<1MB), Medium (1MB-100MB), Poor(>100MB)

# Assume deal with the first file in a directory

directory = sys.argv[1] #'/badc/cmip6/data/CMIP6/ScenarioMIP/NCC//NorESM2-MM/ssp585/r1i1p1f1/Amon/va/gn/latest/'
nickname = None
if '-n' in sys.argv:
    for x in range(len(sys.argv)):
        if sys.argv[x] == '-n':
            nickname = sys.argv[x+1]

base_eval = '-b' in sys.argv

raw = '-r' in sys.argv

def main(directory):
    vprint('Assessment for ', directory)
    chunks_per_file, numfiles, total_ = scan_path(directory, verbose, base_eval=base_eval)
    if numfiles:

        if nickname:
            dsname = nickname
        else:
            dsname = '-'.join(directory.split('/')[1:3])
        
        with open(f'{dsname}-cfg.txt') as f:
            f.write(f'{chunks_per_file},{numfiles},{total_chunk_size}')

if '-f' in sys.argv:
    vprint('[INFO] Identifying file containing patterns')
    f = open(directory,'r')
    dirs = [q.replace('\n','') for q in f.readlines()]
    f.close()
    for d in dirs:
        main(d)
else:
    main(directory)
