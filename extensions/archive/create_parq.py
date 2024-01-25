from kerchunk import hdf, combine, df
import fsspec.implementations.reference
from fsspec.implementations.reference import LazyReferenceMapper
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt
import json
import xarray as xr
import os, sys

# Requires:
# - Filelist: list of filepaths to use for creating parquet files
# - Config file: contains parquet outdir, filelistdir, pqname, record_size

# python create_parq.py OUTDIR FILELISTDIR pqname 

# From config file
OUTDIR = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/test_parqs/parqs'
FILELISTDIR = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/test_parqs/filelists'

def make_parquet(pqname, record_size):
    print(pqname)
    pq = f'{OUTDIR}/{pqname}'
    with open(f'{FILELISTDIR}/{pqname}.txt') as f:
        files = [r.split('\n')[0] for r in f.readlines()]

    try:
        os.makedirs(pq)
    except:
        pass

    single_ref_sets = []
    for url in files:
        print('[INFO]', url)
        single_ref_sets.append(hdf.SingleHdf5ToZarr(url, inline_threshold=-1).translate())
        
    out = LazyReferenceMapper.create(record_size, pq, fs = fsspec.filesystem("file"))

    out_dict = combine.MultiZarrToZarr(
        single_ref_sets,
        remote_protocol="file",
        concat_dims=["time"],
        out=out
    ).translate()
    
    out.flush()

    print('Written refs to df', pq)

# From config file
pqname = sys.argv[-1]
record_size = 100
#try:
if True:
    if not os.path.isfile(f'{OUTDIR}/{pqname}/.zmetadata'):
        make_parquet(pqname, record_size)
    else:
        print('Zmetadata file already exists')
#except:
    #print('Error recorded for', pqname)