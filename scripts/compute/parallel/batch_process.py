from kerchunk import hdf, combine, df
import fsspec.implementations.reference
from fsspec.implementations.reference import LazyReferenceMapper
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt

import json

import xarray as xr
import os, sys

VERBOSE = True

def vprint(msg):
    if VERBOSE:
        print('[INFO]', msg)

tasks = sys.argv[-1]
id = sys.argv[-2]
DEV = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/'
PATH = '/gws/nopw/j04/esacci_portal/kerchunk/parq/ocean_daily_all_parts'
pq = f'{PATH}/batch{id}'
with open(f'{DEV}/test_parqs/filelists/gargant.txt') as f:
    files = [r.split('\n')[0] for r in f.readlines()]

fcount = len(files)
files_per_task = int(fcount / int(tasks))

subset = files[int(files_per_task*int(id)):int(files_per_task*(int(id)+1))]

try:
    os.makedirs(pq)
except:
    pass

single_ref_sets = []
for url in subset:
    vprint(url)
    single_ref_sets.append(hdf.SingleHdf5ToZarr(url, inline_threshold=-1).translate())
vprint('Kerchunked all files')
out = LazyReferenceMapper.create(100, pq, fs = fsspec.filesystem("file"))
vprint('Created Lazy Reference Mapper')
out_dict = combine.MultiZarrToZarr(
    single_ref_sets,
    remote_protocol="file",
    concat_dims=["time"],
    out=out).translate()
vprint('Written to Parquet Store')

out.flush()
vprint('Completed Flush')

