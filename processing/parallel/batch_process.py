from kerchunk import hdf, combine, df
import fsspec.implementations.reference
from fsspec.implementations.reference import LazyReferenceMapper
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt

import json

import xarray as xr
import os, sys

tasks = sys.argv[-1]
id = sys.argv[-2]
DEV = '/home/users/dwest77/Documents/kerchunk_dev/parquet/dev'
PATH = '/home/users/dwest77/Documents/kerchunk_dev/parquet/dev/batch/esacci9'
pq = f'{PATH}/batch30-{id}'
with open(f'{DEV}/filelists/test9.txt') as f:
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
    print('[INFO]', url)
    single_ref_sets.append(hdf.SingleHdf5ToZarr(url, inline_threshold=-1).translate())
    
out = LazyReferenceMapper.create(100, pq, fs = fsspec.filesystem("file"))

out_dict = combine.MultiZarrToZarr(
    single_ref_sets,
    remote_protocol="file",
    concat_dims=["time"],
    out=out).translate()

out.flush()

