from kerchunk import hdf, combine, df
import fsspec.implementations.reference
from fsspec.implementations.reference import LazyReferenceMapper
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt

import json

import xarray as xr
import os, sys
from datetime import datetime

PATH = '/home/users/dwest77/Documents/kerchunk_dev/parquet/dev/'
pq = f'{PATH}/batch/esacci_7'


parquet = '-p' in sys.argv

test_json = '/gws/nopw/j04/esacci_portal/kerchunk/kfiles/esacci4.json'
#@profile
def main(fname):
    if parquet:
        t1 = datetime.now()
        fs = fsspec.implementations.reference.ReferenceFileSystem(
            pq, remote_protocol='file', target_protocol="file", lazy=True)
        ds = xr.open_dataset(
            fs.get_mapper(), engine="zarr",
            backend_kwargs={"consolidated": False, "decode_times": False}
        )
        ds['chlor_a'][0,2059:2284, 4219:4444].plot()
        plt.savefig('img/largeparq.png')
        print(parquet, (datetime.now()-t1).total_seconds())
    else:
        t1 = datetime.now()
        mapper = fsspec.get_mapper('reference://', fo=fname)
        ds = xr.open_zarr(mapper)
       
main(sys.argv[1])
#print(ds['var1'])