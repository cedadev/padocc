from kerchunk import hdf, combine, df
import fsspec.implementations.reference
from fsspec.implementations.reference import LazyReferenceMapper
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt

import json

import xarray as xr
import os, sys

pq = 'test_parqs/parqs/esacci25'

fs = fsspec.implementations.reference.ReferenceFileSystem(
    pq, 
    remote_protocol='https', 
    target_protocol="file", 
    lazy=True)

ds = xr.open_dataset(
    fs.get_mapper(), 
    engine="zarr",
    backend_kwargs={"consolidated": False, "decode_times": False}
)

ds['sea_ice_thickness'].mean(dim='time').plot()
plt.savefig('ice_thickness.png')