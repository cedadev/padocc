import fsspec
import xarray as xr
kfile = '/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline/in_progress/CMIP6_rel1_6233/CMIP_BCC_BCC-CSM2-MR_historical_r1i1p1f1_Amon_huss_gn_v20181126/kerchunk-1c.json'
kfile2 = '/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline/complete/CMIP6_rel1_6233/CMIP_AS-RCEC_TaiESM1_historical_r1i1p1f1_3hr_clt_gn_v20201013-kr1.0.json'
mapper  = fsspec.get_mapper('reference://',fo=kfile, target_options={"compression":None})
# Need a safe repeat here
ds = xr.open_zarr(mapper, consolidated=False, decode_times=True)
print(ds.time)
print('Pass')