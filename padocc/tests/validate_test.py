from padocc.phases.validate_new import ValidateDatasets
file = '/badc/cmip6/data/CMIP6/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/ssp119/r1i1p1f2/3hr/huss/gr/v20190328/huss_3hr_CNRM-ESM2-1_ssp119_r1i1p1f2_gr_201501010300-203501010000.nc'
file2 = '/badc/cmip6/data/CMIP6/ScenarioMIP/CSIRO/ACCESS-ESM1-5/ssp245/r10i1p1f1/Amon/pr/gn/v20200810/pr_Amon_ACCESS-ESM1-5_ssp245_r10i1p1f1_gn_201501-210012.nc'
import xarray as xr
ds = xr.open_dataset(file)
ds2 = xr.open_dataset(file2)
print('Opened datasets')
vd = ValidateDatasets([ds,ds2],'test-valid', dataset_labels=['huss','pr'],verbose=0)
vd.validate_metadata()
vd.validate_data()

print(vd._data_report)
print(vd.pass_fail)