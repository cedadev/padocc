import xarray as xr
import json

sets = [
    '/neodc/esacci/biomass/data/agb/maps/v4.0/netcdf/ESACCI-BIOMASS-L4-AGB-*MERGED*nc',
    '/neodc/esacci/biomass/data/agb/maps/v4.0/netcdf/ESACCI-BIOMASS-L4-AGB-*CHANGE*nc',
    '/neodc/esacci/cloud/data/version3/L3C/ATSR2-AATSR/v3.0/*/*/*nc',
    '/neodc/esacci/cloud/data/version3/L3C/AVHRR-AM/v3.0/*/*/*nc',
    '/neodc/esacci/cloud/data/version3/L3C/AVHRR-PM/v3.0/*/*/*nc',
    '/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/all_products/monthly/v6.0/*/*nc',
    '/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/chlor_a/monthly/v6.0/*/*nc',
    '/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/iop/monthly/v6.0/*/*nc',
    '/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/kd/monthly/v6.0/*/*nc',
    '/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/rrs/monthly/v6.0/*/*nc',
    '/neodc/esacci/snow/data/swe/MERGED/v2.0/*/*/*nc',
    '/neodc/esacci/land_cover/data/land_cover_maps/v2.0.7/*nc',
    '/neodc/esacci/land_cover/data/pft/v2.0.8/*nc',
    '/neodc/esacci/land_surface_temperature/data/TERRA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_DAY*.nc',
    '/neodc/esacci/land_surface_temperature/data/TERRA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_NIGHT*.nc',
    '/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_NIGHT*.nc',
    '/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_DAY*.nc',
    '/neodc/esacci/land_surface_temperature/data/MULTISENSOR_IRCDR/L3S/0.01/v2.00/monthly/*/*/ESACCI-LST-L3S-LST-IRCDR_-0.01deg_1MONTHLY_*DAY*nc',
    '/neodc/esacci/land_surface_temperature/data/MULTISENSOR_IRCDR/L3S/0.01/v2.00/monthly/*/*/ESACCI-LST-L3S-LST-IRCDR_-0.01deg_1MONTHLY_*NIGHT*nc',
    '/neodc/esacci/permafrost/data/active_layer_thickness/L4/area4/pp/v03.0/*nc',
    '/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/cryosat2/v2.0/NH/*/ESACCI-SEAICE-L3C-SITHICK-SIRAL_CRYOSAT2-NH25KMEASE2-*nc',
    '/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/cryosat2/v2.0/SH/*/ESACCI-SEAICE-L3C-SITHICK-SIRAL_CRYOSAT2*nc',
    '/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/envisat/v2.0/SH/*/*nc',
    '/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/envisat/v2.0/NH/*/*nc',
    '/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/ssmi_ssmis/12.5km/v3.0/NH/*/*/*nc',
    '/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/ssmi_ssmis/12.5km/v3.0/SH/*/*/*nc',
    '/neodc/esacci/fire/data/burned_area/MODIS/grid/v5.1/*/*nc',
    '/neodc/esacci/soil_moisture/data/daily_files/ACTIVE/v07.1/*/*nc',
    '/neodc/esacci/soil_moisture/data/daily_files/PASSIVE/v07.1/*/*nc',
    '/neodc/esacci/soil_moisture/data/daily_files/COMBINED/v07.1/*/*nc',
    '/neodc/esacci/soil_moisture/data/daily_files/break_adjusted_COMBINED/v07.1/*/*nc'
]

for x in range(2,33):
    pattern = sets[x-2]
    path_to_kerchunk = f'/gws/nopw/j04/esacci_portal/kerchunk/kfiles/esacci{x}.json'
    print(path_to_kerchunk)

    with open(path_to_kerchunk) as f:
        refs = json.load(f)

    krefs = json.loads(refs['refs']['.zattrs'])

    dsrefs = xr.open_mfdataset(pattern).attrs

    missing, noneql, eql = 0, 0, 0

    for key in dsrefs.keys():
        if key not in krefs:
            missing += 1
        else:
            if krefs[key] != dsrefs[key]:
                noneql += 1
            else:
                eql += 1

    print('Missing Attrs:', missing)
    print('Non Equal Attrs:', noneql)
    print('Equal Attrs:', eql)
    print()


