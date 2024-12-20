import netCDF4
import numpy as np
import os

def create_dims(ds, dims, ignore_attrs):

    for dim, meta in dims.items():

        d = ds.createDimension(
            dim,
            meta['size'],
        )

        if 'array' in meta:

            dc = ds.createVariable(
                dim,
                meta['dtype'],
                (dim,)
            )
            if not ignore_attrs:
                for k, v in meta['attrs'].items():
                    dc.setncattr(k, v)

            dc[:] = meta['array']

def create_vars(ds, vars, ignore_attrs):

    for var, meta in vars.items():

        v = ds.createVariable(
            var,
            meta['dtype'],
            meta['dims'],
            fill_value = 7.0,
        )

        if not ignore_attrs:
            for k,val in meta['attrs'].items():
                v.setncattr(k, val)

        v[:] = meta['array']

dims = {
    'time':{
        'size': 1,
        'dtype': int,
        'attrs':{
            'axis':'ABC',
            'units':'None'
        },
        'array':None
    },
    'lat':{
        'size':10,
        'dtype': int,
        'attrs':{
            'long_name': 'Latitude'
        },
        'array': None
    },
    'lon':{
        'size': 20,
        'dtype': int,
        'attrs': {
            'long_name':'Longitude',
            'standard_name':'lon',
        },
        'array':None
    },
    'axis_nbounds':{
        'size':2
    }
}

vars = {
    'height':{
        'array':[2],
        'attrs':{
            'ctype':'xrd'
        },
        'dtype':int,
        'dims':(),
    },
    'rain':{
        'dims': ('time', 'lat', 'lon'),
        'attrs':{
            'standard_name': 'rain',
            'long_name': 'Precipitation',
            'coordinates':'height'
        },
        'array':None,
        'dtype':float,
    },
    'lat_projection':{
        'dims': ('lat',),
        'attrs':{
            'projection': 'Polar',
            'binders':[1.0,2.0]
        },
        'array': None,
        'dtype': float,
    }
}

options = [
    '1DAgg',
    '3DAgg',
]

def main():

    counter = 0

    for option in range(len(options)):
        for i in range(8):
            
            print(option, i, options[option])

            ignore_attrs = False
            if option == 0:
                # 1D standard case
                dims['time']['array'] = dims['time']['size']*i + np.arange(dims['time']['size'], dtype=dims['time']['dtype'])
                dims['lat']['array'] = np.arange(dims['lat']['size'], dtype=dims['lat']['dtype'])
                dims['lon']['array'] = np.arange(dims['lon']['size'], dtype=dims['lon']['dtype'])

                vars['lat_projection']['array'] = np.arange(dims['lat']['size'], dtype=dims['lat']['dtype'])*0.7

                vars['rain']['array'] = np.random.rand(dims['time']['size'],dims['lat']['size'],dims['lon']['size'])
                vars['rain']['dims'] = ('time','lat','lon')

            if option == 1:

                # 3D case
                tm, lt, ln = list(format(i, '#05b')[2:])
                print(tm, lt, ln)

                time = np.split(np.arange(2),2)[int(tm)]
                lat = np.split(np.arange(20),2)[int(lt)]
                lon = np.split(np.arange(40),2)[int(ln)]

                dims['time']['array'] = time
                dims['lat']['array'] = lat
                dims['lon']['array'] = lon

                vars['lat_projection']['array'] = lat*0.7

                vars['rain']['array'] = np.random.rand(dims['time']['size'],dims['lat']['size'],dims['lon']['size'])
                vars['rain']['dims'] = ('time','lat','lon')

            if option == 2:
                if 'time' in dims:
                    del dims['time']
                dims['lat']['array']  = np.arange(dims['lat']['size'], dtype=dims['lat']['dtype'])
                dims['lon']['array']  = np.arange(dims['lon']['size'], dtype=dims['lon']['dtype'])

                vars['lat_projection']['array'] = np.arange(dims['lat']['size'], dtype=dims['lat']['dtype'])*0.7

                vars['rain']['array'] = np.random.rand(dims['lat']['size'],dims['lon']['size'])
                vars['rain']['dims'] = ('lat','lon')

            dsname = f'{options[option]}/file{i}.nc'

            if counter %5 == 0 and option > 0:
                ignore_attrs = True
            counter += 1

            if not os.path.isdir(options[option]):
                os.makedirs(options[option])

            ds = netCDF4.Dataset(dsname, format='NETCDF4', mode='w')

            ds.Conventions = 'DW-0.1'
            ds.nemo = 'alpha13'

            create_dims(ds, dims, ignore_attrs)
            create_vars(ds, vars, ignore_attrs)

            ds.close()

if __name__ == '__main__':
    main()

