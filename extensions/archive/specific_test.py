import os
import xarray as xr
import json
from datetime import datetime
import sys
import fsspec
import random
import numpy as np
import glob

# Pick a specific variable
# Find number of dimensions
# Start in the middle
# Get bigger until we have a box that has non nan values

def vprint(msg,verb=False):
    if verb:
        print(msg)

def compare_xk(xbox, kbox, faillog, vname, step='', verb=False):
    vprint('Starting xk comparison',verb=verb)
    rtol = np.abs(np.nanmean(kbox))/20
    # Tolerance 0.1% of mean value for xarray set
    failed = False
    closeness = np.isclose(xbox, kbox, rtol=rtol, equal_nan=True)
    if closeness[closeness == False].size > 0:
        vprint(' > Failed elementwise comparison XXX',verb=verb)
        faillog.append(f'ElementwiseFail: {vname} - {step}')
        failed = True
    if np.abs(np.nanmax(kbox) - np.nanmax(xbox)) > rtol:
        vprint(' > Failed maximum comparison XXX',verb=verb)
        faillog.append(f'MaxFail: {vname} - {step}')
        failed = True
    if np.abs(np.nanmin(kbox) - np.nanmin(xbox)) > rtol:
        vprint(' > Failed minimum comparison XXX',verb=verb)
        faillog.append(f'MinFail: {vname} - {step}')
        failed = True
    if np.abs(np.nanmean(kbox) - np.nanmean(xbox)) > rtol:
        vprint(' > Failed mean comparison XXX',verb=verb)
        faillog.append(f'MeanFail: {vname} - {step}')
        failed = True
    if not failed:
        vprint('Passed all growbox tests',verb=verb)
    return (not failed), faillog 

def find_dimensions(dimlen, depth):
    # Round down then add 1
    slicemax = int(dimlen/depth)+1
    return slicemax

def test_growbox(xvariable, kvariable, vname, depth, faillog, verb=False):
    # Run growing-box test for specified variable from xarray and kerchunk datasets
    #if depth % 10 == 0:
        #vprint(depth, verb=verb)

    vslice = []
    if depth != 2:
        for x, dim in enumerate(xvariable.shape):
            dname = xvariable.dims[x]
            if np.issubdtype(xvariable[dname].dtype, np.datetime64):
                #vslice.append([0,find_dimensions(len(xvariable[dname]),depth)])
                vslice.append(slice(0,find_dimensions(len(xvariable[dname]),depth)))
            elif dim == 1:
                #vslice.append([0,1])
                vslice.append(slice(0,1))
            else:
                #vslice.append([0,find_dimensions(dim,depth)])
                vslice.append(slice(0,find_dimensions(dim,depth)))

        xbox = xvariable[tuple(vslice)]
        kbox = kvariable[tuple(vslice)]
    else:
        xbox = xvariable
        kbox = kvariable
    nmkbox = None
    try:
        nmkbox = np.nanmean(kbox)
        checknan = np.abs(nmkbox)     
    except TypeError as err:
        print(' > nanmean type error:',err)
        return None
    if kbox.size > 1 and checknan >= 0:
        # Evaluate kerchunk vs xarray and stop here
        vprint(f' > Found non-nan values with box-size: {int(kbox.size)}',verb=verb)
        return compare_xk(xbox, kbox, faillog, vname, verb=verb)
    else:
        if depth > 2:
            return test_growbox(xvariable, kvariable, vname, int(depth/2), faillog, verb=verb)
        else:
            print(f' > Failed to find non-NaN slice (depth: {depth}, var: {vname})')
            return 'soft-fail', faillog

workdir     = '/gws/nopw/j04/esacci_portal/kerchunk'
patterns = []
codes = []
with open(f'{workdir}/filesets.csv') as f:
    lines = [r.replace('\n','') for r in f.readlines()]
    patterns = [l.split(',')[0] for l in lines]
    codes    = [l.split(',')[1] for l in lines]

#testpattern = '/neodc/esacci/biomass/data/agb/maps/v4.0/netcdf/ESACCI-BIOMASS-L4-AGB-*MERGED*nc'
#proj_code   = 'ESACCI-BIOMASS-L4-AGB-MERGED-100m-2010-2020-fv4.0-kr1.0'
workdir     = '/gws/nopw/j04/esacci_portal/kerchunk/kfiles_1'

def test_single_time(xobj, kobj, faillog, step='', verb=False):
    # Test all variables
    vars = []
    for v in list(xobj.variables):
        if 'time' not in str(v) and str(xobj[v].dtype) in ['float32','int32','int64','float64']:
            vars.append(v)

    vars = ['Rrs_490']
    # Determine number of attempts to make - now testing all variables
    attempts = len(vars)

    totalpass = True
    softfail = True
    sizes = []
    print()
    print(' . Starting generic metadata tests .')
    for v in vars:
        if v not in list(kobj.variables):
            vprint(f'Test Failed: Variable missing from kerchunk dataset: {v}',verb=verb)
            faillog.append(f'VariableMissing: {v} - {step}')
            totalpass = False
        else:
            xshape = xobj[v].shape
            kshape = kobj[v].shape
            k1shape = kshape[1:]
            if xshape != kshape and xshape != k1shape:
                vprint(f'Test Failed: Kerchunk/Xarray shape mismatch for: {v} - ({xobj[v].shape}, {kobj[v].shape})',verb=verb)
                faillog.append(f'ShapeMismatch: {v} - ({xobj[v].shape}, {kobj[v].shape}) - {step}')
                totalpass = False
        sizes.append(xobj[v].size)
    if totalpass:
        print('All generic tests have passed, proceeding\n')
    else:
        print('One or more generic tests failed - aborting\n')
        return None, None, faillog
    
    # Make depth such that initial box has ~1000 items
    defaults = np.array(sizes)/10000
    defaults[defaults < 100] = 100
    print(f' . Starting growbox method ({attempts} variables).')
    tried_vars = []
    for index in range(attempts):
        if int(index/attempts) % 10:
            print(f' > {int((index/attempts)/10)}%')
        var =  vars[index]
        tried_times = 0
        while var in tried_vars and tried_times < 100 and len(tried_vars) < len(vars):
            var =  vars[random.randint(0,len(vars))]
            tried_times += 1
        tried_vars.append(var)

        vprint(f'\nTesting grow-box method for {var}',verb=verb)
        status, faillog = test_growbox(xobj[var],kobj[var], var, int(defaults[index]), faillog, verb=verb) 
        if status == 'soft-fail':
            vprint(f' > Grow-box failed softly for {var}',verb=verb)
            softfail = True
        elif status:
            vprint(f' > Grow-box method passed for {var}',verb=verb)
        else:
            vprint(f' > Grow-box method failed for {var} XXX',verb=verb)
            totalpass = False
    if totalpass:
        print('Growbox tests have passed, proceeding\n')
    else:
        print('One or more growbox tests failed - aborting\n')
        return totalpass, softfail, faillog
    return totalpass, softfail, faillog

def run_tests(testpattern, proj_code, workdir,verb=False, mem_override=False):
    kfile   = f'{workdir}/{proj_code}.json'
    if not os.path.isfile(kfile):
        print('no such file',kfile)
        return False

    print('Starting tests for',proj_code)
    print()
    xfiles = glob.glob(testpattern)
    # Open 3 random files in turn
    times = int(len(xfiles)/2)#/1000)
    if times < 3:
        times = 3
    indexes = []

    vprint(f'Opening kerchunk dataset',verb=verb)
    mapper  = fsspec.get_mapper('reference://',fo=kfile, target_options={"compression":None})
    kobj    = xr.open_zarr(mapper, consolidated=False)

    faillog = []

    for step in range(53,55):
        print(f'Running tests for file {step+1}/{times}')
        index = step #random.randint(0,len(xfiles)-1)
        #count = 0
        #while index in indexes or count > 100:
            #index = random.randint(0,len(xfiles)-1)
            #count += 1
        indexes.append(index)

        vprint(f'Opening xarray object {step+1}/{times}',verb=verb)
        if os.path.getsize(xfiles[index]) > 4e9 and not mem_override: # 3GB file
            print('Memory Exception warning - ensure you have 12GB or more dedicated to this task')
            return False, False, [f'MemException: {os.path.getsize(xfiles[index])}']
        xobj    = xr.open_dataset(xfiles[index])

        ksel    = kobj.sel(time=xobj.time)

        #print(f'{ksel.time} matched to {xobj.time}')

        totalpass, softfail, faillog = test_single_time(xobj, ksel, faillog, step=index, verb=verb)
        if not totalpass:
            vprint(f'Testing failed for {step+1}', verb=verb)
            return None, None, faillog
    
    if totalpass:
        print()
        print('All tests passed for',proj_code)
    return totalpass, softfail, faillog

id = sys.argv[1]

mem_override = ('-f' in sys.argv)

workdir = '/gws/nopw/j04/esacci_portal/kerchunk/kfiles_1'
tempdir = '/gws/nopw/j04/esacci_portal/kerchunk/temp'

with open(f'{tempdir}/configs/{id}.txt') as f:
    content = [r.replace('\n','') for r in f.readlines()]
pattern = content[0].split(',')[0]
code = content[0].split(',')[1]

verb    = True

if True:#not os.path.isfile(f'{tempdir}/flags/{code}_PASS.txt') and not os.path.isfile(f'{tempdir}/flags/{code}_SPASS.txt'):
    if os.path.isfile(f'{tempdir}/flags/{code}_FAIL.txt'):
        os.remove(f'{tempdir}/flags/{code}_FAIL.txt')
    pf, soft, log = run_tests(pattern, code, workdir, verb=verb, mem_override=mem_override)
    print('Run: ',pf)
    if not pf:
        fname = f'{code}_FAIL.txt'
    else:
        if soft:
            fname = f'{code}_SPASS.txt'
        else:
            fname = f'{code}_PASS.txt'
    with open(f'{tempdir}/flags/{fname}','w') as f:
        f.write('\n'.join(log))
else:
    print('Pass/Softpass detected - skipping')
