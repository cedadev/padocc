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

def squeeze_dims(variable):
    # Correct 1-size dimensions:
    concat_axes = []
    for x, d in enumerate(variable.shape):
        if d == 1:
            concat_axes.append(x)
    for dim in concat_axes:
        variable = variable.squeeze(axis=dim)
    return variable

def get_vslice(shape, dtypes, lengths, depth):
    vslice = []
    for x, dim in enumerate(shape):
        if np.issubdtype(dtypes[x], np.datetime64):
            vslice.append(slice(0,find_dimensions(lengths[x],depth)))
        elif dim == 1:
            vslice.append(slice(0,1))
        else:
            vslice.append(slice(0,find_dimensions(dim,depth)))
    return vslice

def test_growbox(xvariable, kvariable, vname, depth, faillog, verb=False):
    # Run growing-box test for specified variable from xarray and kerchunk datasets
    #if depth % 10 == 0:
        #vprint(depth, verb=verb)
    if xvariable.size > 1 and kvariable.size > 1:
        xvariable = squeeze_dims(xvariable)
        kvariable = squeeze_dims(kvariable)

    vslice = []
    if depth != 2:
        shape = xvariable.shape
        dtypes  = [xvariable[xvariable.dims[x]].dtype for x in range(len(xvariable.shape))]
        lengths = [len(xvariable[xvariable.dims[x]]) for x in range(len(xvariable.shape))]
        vslice = get_vslice(shape, dtypes, lengths, depth)

        xbox = xvariable[tuple(vslice)]
        kbox = kvariable[tuple(vslice)]
    else:
        xbox = xvariable
        kbox = kvariable

    try:
        kb = np.array(kbox)
        isnan = np.all(kb!=kb)
    except TypeError as err:
        faillog.append(f'TypeError:{err} - nan conversion')
        isnan = True
    except KeyError as err:
        faillog.append(f'KeyError:{err} - check versions')
        isnan = True
    if kbox.size > 1 and not isnan:
        # Evaluate kerchunk vs xarray and stop here
        vprint(f' > Found non-nan values with box-size: {int(kbox.size)}',verb=verb)
        return compare_xk(xbox, kbox, faillog, vname, verb=verb)
    else:
        if depth > 2:
            return test_growbox(xvariable, kvariable, vname, int(depth/2), faillog, verb=verb)
        else:
            print(f' > Failed to find non-NaN slice (depth: {depth}, var: {vname})')
            return 'soft-fail', faillog

def test_single_time(xobj, kobj, faillog, step='', verb=False):
    # Test all variables
    vars = []
    for v in list(xobj.variables):
        if 'time' not in str(v) and str(xobj[v].dtype) in ['float32','int32','int64','float64']:
            vars.append(v)

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

def get_select_files(testpattern):
    xfiles = glob.glob(testpattern)
    # Open 3 random files in turn
    numfiles = int(len(xfiles)/1000)
    if numfiles < 3:
        numfiles = 3
    if numfiles > len(xfiles):
        numfiles = len(xfiles)

    return xfiles, numfiles

def open_kerchunk(kfile, isparq=False):
    if isparq:
        kobj = None
    else:
        mapper  = fsspec.get_mapper('reference://',fo=kfile, target_options={"compression":None})
        kobj    = xr.open_zarr(mapper, consolidated=False)
    return kobj

def pick_index(nfiles, indexes):
    index = random.randint(0,nfiles)
    count = 0
    while index in indexes and count < 100:
        index = random.randint(0,nfiles)
        count += 1
    indexes.append(index)
    return indexes

def run_tests(testpattern, proj_code, workdir,verb=False, mem_override=False):
    kfile   = f'{workdir}/{proj_code}.json'
    if not os.path.isfile(kfile):
        print('no such file',kfile)
        return False

    print('Starting tests for',proj_code)
    print()
    xfiles, numfiles = get_select_files(testpattern)
    indexes = []

    vprint(f'Opening kerchunk dataset',verb=verb)
    kobj = open_kerchunk(kfile)

    totalpass = True
    faillog = []

    for step in range(numfiles):
        print(f'Running tests for file {step+1}/{numfiles}')
        indexes = pick_index(len(xfiles)-1, indexes)
        index   = indexes[-1]

        vprint(f'Opening xarray object {step+1}/{numfiles}',verb=verb)

        # Memory Size Check
        if os.path.getsize(xfiles[index]) > 4e9 and not mem_override: # 3GB file
            print('Memory Exception warning - ensure you have 12GB or more dedicated to this task')
            faillog.append(f'MemException: {os.path.getsize(xfiles[index])}, file {index}')
            return False, False, faillog

        # Temporal Aquisition Check
        xobj = xr.open_dataset(xfiles[index])
        try:
            ksel = kobj.sel(time=xobj.time)
        except:
            try:
                ksel = kobj.isel(time=index)
            except:
                vprint('Temporal Selection Error: Unable to select time')
                faillog.append(f'TemporalError: file {index}')
                return False, False, faillog

        # Proceed with testing
        testpass, softfail, faillog = test_single_time(xobj, ksel, faillog, step=index, verb=verb)
        if not testpass:
            vprint(f'Testing failed for {step+1}', verb=verb)
        totalpass = totalpass and testpass
    
    if totalpass:
        print()
        print('All tests passed for',proj_code)
    return totalpass, softfail, faillog

id = sys.argv[1]

mem_override = ('-f' in sys.argv)

workdir = '/gws/nopw/j04/esacci_portal/kerchunk/kfiles_1.1'
tempdir = '/gws/nopw/j04/esacci_portal/kerchunk/temp'

with open(f'{tempdir}/configs/{id}.txt') as f:
    content = [r.replace('\n','') for r in f.readlines()]
pattern  = content[0].split(',')[0]
code     = f'{content[0].split(",")[1]}-{content[0].split(",")[2]}'

verb     = ('-v' in sys.argv)

nosave   = ('-n' in sys.argv)

if not os.path.isfile(f'{tempdir}/flags/{code}_PASS.txt') and not os.path.isfile(f'{tempdir}/flags/{code}_SPASS.txt'):
    if os.path.isfile(f'{tempdir}/flags/{code}_FAIL.txt'):
        os.remove(f'{tempdir}/flags/{code}_FAIL.txt')
    pf, soft, log = run_tests(pattern, code, workdir, verb=verb, mem_override=mem_override)
    if not pf:
        fname = f'{code}_FAIL.txt'
    else:
        if soft:
            fname = f'{code}_SPASS.txt'
        else:
            fname = f'{code}_PASS.txt'
    if not nosave:
        with open(f'{tempdir}/flags/{fname}','w') as f:
            f.write('\n'.join(log))
else:
    print('Pass/Softpass detected - skipping')
