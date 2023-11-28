## Tool for scanning a netcdf file or set of netcdf files for kerchunkability

# Determine total number of netcdf chunks in first file
# Determine number of netcdf files

# Calculate total number of chunks and output

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr
import os, sys
from datetime import datetime
import glob
import math
import json

import numpy as np

VERBOSE = False

def vprint(msg):
    if VERBOSE:
        print(msg)

def format_float(value, sfs):
    unit_index = -1
    units = ['K','M','G','T','P']
    while value > 1000:
        value = value / 1000
        unit_index += 1
    return f'{value:.2f} {units[unit_index]}B'

def get_refs(nfile):
    vprint(nfile)
    try:
        vprint('Using HDF5 reader')
        tdict = SingleHdf5ToZarr(nfile, inline_threshold=1).translate()
        return tdict['refs']
    except OSError:
        vprint('Switching to NetCDF3 reader')
        try:
            tdict = NetCDF3ToZarr(nfile, inline_threshold=1).translate()
            return tdict['refs']
        except:
            return False

def get_internals(testfile):
    refs = get_refs(testfile)
    sizes = []
    vars = {}
    chunks = 0
    for chunkkey in refs.keys():
        try:
            sizes.append(int(refs[chunkkey][2]))
            chunks += 1
            vars[chunkkey.split('/')[0]] = 1
        except:
            pass
    return np.sum(sizes), chunks, sorted(list(vars.keys()))

def make_filelist(pattern, proj_dir):
    if os.path.isdir(proj_dir):
        os.system(f'ls {pattern} > {proj_dir}/allfiles.txt')
    else:
        print(f'Error: Project Directory not located - {proj_dir}')

def eval_sizes(files):
    return [os.stat(files[count]).st_size for count in range(len(files))]

def main(files, proj_dir, proj_code):
    vprint('Assessment for ' + proj_code)
    success, escape, vs, is_varwarn, is_skipwarn = False, False, None, False, False
    count = 0
    cpf = []
    volms = []
    while not escape and len(cpf) < 5:
        print(f'Attempting file {count+1} (min 5, max 100)')
        # Add random file selector here
        try:
            volume, chunks_per_file, vars = get_internals(files[count])
            cpf.append(chunks_per_file)
            volms.append(volume)
            if not vs:
                vs = vars
            if vars != vs:
                print('Warning: Variables differ between files')
                is_varwarn = True
            print(f' > Data saved for file {count+1}')
        except:
            print(f'Skipped file {count} for unspecified issue')
            is_skipwarn = True
        if count >= 100:
            escape = True
        count += 1
    if count > 100:
        print('Filecount Exceeded: No valid files in first 100 tried')
    
    avg_cpf = sum(cpf)/len(cpf)
    avg_vol = sum(volms)/len(volms)
    avg_chunk = avg_vol/avg_cpf
    kchunk_const = 167 # Bytes per Kerchunk ref (standard/typical)

    spatial_res = 180*math.sqrt(2*len(vs)/avg_cpf)
    details = {
        'data_represented' : format_float(avg_vol*len(files), 2), 
        'num_files'        : str(len(files)),
        'chunks_per_file'  : f'{avg_cpf:.1f}',
        'total_chunks'     : f'{(avg_cpf * len(files)):.2f}',
        'estm_chunksize'   : format_float(avg_chunk,2),
        'estm_spatial_res' : f'{spatial_res:.2f} deg',
        'variable_count'   : len(vs),
        'addition'         : f'{kchunk_const*100/avg_chunk:.3f} %',
        'var_err'          : is_varwarn,
        'file_err'         : is_skipwarn,
        'type'             : 'JSON'
    }

    if escape:
        details['scan_status'] = 'FAILED'
    
    c2m = 1.67e-4 # Memory for each chunk in kerchunk in MB

    if avg_cpf * len(files) * c2m > 500e6:
        details['type':'parq']

    with open(f'{proj_dir}/detail-cfg.json','w') as f:
        # Replace with dumping dictionary
        f.write(json.dumps(details))
    vprint(f'Written config info to {proj_code}/detail-cfg.json')

def setup_main(args):
    cfg_file = f'{args.proj_dir}/base-cfg.json'
    if os.path.isfile(cfg_file):
        with open(cfg_file) as f:
            cfg = json.load(f)
    else:
        print(f'Error: cfg file missing or not provided - {cfg_file}')
        return None

    proj_code = cfg['proj_code']
    workdir   = cfg['workdir']
    proj_dir  = cfg['proj_dir']
    print(proj_code, workdir, proj_dir)

    try:
        pattern   = cfg['pattern']
    except KeyError:
        pattern = None


    filelist = f'{proj_dir}/allfiles.txt'
    if pattern:
        make_filelist(pattern, proj_dir)
    
    if not os.path.isfile(filelist):
        print('Error: No filelist detected - ',filelist)
        return None

    with open(filelist) as f:
        files = [r.strip() for r in f.readlines()]
        numfiles = len(files)
    if not os.path.isfile(f'{proj_dir}/detail-cfg.json') or args.forceful:
        main(files, proj_dir, proj_code)
    else:
        print('Skipped scanning - detailed config already exists')

# Assume deal with the first file in a directory

def get_proj_code(groupdir, pid):
    with open(f'{groupdir}/proj_codes.txt') as f:
        proj_code = f.readlines()[int(pid)].strip()
    return proj_code


def scan_files(args):

    print('Initialising Scan', args.proj_code)

    if args.groupID:
        if not args.groupdir:
            args.groupdir = f'{args.workdir}/groups/{args.groupID}'
        args.proj_code = get_proj_code(args.groupdir, proj_code)
        args.proj_dir = f'{args.workdir}/in_progress/{args.proj_code}'

    setup_main(args)