# -*- coding: utf-8 -*-
import sys
import json
import os
import random

config = {
    'proj_code': None,
    'workdir': None,
    'proj_dir':None,
    'pattern': None,
    'update': None,
    'remove': None
}

general  = "/badc/cmip6/data/CMIP6/"
groupdir = ''
workdir  = ''

# List 100 random CMIP datasets

def get_CMIP_data_recursive(path):
    contents = []
    for c in os.listdir(path):
        if os.path.isdir(os.path.join(path,c)):
            contents.append(c)
    if len(contents) > 0:
        randsel = contents[random.randint(0,len(contents)-1)]
        return get_CMIP_data_recursive(os.path.join(path, randsel))
    else:
        return path

def get_proj_code(path, prefix=''):
    return path.replace(prefix,'').replace('/','_')
    
def get_fpaths():
    file = f'{groupdir}/CMIP6_rand100_00/proj_codes.txt'
    with open(file) as f:
        contents = [r.strip() for r in f.readlines()]
    return contents

def test_cmip6():
    fpaths = get_fpaths()
    word = ''
    for x in range(400):
        print(x)
        fpath = get_CMIP_data_recursive(general)
        while fpath in fpaths:
            fpath = get_CMIP_data_recursive(general)
        proj_code = get_proj_code(fpath)
        workdir = '/gws/nopw/j04/esacci_portal/kerchunk/pipeline/in_progress'
        proj_dir = f'{workdir}/{proj_code}'
        pattern = f'{os.path.realpath(fpath)}/*.nc'
        word += f'{proj_code},{workdir},{proj_dir},{pattern},,\n'

    if not os.path.isdir(f'{groupdir}/CMIP6_rand400_00'):
        os.makedirs(f'{groupdir}/CMIP6_rand400_00')

    with open(f'{groupdir}/CMIP6_rand400_00/datasets.csv','w') as f:
        f.write(word)
    print('Wrote 100 datasets to config group CMIP6_rand100_00')

if __name__ == '__main__':
    # Get a list of paths from some input file
    # For each path, get project_code, workdir, proj_dir, pattern.

    group = sys.argv[1]
    prefix = sys.argv[2]

    groupdir = os.environ['GROUPDIR']
    workdir = os.environ['WORKDIR']

    with open(f'{groupdir}/filelists/{group}.txt') as f:
        datasets = [r.strip() for r in f.readlines()]
    records = ''
    for ds in datasets:
        proj_code = get_proj_code(ds, prefix=prefix)
        proj_dir = f'{workdir}/{group}/{proj_code}'
        pattern = f'{os.path.realpath(ds)}/*.nc'
        records += f'{proj_code},{workdir},{proj_dir},{pattern},,\n'
    
    if not os.path.isdir(f'{groupdir}/{group}'):
        os.makedirs(f'{groupdir}/{group}')

    with open(f'{groupdir}/{group}/datasets.csv','w') as f:
        f.write(records)
    print(f"Wrote {len(datasets)} datasets to config group {group}")
