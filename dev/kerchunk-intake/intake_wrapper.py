import os, sys

import pandas as pd

base_path = '/badc/cmip6/data/CMIP6/ScenarioMIP/NCC/'

SBATCH = """#!/bin/bash
#SBATCH --partition=short-serial-4hr
#SBATCH --account=short4hr
#SBATCH --job-name={}

#SBATCH --time={}
#SBATCH --time-min=2:00
#SBATCH --mem=2G

#SBATCH -o outs/{}
#SBATCH -e errs/{}
{}

module add jaspy
source {}
python {} {}
"""

VENVPATH        = '/home/users/dwest77/venvs/kvenv/bin/activate'
KERCHUNK_INTAKE = '/home/users/dwest77/Documents/intake_dev/kerchunk-intake'

# format(jobname, time, outs, errs, dependency, venvpath, script, cmdargs)

workingdir = ''

def vprint(message, verbose=True):
    if verbose:
        print(message)

def identify_paths(path,d):
    fpaths = []
    subpaths = os.listdir(path)
    counter = 0
    isnc = False
    if 'latest' in subpaths:
        version = os.readlink(path +'/latest')
        fpaths.append(path + '/' + version)
    else:
        while counter < len(subpaths) and not isnc:
            subpath = subpaths[counter]
            if os.path.isdir(path + '/' + subpath):
                fpaths += identify_paths(path + '/' + subpath, d+1)
            counter += 1
    return fpaths

def format_path(path):
    path = path.replace('//','/')
    if not path.endswith('/'):
        path += '/'
    return path

def format_sbatch(jobname, time, outs, errs, dependency, venvpath, script, cmdargs):
    return SBATCH.format(jobname, time, outs, errs, dependency, venvpath, script, cmdargs)

def assemble_batch_array(paths, batch=False, assemble=False, s3=False):
    """
    Write each path sequentially to a jobfile marked job-$SLURM_ARRAY_TASK_ID.txt
    Reassemble the batch script and run it.
    """
    if batch:
        for x, path in enumerate(paths):
            f = open(f'{KERCHUNK_INTAKE}/jarrfiles/job-{x}.txt','w')
            f.write(format_path(path))
            f.close()

        script  = f'{KERCHUNK_INTAKE}/intake_tool.py'
        cmdargs = f'{KERCHUNK_INTAKE}/jarrfiles/job-$SLURM_ARRAY_TASK_ID.txt -f'

        arrayjob = format_sbatch(
            'ceda_cmip6_%A_%a',
            '30:00',
            '%A_%a.out',
            '%A_%a.err',
            '',
            VENVPATH,
            script,
            cmdargs
        )

        if not os.path.exists(f'{KERCHUNK_INTAKE}/sbatch/ceda_cmip6.sbatch'):
            os.system(f'touch {KERCHUNK_INTAKE}/sbatch/ceda_cmip6.sbatch')
        f = open(f'{KERCHUNK_INTAKE}/sbatch/ceda_cmip6.sbatch','w')
        f.write(arrayjob)
        f.close()
        os.system(f'sbatch --array=0-{str(len(paths)-1)} {KERCHUNK_INTAKE}/sbatch/ceda_cmip6.sbatch > {KERCHUNK_INTAKE}/sbatch/ceda_cmip6_current.txt')
        print('Submitted batch array')

        # Write assembler job with dependency
        f = open(f'{KERCHUNK_INTAKE}/sbatch/ceda_cmip6_current.txt','r')
        jobid = f.readlines()[0].replace('\n','').split(' ')[-1] # Last item
        f.close()

    if assemble:

        # Write assembler job with dependency
        f = open(f'{KERCHUNK_INTAKE}/sbatch/ceda_cmip6_current.txt','r')
        try:
            jobid = f.readlines()[0].replace('\n','').split(' ')[-1] # Last item
        except:
            print('[Error] No previous job record found')
            f.close()
            return None
        f.close()

        ascript  = f'{KERCHUNK_INTAKE}/intake_assemble.py'
        acmdargs = f'{jobid}'

        assemblejob = format_sbatch(
            'ceda_cmip6_%j',
            '10:00',
            '%j.out',
            '%j.err',
            f'#SBATCH -d afterok:{jobid}',
            VENVPATH,
            ascript,
            acmdargs,
        )

        if not os.path.exists(f'{KERCHUNK_INTAKE}/sbatch/ceda_cmip6_assemble.sbatch'):
            os.system(f'touch {KERCHUNK_INTAKE}/sbatch/ceda_cmip6_assemble.sbatch')
        f = open(f'{KERCHUNK_INTAKE}/sbatch/ceda_cmip6_assemble.sbatch','w')
        f.write(assemblejob)
        f.close()
        os.system(f'sbatch {KERCHUNK_INTAKE}/sbatch/ceda_cmip6_assemble.sbatch')
    
clobber = True

#f = open('test_paths.txt','r')
#f.write('\n'.join(batch_paths))
#f.close()
#x=input('>>')
#content = f.readlines()
#batch_paths = [c.replace('\n','') for c in content]
#f.close()

def strip_attrs(proj_path):
    cmip6_path = '/badc/cmip6/data/'
    if not proj_path.startswith(cmip6_path):
        vprint(f'[INFO] Unsupported path format: {proj_path}')
        return 1

    vprint('[INFO] Extracting Attributes', verbose=False)
    path_attrsr = proj_path.replace(cmip6_path,'').split('/') # Get all but last as it should be blank
    if path_attrsr[-1] == '':
        path_attrsr = path_attrsr[:-1]
        
    path_attrs = []
    for p in path_attrsr:
        if p != '':
            path_attrs.append(p)

    return path_attrs

def check_existing_records(dataframe, entryarr, colset):
    queries = []
    for column, index in colset:
        try:
            value = entryarr[index]
            queries.append(f"{column} == '{value}'")
        except IndexError:
            pass

    query = ' and '.join(queries)
    result = dataframe.query(query)
    return result.empty

def get_columns():
    g = open(f'{KERCHUNK_INTAKE}/templates/columns.txt','r')
    columns = [l.replace('\n','') for l in g.readlines()]
    columns = columns[:12]
    g.close()
    return columns

def check_each_path(paths, clobber=False):
    # Find the attribute in the maincsv file
    # If all + version matches, do not run unless clobber is true
    # If stuff does not match, run the file
    columns = get_columns()
    colset = [(c, x) for x, c in enumerate(columns[:9])]

    intake_store = '/gws/nopw/j04/cedaproc/kerchunk-store'

    checked_paths = []
    for p in paths:
        if not clobber:
            attrs = strip_attrs(p)

            cat_id = '_'.join(attrs[1:3])
            local_csv = f'{intake_store}/{cat_id}/{cat_id}_catalog.csv'
            try:
                dataframe = pd.read_csv(local_csv)
                if check_existing_records(dataframe, attrs, colset):
                    checked_paths.append(p)
            except FileNotFoundError:
                vprint('[INFO] CSV not found, assuming none exists')
                checked_paths.append(p)
        else:
            checked_paths.append(p)
    return checked_paths

def main(args):

    #paths       = identify_paths(base_path,0)
    #batch_paths = check_each_path(paths[:11])[:10]

    with open('/home/users/dwest77/Documents/kerchunk_dev/kerchunk-tools/kerchunk_tools/filelists/cci_delivery/paths') as f:
        batch_paths = [line.split('\n')[0] for line in f.readlines()]

    overwrite = '-o' in args
    batch = '-b' in args
    assemble = '-a' in args
    s3 = '-s' in args
    proceed = '-y' in args

    if not proceed and len(batch_paths) > 0:
        proceed = input('proceed? (y/n): ') == 'y'

    if proceed:

        if overwrite:

            os.system('rm outs/*')
            os.system('rm errs/*')
            os.system('rm jarrfiles/*')

            for sbatch_dir in ['jarrfiles','errs','outs','sbatch']:
                if not os.path.isdir(os.path.join(workingdir, sbatch_dir)):
                    os.makedirs(os.path.join(workingdir, sbatch_dir))

        assemble_batch_array(batch_paths, batch=batch, assemble=assemble, s3=s3)
    elif len(batch_paths) == 0:
        print('All records up to date - No new paths or versions, no processes initialised')

main(sys.argv[1:])
