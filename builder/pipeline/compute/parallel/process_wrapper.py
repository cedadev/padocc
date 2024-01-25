#python
import os
import sys
from getopt import getopt
import numpy as np

BASE = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder'

PATH = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/temp/ocean-daily-all'

dirs = [
    f'{PATH}/outs',
    f'{PATH}/errs',
    f'{PATH}/jbs_sbatch',
    f'{PATH}/filelists'
]

def mkfiles(p):
    if not os.path.isdir(p):
        os.makedirs(p)
    else:
        os.system(f'rm -rf {p}/*')

for d in dirs:
    mkfiles(d)

SBATCH = """#!/bin/bash
#SBATCH --partition=short-serial-4hr
#SBATCH --account=short4hr
#SBATCH --job-name={}

#SBATCH --time={}
#SBATCH --time-min=10:00
#SBATCH --mem=2G

#SBATCH -o {}
#SBATCH -e {}
{}

module add jaspy
source {}
python {} {}
"""

def format_sbatch(jobname, time, outs, errs, dependency, venvpath, script, cmdargs):
    outs = f'{PATH}/outs/{outs}'
    errs = f'{PATH}/errs/{errs}'
    return SBATCH.format(
         jobname, 
         time, 
         outs, 
         errs, 
         dependency, 
         venvpath, 
         script, 
         cmdargs)

with open(f'{BASE}/test_parqs/filelists/gargant.txt') as f:
    files = [r.split('\n')[0] for r in f.readlines()]

fcount = 160 #len(files)/4

VENVPATH = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/build_venv/bin/activate'
script = f'{BASE}/processing/parallel/batch_process.py'
cmdargs = '$SLURM_ARRAY_TASK_ID 1600'

arrayjob = format_sbatch(
    'parq_%A_%a',
    '30:00',
    '%A_%a.out',
    '%A_%a.err',
    '',
    VENVPATH,
    script,
    cmdargs
)
with open(f'{PATH}/control.sbatch','w') as f:
    f.write(arrayjob)
print(fcount)
os.system(f'sbatch --array=0-{int(fcount-1)} {PATH}/control.sbatch')