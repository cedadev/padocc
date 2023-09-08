#python
import os
import sys
from getopt import getopt
import numpy as np

PATH = '/home/users/dwest77/Documents/kerchunk_dev/parquet/dev/batch/esacci9'

dirs = [
    f'{PATH}/outs',
    f'{PATH}/errs',
    f'{PATH}/jbs_sbatch',
    f'{PATH}/filelists'
]

def mkfiles(p):
    if not os.path.isdir(p):
	    os.makedirs(p)

for d in dirs:
    mkfiles(d)

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

def format_sbatch(jobname, time, outs, errs, dependency, venvpath, script, cmdargs):
    return SBATCH.format(jobname, time, outs, errs, dependency, venvpath, script, cmdargs)

with open(f'{PATH}/../../filelists/test9.txt') as f:
    files = [r.split('\n')[0] for r in f.readlines()]

fcount = len(files)/4

VENVPATH = '/home/users/dwest77/venvs/kvenv/bin/activate'
script = f'{PATH}/../../batch_parq.py'
cmdargs = '$SLURM_ARRAY_TASK_ID $SLURM_ARRAY_TASK_COUNT'

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