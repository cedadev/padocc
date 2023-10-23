#!/bin/bash
#SBATCH --partition=short-serial-4hr
#SBATCH --account=short4hr
#SBATCH --job-name=parq_%A_%a

#SBATCH --time=30:00
#SBATCH --time-min=10:00
#SBATCH --mem=2G

#SBATCH -o /home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/temp/ocean-daily-all/outs/%A_%a.out
#SBATCH -e /home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/temp/ocean-daily-all/errs/%A_%a.err


module add jaspy
source /home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/build_venv/bin/activate
python /home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/processing/parallel/batch_process.py $SLURM_ARRAY_TASK_ID 1600
