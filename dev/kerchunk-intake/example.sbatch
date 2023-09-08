#!/bin/bash
#SBATCH --partition=short-serial-4hr        // This is the queue we want to use
#SBATCH --account=short4hr                  // The account we're using to submit jobs
#SBATCH --job-name=ceda_cmip6_%A_%a         // The job name (%A and %a are shorthand for the job Id and Array Id)

#SBATCH --time=30:00                        // Specify a maximum time in minutes for the job
#SBATCH --time-min=2:00                     // Specify a minimum expected time in minutes for the job
#SBATCH --mem=2G                            // Specify expected memory RAM requirements

#SBATCH -o outs/%A_%a.out                   // Output file name (print outputs)
#SBATCH -e errs/%A_%a.err                   // Error file name (shows errors)


module add jaspy                            // Set up environment and run the python script with some command-line arguments
source /home/users/dwest77/venvs/kvenv/bin/activate
python /home/users/dwest77/Documents/intake_dev/kerchunk-intake/intake_tool.py /home/users/dwest77/Documents/intake_dev/kerchunk-intake/jarrfiles/job-$SLURM_ARRAY_TASK_ID.txt -f
