# Batch Process Kerchunk Pipeline phases for multiple datasets

 - Run each phase of the pipeline for x datasets using SLURM

Phases to perform en masse.:
 - Init
 - Scan
 - Process
 - Test

Init:
 - Take a csv format file documenting many datasets
 - Create config files and set up each dataset.
 - Not a parallel script - only needs serial run

Scan:
 - Distribute scanning of dataset sections

Process:
 - Distribute processing

Test:
 - Distribute testing

Requirements:
 - Wide configuration script, wide_config.py <csv-file>
 - pipeline running script, group_run.py <phase> <group>