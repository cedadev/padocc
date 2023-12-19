# kerchunk-builder
A repository for building a kerchunk infrastructure using existing tools, and a set of showcase notebooks to use on example data in this repository.

Now a repository under cedadev group!

Example Notebooks:
https://mybinder.org/v2/gh/cedadev/kerchunk-builder.git/main?filepath=showcase/notebooks

# Pipeline Phases

All pipeline phases are now run using master scripts `single_run.py` or `group_run.py` 

## 0 Activating Environment Settings

`source build_venv/bin/activate`

Python virtual environment setup

`. templates/<config>.sh`

Sets all environment variables, if a shell script is already present with the correct name. Environment variables to set are:
 - WORKDIR: (Required) - Central workspace for saving data
 - GROUPDIR: (Required for parallel) - Workspace for a specific group
 - SRCDIR: (Required for parallel) - Kerchunk pipeline repo path ending in `/kerchunk-builder`
 - KVENV: (Required for parallel) - Path to virtual environment.
All of the above can be passed as flags to each script, or set as environment variables before processing.

## 1 Running the Pipeline - Examples

### 1.1 Single running of an isolated dataset
`python single_run.py scan a11x34 -vfbd`

The above runs the scan process for project code `a11x34` with verbose level 1, forced running (overwrites existing files), bypass errors with `-b` and dry-running with `-d`. Note that running with `-f` and `-d` means that sections will not be skipped if files already exist, but no new files will be generated.

### 1.2 Single running for a dataset within a group
`python single_run.py scan 0 -vfbd -G CMIP6_exampleset_1 -r scan_2`

The above has the same features as before, except now we are using project id `0` in place of a project code, with a group ID (`-G`) supplied as well as a repeat ID (`-r`) from which to identify the correct project code from a group. This is an example of what each parallel job will execute, so using this format is solely for test purposes.

### 1.3 Group running of multiple datasets
`python group_run.py scan CMIP6_exampleset_1 -vfbd -r scan_2`

The above is the full parallelised job execution command which would activate all jobs with the `single_run.py` script as detailed in section 1.2. This command creates a sbatch file and separate command to start the parallel jobs, which will include all datasets within the `scan_2` subgroup of the `CMIP6_exampleset_1` group. Subgroups are created with the `identify_reruns.py` script.

### 1.4 Full Worked Example
Using the example documents in this repository, we can run an example group containing just two datasets. Any number of datasets would also follow this method, two is not a unique number other than being the smallest so to minimise duplication.

#### 1.4.1 Init
The first step is to initialise the group from the example csv given. Here I am giving the group the identifier `UKCP_test1` as the second argument after the phase `init` which we are peforming. With `-i` we supply an input csv (This can also be a text file for some cases where the project code can be generated). Finally `-v` means we get to see general information as the program is running.
`python group_run.py init UKCP_test1 -i examples/UKCP_test1.csv -v`

#### 1.4.2 Scan
Scanning will give an indication of how long each file will take to produce and some other characteristics which will come into play in later phases.
`python group_run.py scan UKCP_test1 -v`

If running in `dryrun` mode, this will generate an sbatch submission command like:
`sbatch --array=0-2 /gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline/groups/UKCP_test1/sbatch/scan.sbatch`

Which can be copied into the terminal and executed. Otherwise the jobs will be automatically submitted.

#### 1.4.3 Compute
`python group_run.py compute UKCP_test1 -vv`

This command differs only with the level of verboseness, with this many 'v's we will see the debug information as well as general information. Again this will produce an sbatch command to be copied to the terminal if in dryrun mode.

#### 1.4.4 Validate
`python group_run.py validate UKCP_test1 -vv`

This final step will submit all datasets for validation, which includes copying the final output file to the `/complete` directory within the workdir set as an environment variable.


## 2 Pipeline Phases in detail

### 2.1 Init
Initialise and configure for running the pipeline for any number of datasets in parallel.
If using the pipeline with a group of datasets, an input file is required (`-i` option) which must be one of:
 - A text file containing the wildcard paths describing all files within each dataset (CMIP6)
 - A properly formatted CSV with fields for each entry corresponding to the headers:
   - Project code: Unique identifier for this dataset, commonly taken from naming conventions in the path
   - Pattern/Filename
   - Updates: Boolean 1/0 if updates file is present
   - Removals: Boolean 1/0 if removals file is present

### 2.2 Scan
Run kerchunk-scan tool (or similar) to build a test kerchunk file and determine parameters:
 - chunks per netcdf file (Nc)
 - average chunk size (Tc)
 - total expected kerchunk size (Tk)

### 2.3 Configure
Determine manual/automatic configuration adjustments to computing for later phases.
This section may need manual interaction in8itially, but with enough runs we can build profiles that fit different dataset types.
Config Information includes:
 - Metadata adjustments/corrections
 - Time dimension adjustments
 - Record size calculation

### 2.4 Compute
Create parquet store for a specified dataset, using method depending on total expected kerchunk size (Tk)

#### 2.4.1 Large Chunkset Tk value - Parallel (Batch) processing
 - Batch process to create parts        - batch_process/process_wrapper.py
 - Combine parts using copier script    - combine_refs.py
 - Correct metadata (shape, parameters) - correct_meta.py
 - Run time correction script if necessary - correct_time.py

#### 2.4.2 Small Chunkset Tk value - Serial processing
Run create parquet script - create_parq.py

#### 2.4.3 Additions
Edit parquet store in general where necessary:
 - Edit file paths to add dap http links - add_dap.py 

## Post-Processing Phase

### 4. Test
Run a series of tests on parquet store usage:
 - Ensure small plot success with no errors
 - Ensure large plot (dask gateway) success with no errors or killed job.

### 5. Catalog
Update catalog system with addition of new parquet store:
 - pystac client
 - intake catalog
