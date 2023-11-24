# kerchunk-builder
A repository for building a kerchunk infrastructure using existing tools, and a set of showcase notebooks to use on example data in this repository.

Now a repository under cedadev group!

Example Notebooks:
https://mybinder.org/v2/gh/cedadev/kerchunk-builder.git/main?filepath=showcase/notebooks

# Pipeline Phases

## Pre-processing Phase

### 1. Scan
Run kerchunk-scan tool (or similar) to build a test kerchunk file and determine parameters:
 - chunks per netcdf file (Nc)
 - average chunk size (Tc)
 - total expected kerchunk size (Tk)

### 2. Configure
Determine manual/automatic configuration adjustments to computing for later phases.
This section may need manual interaction in8itially, but with enough runs we can build profiles that fit different dataset types.
Config Information includes:
 - Metadata adjustments/corrections
 - Time dimension adjustments
 - Record size calculation

## Processing Phase

### 3. Compute
Create parquet store for a specified dataset, using method depending on total expected kerchunk size (Tk)

#### 3a. Large Chunkset Tk value - Parallel (Batch) processing
 - Batch process to create parts        - batch_process/process_wrapper.py
 - Combine parts using copier script    - combine_refs.py
 - Correct metadata (shape, parameters) - correct_meta.py
 - Run time correction script if necessary - correct_time.py

#### 3b. Small Chunkset Tk value - Serial processing
Run create parquet script - create_parq.py

#### 3c. Additions
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
