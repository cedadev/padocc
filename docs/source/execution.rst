Pipeline Flags
==============

====================
BypassSwitch Options
====================

Certain non-fatal errors may be bypassed using the Bypass flag:
::

  Format: -b "FBDSCM"

  Default: "FDSC" # Highlighted by a '*'

  "F" - * Skip individual file scanning errors.
  "D" - * Skip driver failures - Pipeline tries different options for NetCDF (default)
      -   Only need to turn this skip off if all drivers fail (KerchunkFatalDriverError)
  "B" -   Skip Box compute errors.
  "S" - * Skip Soft fails (NaN-only boxes in validation) (default)
  "C" - * Skip calculation (data sum) errors (time array typically cannot be summed) (default)
  "M" -   Skip memory checks (validate/compute aborts if utilisation estimate exceeds cap)

========================
Single Dataset Operation
========================

Run all single-dataset processes with the ```single-run.py``` script.

.. code-block:: python
    
  usage: single_run.py [-h] [-f] [-v] [-d] [-Q] [-b BYPASS] [-w WORKDIR] [-g GROUPDIR] 
                       [-p PROJ_DIR] [-G GROUPID] [-t TIME_ALLOWED] [-M MEMORY] [-s SUBSET] 
                       [-r REPEAT_ID] [-n NEW_VERSION] [-m MODE] 
                       phase proj_code

  Run a pipeline step for a single dataset

  positional arguments:
    phase                 Phase of the pipeline to initiate
    proj_code             Project identifier code

  options:
    -h, --help            show this help message and exit

    # Action-based - standard flags
    -f, --forceful        Force overwrite of steps if previously done
    -v, --verbose         Print helpful statements while running
    -d, --dryrun          Perform dry-run (i.e no new files/dirs created)
    -Q, --quality         Quality assured checks - thorough run
    -b BYPASS, --bypass-errs BYPASS
                          Bypass switch options: See Above

    # Environment Variables
    -w WORKDIR, --workdir WORKDIR
                          Working directory for pipeline
    -g GROUPDIR, --groupdir GROUPDIR
                          Group directory for pipeline
    -p PROJ_DIR, --proj_dir PROJ_DIR
                          Project directory for pipeline

    # Single-job within group
    -G GROUPID, --groupID GROUPID
                          Group identifier label
    -t TIME_ALLOWED, --time-allowed TIME_ALLOWED
                          Time limit for this job
    -M MEMORY, --memory MEMORY
                          Memory allocation for this job (i.e "2G" for 2GB)
    -s SUBSET, --subset SUBSET
                          Size of subset within group
    -r REPEAT_ID, --repeat_id REPEAT_ID
                          Repeat id (1 if first time running, <phase>_<repeat> otherwise)

    # Specialised
    -n NEW_VERSION, --new_version NEW_VERSION
                          If present, create a new version
    -m MODE, --mode MODE  Print or record information (log or std)

=============================
Multi-Dataset Group Operation
=============================

Run all multi-dataset group processes within the pipeline using the ```group_run.py``` script.

.. code-block:: python
  
  usage: group_run.py [-h] [-S SOURCE] [-e VENVPATH] [-i INPUT] [-f] [-v] [-d] [-Q] [-b BYPASS] [-w WORKDIR] 
                      [-g GROUPDIR] [-p PROJ_DIR] [-G GROUPID] [-t TIME_ALLOWED] [-M MEMORY] [-s SUBSET] 
                      [-r REPEAT_ID] [-n NEW_VERSION] [-m MODE] 
                      phase groupID

  Run a pipeline step for a group of datasets

  positional arguments:
    phase                 Phase of the pipeline to initiate
    groupID               Group identifier code

  options:
    -h, --help            show this help message and exit

    # Group-run specific
    -S SOURCE, --source SOURCE
                          Path to directory containing master scripts (this one)
    -e VENVPATH, --environ VENVPATH
                          Path to virtual (e)nvironment (excludes /bin/activate)
    -i INPUT, --input INPUT
                          input file (for init phase)

    # Action-based - standard flags
    -f, --forceful        Force overwrite of steps if previously done
    -v, --verbose         Print helpful statements while running
    -d, --dryrun          Perform dry-run (i.e no new files/dirs created)
    -Q, --quality         Quality assured checks - thorough run
    -b BYPASS, --bypass-errs BYPASS
                          Bypass switch options: See Above

    # Environment Variables
    -w WORKDIR, --workdir WORKDIR
                          Working directory for pipeline
    -g GROUPDIR, --groupdir GROUPDIR
                          Group directory for pipeline
    -p PROJ_DIR, --proj_dir PROJ_DIR
                          Project directory for pipeline

    # Single-job within group
    -G GROUPID, --groupID GROUPID
                          Group identifier label
    -t TIME_ALLOWED, --time-allowed TIME_ALLOWED
                          Time limit for this job
    -M MEMORY, --memory MEMORY
                          Memory allocation for this job (i.e "2G" for 2GB)
    -s SUBSET, --subset SUBSET
                          Size of subset within group
    -r REPEAT_ID, --repeat_id REPEAT_ID
                          Repeat id (1 if first time running, <phase>_<repeat> otherwise)

    # Specialised
    -n NEW_VERSION, --new_version NEW_VERSION
                          If present, create a new version
    -m MODE, --mode MODE  Print or record information (log or std)