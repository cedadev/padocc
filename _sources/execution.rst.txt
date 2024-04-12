Pipeline Flags
==============

====================
BypassSwitch Options
====================

Certain non-fatal errors may be bypassed using the Bypass flag:
::

  Format: -b "DBSCR"

  Default: "DBSCR" # Highlighted by a '*'

  "D" - * Skip driver failures - Pipeline tries different options for NetCDF (default).
      -   Only need to turn this skip off if all drivers fail (KerchunkFatalDriverError).
  "B" - * Skip Box compute errors.
  "S" - * Skip Soft fails (NaN-only boxes in validation) (default).
  "C" - * Skip calculation (data sum) errors (time array typically cannot be summed) (default).
  "X" -   Skip initial shape errors, by attempting XKShape tolerance method (special case.)
  "R" - * Skip reporting to status_log which becomes visible with assessor. Reporting is skipped
          by default in single_run.py but overridden when using group_run.py so any serial
          testing does not by default report the error experienced to the status log for that project.
  "F" -   Skip scanning (fasttrack) and go straight to compute. Required if running compute before scan
          is attempted.

========================
Single Dataset Operation
========================

Run all single-dataset processes with the ``single-run.py`` script.

.. code-block:: python
    
  usage: single_run.py [-h] [-f] [-v] [-d] [-Q] [-B] [-A] [-w WORKDIR] [-g GROUPDIR] [-p PROJ_DIR] 
                       [-t TIME_ALLOWED] [-G GROUPID] [-M MEMORY] [-s SUBSET]
                       [-r REPEAT_ID] [-b BYPASS] [-n NEW_VERSION] [-m MODE] [-O OVERRIDE_TYPE]
                       phase proj_code

  Run a pipeline step for a single dataset

  positional arguments:
    phase                 Phase of the pipeline to initiate
    proj_code             Project identifier code

  options:
    -h, --help            show this help message and exit
    -f, --forceful        Force overwrite of steps if previously done
    -v, --verbose         Print helpful statements while running
    -d, --dryrun          Perform dry-run (i.e no new files/dirs created)
    -Q, --quality         Create refs from scratch (no loading), use all NetCDF files in validation
    -B, --backtrack       Backtrack to previous position, remove files that would be created in this job.
    -A, --alloc-bins      Use binpacking for allocations (otherwise will use banding)

    -w WORKDIR, --workdir WORKDIR
                          Working directory for pipeline
    -g GROUPDIR, --groupdir GROUPDIR
                          Group directory for pipeline
    -p PROJ_DIR, --proj_dir PROJ_DIR
                          Project directory for pipeline
    -t TIME_ALLOWED, --time-allowed TIME_ALLOWED
                          Time limit for this job
    -G GROUPID, --groupID GROUPID
                          Group identifier label
    -M MEMORY, --memory MEMORY
                          Memory allocation for this job (i.e "2G" for 2GB)
    -s SUBSET, --subset SUBSET
                          Size of subset within group
    -r REPEAT_ID, --repeat_id REPEAT_ID
                          Repeat id (1 if first time running, <phase>_<repeat> otherwise)
    -b BYPASS, --bypass-errs BYPASS
                          Bypass switch options: See Above

    -n NEW_VERSION, --new_version NEW_VERSION
                          If present, create a new version
    -m MODE, --mode MODE  Print or record information (log or std)
    -O OVERRIDE_TYPE, --override_type OVERRIDE_TYPE
                          Specify cloud-format output type, overrides any determination by pipeline.

=============================
Multi-Dataset Group Operation
=============================

Run all multi-dataset group processes within the pipeline using the ``group_run.py`` script.

.. code-block:: python
  
  usage: group_run.py [-h] [-S SOURCE] [-e VENVPATH] [-i INPUT] [-A] [--allow-band-increase] [-f] [-v] [-d] [-Q] [-b BYPASS] [-B] [-w WORKDIR] [-g GROUPDIR]
                      [-p PROJ_DIR] [-G GROUPID] [-t TIME_ALLOWED] [-M MEMORY] [-s SUBSET] [-r REPEAT_ID] [-n NEW_VERSION] [-m MODE]
                      phase groupID

  Run a pipeline step for a group of datasets

  positional arguments:
    phase                 Phase of the pipeline to initiate
    groupID               Group identifier code

  options:
    -h, --help            show this help message and exit
    -S SOURCE, --source SOURCE
                          Path to directory containing master scripts (this one)
    -e VENVPATH, --environ VENVPATH
                          Path to virtual (e)nvironment (excludes /bin/activate)
    -i INPUT, --input INPUT
                          input file (for init phase)
    -A, --alloc-bins      input file (for init phase)

    --allow-band-increase
                          Allow automatic banding increase relative to previous runs.

    -f, --forceful        Force overwrite of steps if previously done
    -v, --verbose         Print helpful statements while running
    -d, --dryrun          Perform dry-run (i.e no new files/dirs created)
    -Q, --quality         Quality assured checks - thorough run

    -b BYPASS, --bypass-errs BYPASS
                          Bypass switch options: See Above

    -B, --backtrack       Backtrack to previous position, remove files that would be created in this job.
    -w WORKDIR, --workdir WORKDIR
                          Working directory for pipeline
    -g GROUPDIR, --groupdir GROUPDIR
                          Group directory for pipeline
    -p PROJ_DIR, --proj_dir PROJ_DIR
                          Project directory for pipeline
    -G GROUPID, --groupID GROUPID
                          Group identifier label
    -t TIME_ALLOWED, --time-allowed TIME_ALLOWED
                          Time limit for this job
    -M MEMORY, --memory MEMORY
                          Memory allocation for this job (i.e "2G" for 2GB)
    -s SUBSET, --subset SUBSET
                          Size of subset within group
    -r REPEAT_ID, --repeat_id REPEAT_ID
                          Repeat id (main if first time running, <phase>_<repeat> otherwise)
    -n NEW_VERSION, --new_version NEW_VERSION
                          If present, create a new version
    -m MODE, --mode MODE  Print or record information (log or std)

=======================
Assessor Tool Operation
=======================

Perform assessments of groups within the pipeline using the ``assess.py`` script.

.. code-block:: python

  usage: assess.py [-h] [-B] [-R REASON] [-s OPTION] [-c CLEANUP] [-U UPGRADE] [-l] [-j JOBID] [-p PHASE] [-r REPEAT_ID] [-n NEW_ID] [-N NUMBERS] [-e ERROR] [-E] [-W]
                   [-O] [-w WORKDIR] [-g GROUPDIR] [-v] [-m MODE]
                   operation groupID

  Run a pipeline step for a single dataset

  positional arguments:
    operation             Operation to perform - choose from ['progress', 'blacklist', 'upgrade', 'summarise', 'display', 'cleanup', 'match',
                          'status_log']
    groupID               Group identifier code for the group on which to operate.

  options:
    -h, --help            show this help message and exit
    -B, --blacklist       Use when saving project codes to the blacklist

    -R REASON, --reason REASON
                          Provide the reason for handling project codes when saving to the blacklist or upgrading
    -s OPTION, --show-opts OPTION
                          Show options for jobids, labels, also used in matching and status_log.
    -c CLEANUP, --clean-up CLEANUP
                          Clean up group directory of errors/outputs/labels
    -U UPGRADE, --upgrade UPGRADE
                          Upgrade to new version
    -l, --long            Show long error message (no concatenation)
    -j JOBID, --jobid JOBID
                          Identifier of job to inspect
    -p PHASE, --phase PHASE
                          Pipeline phase to inspect
    -r REPEAT_ID, --repeat_id REPEAT_ID
                          Inspect an existing ID for errors
    -n NEW_ID, --new_id NEW_ID
                          Create a new repeat ID, specify selection of codes by phase, error etc.
    -N NUMBERS, --numbers NUMBERS
                          Show project code IDs for lists of codes less than the N value specified here.
    -e ERROR, --error ERROR
                          Inspect error of a specific type
    -E, --examine         Examine log outputs individually.
    -W, --write           Write outputs to files
    -O, --overwrite       Force overwrite of steps if previously done
    -w WORKDIR, --workdir WORKDIR
                          Working directory for pipeline
    -g GROUPDIR, --groupdir GROUPDIR
                          Group directory for pipeline
    -v, --verbose         Print helpful statements while running
    -m MODE, --mode MODE  Print or record information (log or std)