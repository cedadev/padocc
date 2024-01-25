Running the Pipeline
====================

============================
Running for a single dataset
============================

Run all single-dataset processes with the ```single-run.py``` script.

```usage: single_run.py [-h] [-w WORKDIR] [-g GROUPDIR] [-G GROUPID] [-p PROJ_DIR] [-n NEW_VERSION] [-m MODE] [-t TIME_ALLOWED] [-b] [-s SUBSET] [-r REPEAT_ID] [-f] [-v] [-d] [-Q] phase proj_code```

positional arguments:
  phase                 Phase of the pipeline to initiate
  proj_code             Project identifier code

options:
  -h, --help            show this help message and exit
  -w WORKDIR, --workdir WORKDIR
                        Working directory for pipeline
  -g GROUPDIR, --groupdir GROUPDIR
                        Group directory for pipeline
  -G GROUPID, --groupID GROUPID
                        Group identifier label
  -p PROJ_DIR, --proj_dir PROJ_DIR
                        Project directory for pipeline
  -n NEW_VERSION, --new_version NEW_VERSION
                        If present, create a new version
  -m MODE, --mode MODE  Print or record information (log or std)
  -t TIME_ALLOWED, --time-allowed TIME_ALLOWED
                        Time limit for this job
  -b, --bypass-errs     Bypass all error messages - skip failed jobs
  -s SUBSET, --subset SUBSET
                        Size of subset within group
  -r REPEAT_ID, --repeat_id REPEAT_ID
                        Repeat id (1 if first time running, <phase>_<repeat> otherwise)
  -f                    Force overwrite of steps if previously done
  -v, --verbose         Print helpful statements while running
  -d, --dryrun          Perform dry-run (i.e no new files/dirs created)
  -Q, --quality         Quality assured checks - thorough run

===============================
Running for a group of datasets
===============================

Run all multi-dataset group processes within the pipeline using the ```group_run.py``` script.

```usage: group_run.py [-h] [-s SOURCE] [-e VENVPATH] [-w WORKDIR] [-g GROUPDIR] [-p PROJ_DIR] [-n NEW_VERSION] [-m MODE] [-t TIME_ALLOWED] [-b] [-i INPUT] [-S SUBSET] [-r REPEAT_ID] [-f] [-v] [-d] [-Q] phase groupID```

positional arguments:
  phase                 Phase of the pipeline to initiate
  groupID               Group identifier code

options:
  -h, --help            show this help message and exit
  -s SOURCE             Path to directory containing master scripts (this one)
  -e VENVPATH           Path to virtual (e)nvironment (excludes /bin/activate)
  -w WORKDIR, --workdir WORKDIR
                        Working directory for pipeline
  -g GROUPDIR, --groupdir GROUPDIR
                        Group directory for pipeline
  -p PROJ_DIR, --proj_dir PROJ_DIR
                        Project directory for pipeline
  -n NEW_VERSION, --new_version NEW_VERSION
                        If present, create a new version
  -m MODE, --mode MODE  Print or record information (log or std)
  -t TIME_ALLOWED, --time-allowed TIME_ALLOWED
                        Time limit for this job
  -b, --bypass-errs     Bypass all error messages - skip failed jobs
  -i INPUT, --input INPUT
                        input file (for init phase)
  -S SUBSET, --subset SUBSET
                        Size of subset within group
  -r REPEAT_ID, --repeat_id REPEAT_ID
                        Repeat id (1 if first time running, <phase>_<repeat> otherwise)
  -f                    Force overwrite of steps if previously done
  -v, --verbose         Print helpful statements while running
  -d, --dryrun          Perform dry-run (i.e no new files/dirs created)
  -Q, --quality         Quality assured checks - thorough run