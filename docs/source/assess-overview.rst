Assessor Tool
=============

The assessor script ```assess.py``` is an all-purpose pipeline checking tool which can be used to assess:
 - The current status of all datasets within a given group in the pipeline (which phase each dataset currently sits in)
 - The errors/outputs associated with previous job runs.
 - Specific logs from datasets which are presenting a specific type of error.

An example command to run the assessor tool can be found below:
::
    python assess.py <operation> <group>

Where the operation can be one of the below options:
 - Progress: Get a general overview of the pipeline; how many datasets have completed or are stuck on each phase.
 - Summarise: Get an assessment of the data processed for this group
 - Display: Display a specific type of information about the pipeline (blacklisted codes, datasets with virtual dimensions or using parquet)

1. Overall Progress of the Pipeline
-----------------------------------

To see the general status of the pipeline for a given group:
::

    python assess.py progress <group>

An example output from this command can be seen below:
::

    Group: cci_group_v1
    Total Codes: 361

    scan      : 1     [0.3 %] (Variety: 1)
        - Complete                 : 1

    complete  : 185   [51.2%] (Variety: 1)
        - complete                 : 185

    unknown   : 21    [5.8 %] (Variety: 1)
        - no data                  : 21

    blacklist : 162   [44.9%] (Variety: 7)
        - NonKerchunkable          : 50
        - PartialDriver            : 3
        - PartialDriverFail        : 5
        - ExhaustedMemoryLimit     : 64
        - ExhaustedTimeLimit       : 18
        - ExhaustedTimeLimit*      : 1
        - ValidationMemoryLimit    : 21

In this case there are 185 datasets that have completed the pipeline with 1 left to be scanned. The 21 unknowns have no log file so there is no information on these. This will be resolved in later versions where a `seek` function will automatically run when checking the progress, to fix gaps in the logs for missing datasets.


An example use case is to write out all datasets that require scanning to a new label (repeat_label):
::

    python assess.py progress <group> -p scan -r <label_for_scan_subgroup> -W


The last flag ```-W``` is required when writing an output file from this program, otherwise the program will dryrun and produce no files.

2. Checking errors
------------------
Check what repeat labels are available already using:
::

    python assess.py display <group> -s labels

For listing the status of all datasets from a previous repeat idL
::

    python assess.py progress <group> -r <repeat_id>


For selecting a specific type of error (-e) and examine the full log for each example (-E)
::

    python assess.py progress <group> -r <old_id> -e "type_of_error" -p scan -E

Following from this, you may want to rerun the pipeline for just one type of error previously found:
::

    python assess.py progress <group> -r <old_repeat_id> -e "type_of_error" -p scan -n <new_repeat_id>

Note that if you are looking at a specific repeat ID, you can forego the phase (-p) flag, since it is expected this set would appear in the same phase anyway.

3. Special Display options
--------------------------

Check how many of the datasets in a group have virtual dimensions
::

    python assess.py display <group> -s virtuals