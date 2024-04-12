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
 - Display: Display a specific type of information about the pipeline (blacklisted codes, datasets with virtual dimensions or using parquet)
 - Match: Match specific attributes within the ``detail-cfg.json`` file (and save to a new ID).
 - Summarise: Get an assessment of the data processed for this group
 - Upgrade: Update the version of a set of kerchunk files (includes internal metadata standard updates (timestamped, reason provided).
 - Cleanup: Remove cached files as part of group runs (errs, outs, repeat_ids etc.)

1. Progress of a group
----------------------

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

1.1. Checking errors
--------------------
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

    python assess.py progress <group> -r <old_repeat_id> -e "type_of_error" -p scan -n <new_repeat_id> -W

.. Note::

    If you are looking at a specific repeat ID, you can forego the phase (-p) flag, since it is expected this set would appear in the same phase anyway.
    The (-W) write flag is also required for any commands that would output data to a file. If the file already exists, you will need to specify an override
    level (-O or -OO) for merging or overwriting existing data (project code lists) respectively.

2. Display options
--------------------------

Check how many of the datasets in a group have virtual dimensions
::

    python assess.py display <group> -s virtuals

3. Match Special Attributes
---------------------------

Find the project codes where a specific attribute in ``detail-cfg.json`` matches some given value
::

    python assess.py match <group> -c "links_added:False"

4. Summarise data
-----------------

Summarise the Native/Kerchunk data generated (thus far) for an existing group.
::

    python assess.py summarise <group>

5. Upgrade Kerchunk version
---------------------------

Upgrade all kerchunk files (compute-validate stages) to a new version for a given reason. This is the 'formal' way of updating the version.
::

    python assess.py upgrade <group> -r <codes_to_upgrade> -R "Reason for upgrade" -W -U "krX.X" # New version id

6. Cleanup
----------

"Clean" or remove specific types of files:
 - Errors/Outputs in the correct places
 - "labels" i.e repeat_ids (including allocations and bands under that repeat_id)

In the below example we will remove every created ``repeat_id`` (equivalent terminology to 'label') except for ``main``.
::

    python assess.py cleanup <group> -c labels
