Assessor Tool
=============

The assessor script ```assess.py``` is an all-purpose pipeline checking tool which can be used to assess:
 - The current status of all datasets within a given group in the pipeline (which phase each dataset currently sits in)
 - The errors/outputs associated with previous job runs.

1. Overall Progress of the Pipeline
--------------------------------

To see the general status of the pipeline for a given group:
::
python assess.py <group> progress
::

An example use case is to write out all datasets that require scanning to a new label (repeat_label):
::
python assess.py <group> progress -p scan -r <label_for_scan_subgroup> -W
::

The last flag ```-W``` is required when writing an output file from this program, otherwise the program will dryrun and produce no files.

2. Checking errors
------------------
Check what repeat labels are available already using
::
python assess.py <group> errors -s labels
::

Show what jobs have previously run
::
python assess.py <group> errors -s jobids
::

For showing all errors from a previous job run
::
python assess.py <group> errors -j <jobid>
::

For selecting a specific type of error to investigate (-i) and examine the full log for each example (-E)
::
python assess.py test errors -j <jobid> -i "type_of_error" -E
::