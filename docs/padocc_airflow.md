# Padocc in Airflow - Pseudo Workflow

The PADOCC pipeline consists of three main stages (with a fourth in development):
 - scan
 - compute
 - validate

These phases are designed as operations which are applied to all member datasets of a 'group'.
 - Each dataset may consist of any number of source files (Netcdf or otherwise) to be aggregated into a single 'atomic' dataset object.
 - Each group may contain any number of datasets, referred to as 'projects'.

All projects are initialised as part of the group. They can be added/removed later or merged with other groups, but they should remain part of a group at all times, even if the group consists of just one member.

Operations can be performed across all members or just a subset of the group, which becomes important in the later phases. The pseudo workflow consists of the following:

1. Run the initialisation for a group, using a config file or STAC record selection.
 
2. Run the scan operation for all projects, either in serial or using job submissions.

3a. Successful projects can then run through the compute phase, submissions are preferred here, and padocc has an allocation system that can be applied to any subset. The 'success' subset can be generated from padocc tools:
 - 3ai. Create the 'scan-success' subset.
 - 3aii. Deploy computation for 'scan-success'.

3b. All unsuccessful projects have a log file and error status to examine. Some of these may be resolvable with changes to the config file that can be made via padocc (rather than editing the files themselves.) This is a `manual (unknown)` step. Projects can then be rescanned once changes have been made. This can happen any number of times to fix any issues, but in some cases projects are simply not operable and should either be added to the 'faultlist' or removed from the group. Both of which are `manual (known)`

4a. Successfully computed projects will now have a cloud file generated. The validation phase should be run on this new group.
 - 4ai. Create the 'compute-success' subset.
 - 4aii. Deploy validation for this subset.

4b. Again, recomputations can happen any number of times with changes to the project, although often this step needs to be run due to time/memory issues with SLURM - rerunning should allow for higher values for both if that was the problem. The specific flags are `unknown` but the rerun process is `known`.

5a. Projects that have been successfully validated are moved to a 'complete' folder and can be safely ingested (this step is outside the pipeline).

5b. Projects that have issues or were skipped (in CFA-only case) can be manually checked and then 'ticked' to complete this step. This can be completed via padocc (`known`) but requires a coding interface/terminal.

5c. Projects that fail validation need to be checked for changes and potentially recomputed. This could mean any number of possible changes and is therefore `unknown`.