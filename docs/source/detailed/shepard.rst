The SHEPARD Module
==================

The latest development in the PADOCC package is the SHEPARD Module.

SHEPARD (Serial Handler for Enabling PADOCC Aggregations via Recurrent Deployment) is a component
designed as an entrypoint script within the PADOCC environment to automate the operation of the pipeline.
Groups of datasets (called ``flocks``) can be created by producing input files and placing them in a persistent
directory accessible to a deployment of PADOCC. The deployment operates an hourly check in this directory, 
and picks up any added files or any changes to existing files. The groups specified can then be automatically 
run through all sections of the pipeline.

The deployment of SHEPARD at CEDA requires access to the CEDA Archive on JASMIN, or other alternative permanent data storage,
as well as the capability to deploy to (JASMIN) LOTUS cluster for job submissions. A SHEPARD execution can submit parallel jobs across multiple groups, up to a defined batch limit (maximum number of jobs submitted). The selection of jobs across groups is unordered (random) such that new groups can be added to the suite of available groups without 'jumping the queue'.

Deploying SHEPARD
-----------------

.. code::

    $ shepard batch --conf <config.yaml> --parallel --autolog

Running this example command will execute a batch of SHEPARD jobs (in parallel), with autologging to a directory specified in the config file. The config file is a YAML file that should have a structure similar to:

.. code::
    {
        "source_venv": "/path/to/virtual/env",
        "batch_limit": 1000,
        "flock_dir": "/path/to/set/of/groups",
        "complete_dir": "/path/to/completion/directory",
        "common_valid": "/path/to/validation/template",
        "obliterate_quarantine": false,

        "sweep_dir": "/path/to/sweep/dir",
        "cache_dir": "/path/to/cache/dir",
        "flock_size_limit": 100,
        "flock_limit": 20,
        "deployment_label": "SHEPARD"
    }

The ``sweep_dir`` can be used to collect incoming manifest files from some external location that cannot hold them for long. Depending on the setup it may be wise to use a ``cache_dir`` to hold all input files awaiting initialisation, where that directory can become very full without causing issues.

The ``batch_limit`` specifies how many jobs are allowed to be deployed in one batch operation, with the ``flock_limit`` and ``flock_size_limit`` setting boundaries for how many active flocks can be processed at the same time. If the flock limit is reached, SHEPARD will not add new flocks from the input files.

The SHEPARD command can be set up as a CRON job, to run e.g every hour to check on all groups within the flock directory. A task list is assembled for each group, where a project can be assigned the next task in the pipeline if it has succeeded in passing the previous phase. For example, a project which has a ``Success`` status for ``Compute``, will be added to the task list as a ``Validate`` task, as this is the next stage. Projects that have presented a non-standard status message (i.e an error of some kind) will either be moved to the ``shp_sideline`` group (which will be created automatically) or to the ``shp_quarantine`` group if the errors are identified as Fatal. This will change in time as the fatal errors can be overcome by using a different cloud format.

SHEPARD will automatically complete projects that have a success or warning status from the validation phase, but these projects will not be deleted individually. The whole group is summarised for data analysis before deletion, to establish the total amount of source data assessed and cloud data generated. Non-fatal warnings are typically differences in metadata, and will be preserved on completion via the ``data_report`` which is also moved to the completion directory for each project. Until every project is eligible for completion, a group will remain in the SHEPARD pipeline.

Manual intervention
-------------------

As the flock directory is accessible from any terminal, any manual operation may also be performed on any group, especially in the case of recorded errors. SHEPARD runs according to the schedule set by a CRON job, but between those events any changes can be made to the groups, that will then take effect in the next SHEPARD iteration.

Ignored Groups
--------------

SHEPARD will ignore any group with a ``.shpignore`` file present in the group directory. This takes the group out of consideration for any SHEPARD processing, without having to move the whole group. Manual/parallel processes can still be performed, but this group will no longer be automatically updated by SHEPARD.

Status viewer
-------------

The SHEPARD command can be used with the ``status`` operation to get a high-level overview of all groups within the ``flock_dir`` that do not have a ``.shpignore`` file. This interface is a simplified view compared to the individual group ``status`` view, and shows only the total number of projects in each phase that have ``error``, ``pending`` or ``success`` status messages. The total ``complete`` projects is shown at the end of the line along with total ``errors (ET)``

.. code::

    $ shepard status --conf <config.yaml>

    Group ID:             | By phase: error, pending, success
    cmip6_3              -> i: 0    0    0     |s: 0    8    0     |c: 0    0    0     |v: 0    0    4     |c:974  , ET: 0
    cmip6_4              -> i: 0    0    0     |s: 0    15   0     |c: 3    0    980   |v: 0    0    0     |c:0    , ET: 3
    cmip6_5              -> i: 0    0    0     |s: 0    15   0     |c: 0    0    0     |v: 0    0    0     |c:977  , ET: 0
    cmip6_7              -> i: 0    0    0     |s: 0    13   0     |c: 0    0    0     |v: 0    0    844   |c:132  , ET: 0
    cmip6_10             -> i: 0    0    0     |s: 0    0    0     |c: 0    0    39    |v: 0    1    15    |c:913  , ET: 0
    cmip6_11             -> i: 0    0    0     |s: 0    0    0     |c: 0    1    0     |v: 4    0    0     |c:976  , ET: 4
    cmip6_13             -> i: 0    0    0     |s: 0    0    0     |c: 0    0    0     |v: 0    0    12    |c:924  , ET: 0
    cmip6_14             -> i: 0    0    0     |s: 0    0    0     |c: 0    0    20    |v: 0    0    4     |c:887  , ET: 0
    cmip6_15             -> i: 0    0    0     |s: 33   0    0     |c: 13   0    942   |v: 0    0    0     |c:0    , ET: 46
    cmip6_16             -> i: 0    0    0     |s: 0    0    6     |c: 44   39   856   |v: 0    0    0     |c:0    , ET: 44
    cmip6_17             -> i: 0    0    0     |s: 0    100  59    |c: 0    0    0     |v: 0    0    0     |c:839  , ET: 0
    cmip6_20             -> i: 0    0    0     |s: 0    0    6     |c: 0    79   913   |v: 0    0    0     |c:0    , ET: 0
    cmip6_21             -> i: 0    0    0     |s: 0    20   0     |c: 6    0    1     |v: 0    0    86    |c:885  , ET: 6
    cmip6_22             -> i: 0    0    0     |s: 0    51   0     |c: 58   11   0     |v: 0    4    0     |c:713  , ET: 58
    cmip6_24             -> i: 0    0    0     |s: 0    212  0     |c: 0    1    0     |v: 0    11   0     |c:769  , ET: 0
    cmip6_26             -> i: 0    0    0     |s: 0    19   0     |c: 8    148  789   |v: 0    0    0     |c:0    , ET: 8
    cmip6_27             -> i: 0    0    0     |s: 46   34   918   |c: 0    0    0     |v: 0    0    0     |c:0    , ET: 46
    cmip6_28             -> i: 0    0    0     |s: 0    112  0     |c: 4    99   766   |v: 0    0    0     |c:0    , ET: 4
    Errors: 219