CCI Water Vapour Example
========================

The CCI water vapour input CSV file can be found in ``extensions/example_water_vapour/`` within this repository. This guide will take you through running the pipeline for this example set of 4 datasets.
Assuming you have already gone through the setup instructions in *Getting Started*, you can now proceed with creating a group for this test dataset.

A new *group* is created within the pipeline using the ``init`` operation as follows:

::

    python group_run.py init <my_new_group> -i extensions/example_water_vapour/water_vapour.csv -v

.. note::
    Multiple flag options are available throughout the pipeline for more specific operations and methods. In the above case we have used the (-v) *verbose* flag to indicate we want to see the ``[INFO]`` messages put out by the pipeline. Adding a second (v) would also show ``[DEBUG]`` messages.
    Also the ``init`` phase is always run as a serial process since it just involves creating the directories and config files required by the pipeline.

The output of the above command should look something like this:

.. code-block:: console

    INFO [main-group]: Running init steps as serial process
    INFO [init]: Starting initialisation
    INFO [init]: Copying input file from relative path - resolved to /home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder
    INFO [init]: Creating project directories
    INFO [init]: Creating directories/filelists for 1/4
    INFO [init]: Updated new status: init - complete
    INFO [init]: Creating directories/filelists for 2/4
    INFO [init]: Updated new status: init - complete
    INFO [init]: Creating directories/filelists for 3/4
    INFO [init]: Updated new status: init - complete
    INFO [init]: Creating directories/filelists for 4/4
    INFO [init]: Updated new status: init - complete
    INFO [init]: Created 24 files, 8 directories in group my_new_group
    INFO [init]: Written as group ID: my_new_group

Ok great, we've initialised the pipeline for our new group! Here's a summary diagram of what directories and files were just created:

::

    WORKDIR
      - groups
         -  my_new_group
             -  proj_codes
                 -  main.txt
             -  blacklist_codes.txt
             -  datasets.csv # (a copy of the input file)

      - in_progress
         -  my_new_group
             -  code_1 # (codes 1 to 4 in this example)
                 -  allfiles.txt
                 -  base-cfg.json
                 -  phase_logs
                     -  scan.log
                     -  compute.log
                     -  validate.log
                 -  status_log.csv

For peace of mind and to check you understand the pipeline assessor tool we would suggest running this command next:

::

    python assess.py progress my_new_group

Upon which your output should look something like this:

.. code-block:: console

    Group: my_new_group
    Total Codes: 4

    Pipeline Current:

    init      : 4     [100.%] (Variety: 1)
        - complete : 4

    Pipeline Complete:

    complete  : 0     [0.0 %]

All 4 of our datasets were initialised successfully, no datasets are complete through the pipeline yet.

The next steps are to ``scan``, ``compute``, and ``validate`` the datasets which would complete the pipeline.

.. note::
    For each of the above phases, jobs will be submitted to SLURM when using the ``group_run`` script. Please make sure to wait until all jobs are complete for one phase *before* running the next job.
    After each job, check the progress of the pipeline with the same command as before to check all the datasets ``complete`` as expected. See below on what to do if datasets encounter errors.

.. code-block:: console

    python group_run.py scan my_new_group
    python group_run.py compute my_new_group
    python group_run.py validate my_new_group

An more complex example of what you might see while running the pipeline in terms of errors encountered can be found below:

.. code-block:: console

    Group: cci_group_v1
    Total Codes: 361

    Pipeline Current:

    compute   : 21    [5.8 %] (Variety: 2)
        - complete                 : 20
        - KeyError 'refs'          : 1

    Pipeline Complete:

    complete  : 185   [51.2%]

    blacklist : 155   [42.9%] (Variety: 8)
        - NonKerchunkable          : 50
        - PartialDriver            : 3
        - PartialDriverFail        : 5
        - ExhaustedMemoryLimit     : 56
        - ExhaustedTimeLimit       : 18
        - ExhaustedTimeLimit*      : 1
        - ValidationMemoryLimit    : 21
        - ScipyDimIssue            : 1

In this example ``cci_group_v1`` group, 185 of the datasets have completed the pipeline, while 155 have been excluded (See blacklisting in the Assessor Tool section). 
Of the remaining 21 datasets, 20 of them have completed the ``compute`` phase and now need to be run through ``validate``, but one encountered a KeyError which needs to be inspected. To view the log for this dataset we can use the command below:

.. code-block:: console

    python assess.py progress cci_group_v1 -e "KeyError 'refs'" -p compute -E

This will match with our ``compute``-phase error with that message, and the (-E) flag will give us the whole error log from that run. This may be enough to assess and fix the issue but otherwise, to rerun just this dataset a rerun command will be suggested by the assessor:

.. code-block:: console

    Project Code: 201601-201612-ESACCI-L4_FIRE-BA-MSI-fv1.1 - <class 'KeyError'>'refs'
    Rerun suggested command:    python single_run.py compute 218 -G cci_group_v1 -vv -d

This rerun command has several flags included, the most importand here is the (-G) group flag, since we need to use the ``single_run`` script so now need to specify the group. The (-d) dryrun flag will simply mean we are not producing any output files since we may need to test and rerun several times.



