CCI Water Vapour Example
========================

The CCI water vapour input CSV file can be found in ``extensions/example_water_vapour/`` within this repository. This guide will take you through running the pipeline for this example set of 4 datasets.
Assuming you have already gone through the setup instructions in *Getting Started*, you can now proceed with creating a group for this test dataset.

A new *group* is created within the pipeline using the ``init`` operation as follows:

::

    padocc init -G <my_new_group> -i extensions/example_water_vapour/water_vapour.csv -v

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

All 4 of our datasets were initialised successfully, no datasets are complete through the pipeline yet.

The next steps are to ``scan``, ``compute``, and ``validate`` the datasets which would complete the pipeline.

.. note::
    For each of the above phases, jobs will be submitted to SLURM when using the ``group_run`` script. Please make sure to wait until all jobs are complete for one phase *before* running the next job.
    After each job, check the progress of the pipeline with the same command as before to check all the datasets ``complete`` as expected. See below on what to do if datasets encounter errors.

.. code-block:: console

    padocc scan -G my_new_group
    padocc compute -G my_new_group
    padocc validate -G my_new_group

This section will be updated for the full release of v1.3 with additional content relating to the assessor tool.