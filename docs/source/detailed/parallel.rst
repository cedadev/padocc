===================
Parallel Deployment
===================

Parallelisation with SLURM
==========================

Padocc now supports parallelisation via LOTUS on JASMIN, through the SLURM batch job manager. Use the CLI flag ``--parallel`` to deploy sets (arrays) of jobs to the cluster. Typically an array job will constitute processing multiple datasets at once, i.e all datasets in a given group. To deploy a subset of a group, see the documentation around **repeat IDs** and how to create subsets within the group based on past runs and other conditions.

Here is an example of deploying a set of jobs to SLURM.

.. code::

    $ padocc compute -G my-test-group --parallel -vv

This will deploy all jobs with debug logs, meaning all log outputs will be displayed. All projects covered by this deployment have their own log files that can be accessed using interactive means per project, and the results from a deployment can be most easily viewed using the ``status`` special feature (see Extra Details).

Other parameters that can be passed as flags for parallel deployments are listed here:
 - ``-t TIME_ALLOWED``: Time limit for this set of jobs (default applied per phase unless specified.)
 - ``-M MEMORY``: Memory limit for each job (default applies per phase.)
 - ``-e VENVPATH``: Must be supplied for all jobs to have the correct environment, or will use the current active environment.

Lotus 2 configurations
----------------------

PADOCC is now configured for Lotus 2 deployments on JASMIN. Simply create a Lotus 2 config file which matches the following options.

.. code::

    {
        "lotus_vn": 2,
        "partition": "standard",
        "account": "no-project",
        "qos": "standard"
    }

See the `JASMIN help docs <https://help.jasmin.ac.uk/docs/software-on-jasmin/rocky9-migration-2024/#new-lotus2-cluster-initial-submission-guide>`_ for guidance on how to configure for Lotus 2 deployments.

This config file is discovered by padocc using the ``$LOTUS_CFG`` environment variable, which must be set to the location of this file. After this step, no further actions are required, the normal parallelisation methods listed above apply as standard.

.. note::

    For Jasmin users with SLURM access, you should check after every ``scan``, ``compute`` and ``validate`` that your SLURM jobs are running properly:
    
    ``squeue -u <jasmin_username>``

    And once the SLURM jobs are complete you should check error logs to see which jobs were successful and which failed for different reasons. See the Interactive section for how to check status and logs of projects in the pipeline.