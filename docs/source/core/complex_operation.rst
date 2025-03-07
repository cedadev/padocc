==================
Complex Operations
==================

Parallelisation with SLURM
==========================

Padocc now supports parallelisation via LOTUS on JASMIN, through the SLURM batch job manager. Use the CLI flag ``--parallel`` to deploy sets (arrays) of jobs to the cluster. Typically an array job will constitute processing multiple datasets at once, i.e all datasets in a given group. To deploy a subset of a group, see the documentation around **repeat IDs** and how to create subsets within the group based on past runs and other conditions.

LOTUS2 is currently in development on JASMIN and once the cluster is deployed and stable the documentation on how to switch to LOTUS2 will be provided here.

Here is an example of deploying a set of jobs to SLURM.

.. code::

    $ padocc compute -G my-test-group --parallel -vv

This will deploy all jobs with debug logs, meaning all log outputs will be displayed. All projects covered by this deployment have their own log files that can be accessed using interactive means per project, and the results from a deployment can be most easily viewed using the ``status`` special feature (see Extra Details).

Other parameters that can be passed as flags for parallel deployments are listed here:
 - ``-t TIME_ALLOWED``: Time limit for this set of jobs (default applied per phase unless specified.)
 - ``-M MEMORY``: Memory limit for each job (default applies per phase.)
 - ``-e VENVPATH``: Must be supplied for all jobs to have the correct environment, or will use the current active environment.