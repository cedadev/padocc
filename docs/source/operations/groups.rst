Groups in PADOCC
================

The advantage of using PADOCC over other tools for creating cloud-format files is the scalability built-in, with parallelisation and deployment in mind.
PADOCC allows the creation of groups of datasets, each with N source files, that can be operated upon as a single entity. 
The operation can be applied to all or a subset of the datasets within the group with relative ease. Here we outline some basic functionality of the ``GroupOperation``. 
See the source documentation page for more detail.

Help with the GroupOperation
----------------------------

.. code-block:: console
    
    Group Operator
    > group.get_stac_representation()      - Provide a mapper and obtain values in the form of STAC records for all projects
    > group.info()                         - Obtain a dictionary of key values
    > group.run()                          - Run a specific operation across part of the group.
    > group.save_files()                   - Save any changes to any files in the group as part of an operation
    > group.check_writable()               - Check if all directories are writable for this group.
    Allocations:
    > group.create_allocations()           - Create a set of allocations, returns a binned list of bands
    > group.create_sbatch()                - Create sbatch script for submitting to slurm.
    Initialisations:
    > group.init_from_stac()               - Initialise a group from a set of STAC records
    > group.init_from_file()               - Initialise a group based on values from a local file
    Evaluations:
    > group.get_project()                  - Get a project operator, indexed by project code
    > group.repeat_by_status()             - Create a new subset group to (re)run an operation, based on the current status
    > group.remove_by_status()             - Delete projects based on a given status
    > group.merge_subsets()                - Merge created subsets
    > group.summarise_data()               - Get a printout summary of data representations in this group
    > group.summarise_status()             - Summarise the status of all group member projects.
    Modifiers:
    > group.add_project()                  - Add a new project, requires a base config (padocc.core.utils.BASE_CFG) compliant dictionary
    > group.remove_project()               - Delete project and all associated files
    > group.transfer_project()             - Transfer project to a new "receiver" group
    > group.merge()                        - Merge two groups
    > group.unmerge()                      - Split one group into two sets, given a list of datasets to move into the new group.

Instantiating a Group
---------------------

A group is most easily created using a python terminal or Jupyter notebook, with a similar form to the below.

.. code-block:: python

    from padocc import GroupOperation

    my_group = GroupOperation(
        'mygroup',
        workdir='path/to/dir',
        verbose=1
    )

At the point of defining the group, all required files and folders are created on the file system with default
or initial values for some parameters. Further processing steps which incur changes to parameters will only be saved
upon completion of an operation. If in doubt, all files can be saved with current values using ``.save_files()``
for the group.

This is a blank group with no attached parameters, so the initial values in all created files will be blank or templated
with default values. To fill the group with actual data, we need to initialise from an input file.

.. note::

    In the future it will be possible to instantiate from other file types or records (e.g STAC) but for now the accepted
    format is a csv file, where each entry fits the format:
    ``project_code, /file/pattern/**/*.nc, /path/to/updates.json or empty, /path/to/removals.json or empty``

Initialisation from a File
--------------------------

A group can be initialised from a CSV file using:

.. code-block:: python

    my_group.init_from_file('/path/to/csv.csv')

Substitutions can be provided here if necessary, of the format:

.. code-block:: python

    substitutions = {
        'init_file': {
            'swap/this/for':'this'
        },
        'dataset_file': {
            'swap/that/for':'that'
        },
        'datasets': {
            'swap/that/for':'these'
        },
    }

Where the respective sections relate to the following:
 - Init file: Substitutions to the path to the provided CSV file
 - Dataset file: Substitutions in the CSV file, specifically with the paths to ``.txt`` files or patterns.
 - Datasets: Substitutions in the ``.txt`` file that lists each individual file in the dataset.

Applying an operation
---------------------

Now we have an initialised group, in the same group instance we can apply an operation.

.. code-block:: python

    mygroup.run('scan', mode='kerchunk')

The operation/phase being applied is a positional argument and must be one of ``scan``, ``compute`` or ``validate``. 
(``ingest/catalog`` may be added with the full version 1.3). There are also several keyword arguments that can be applied here:
 - mode: The format to use for the operation (default is Kerchunk)
 - repeat_id: If subsets have been produced for this group, use the subset ID, otherwise this defaults to ``main``.
 - proj_code: For running a single project code within the group instead of all groups.
 - subset: Used in combination with project code, if both are set they must be integers where the group is divided into ``subset`` sections, and this operation is concerned with the nth one given by ``proj_code`` which is now an integer.
 - bypass: BypassSwitch object for bypassing certain errors (see the Deep Dive section for more details)

Merging or Unmerging
--------------------
**currently in development - alpha release**