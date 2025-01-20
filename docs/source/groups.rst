Groups in PADOCC
================

The advantage of using PADOCC over other tools for creating cloud-format files is the scalability built-in, with parallelisation and deployment in mind.
PADOCC allows the creation of groups of datasets, each with N source files, that can be operated upon as a single entity. 
The operation can be applied to all or a subset of the datasets within the group with relative ease. Here we outline some basic functionality of the ``GroupOperation``. 
See the source documentation page for more detail.

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