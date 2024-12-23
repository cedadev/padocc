==============
Scanner Module
==============

A scan operation is performed across a group of datasets/projects to determine specific
properties of each project and some estimates of time/memory allocations that will be
required in later phases.

The scan phase can be activated with the following:

.. code:: python

    mygroup = GroupOperation(
        'my-group',
        workdir='path/to/pipeline/directory'
    )
    # Assuming this group has already been initialised from a file.

    mygroup.run('scan',mode='kerchunk')

.. automodule:: padocc.phases.scan
    :members:

==============
Compute Module
==============

Computation will either refer to outright data conversion to a new format, 
or referencing using one of the Kerchunk drivers to create a reference file. 
In either case the computation may be extensive and require processing in the background
or deployment and parallelisation across the group of projects.

Computation can be executed in serial for a group with the following:

.. code:: python

    mygroup = GroupOperation(
        'my-group',
        workdir='path/to/pipeline/directory'
    )
    # Assuming this group has already been initialised and scanned

    mygroup.run('compute',mode='kerchunk')

.. automodule:: padocc.phases.compute
    :members:
    :show-inheritance:

=================
Validation Module
=================

Finally, it is advised to run the validator for all projects in a group to determine any issues
with the conversion process. Some file types or specific arrangements may produce unwanted effects
that result in differences between the original and new representations. This can be identified with the
validator which checks the Xarray representations and identifies differences in both data and metadata.

.. code:: python

    mygroup = GroupOperation(
        'my-group',
        workdir='path/to/pipeline/directory'
    )
    # Assuming this group has already been initialised, scanned and computed

    mygroup.run('validate')

    # The validation reports will be saved to the filesystem for each project in this group
    # as 'data_report.json' and 'metadata_report.json'

.. automodule:: padocc.phases.validate
    :members:
