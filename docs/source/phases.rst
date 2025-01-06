=============================
Phases of the PADOCC Pipeline
=============================

.. image:: _images/padocc.png
   :alt: Stages of the PADOCC workflow

**Initialisation of a Group of Datasets**


The pipeline takes a CSV (or similar) input file from which to instantiate a ``GroupOperation``, which includes:
 - creating subdirectories for all associated datasets (projects)
 - creating multiple group files with information regarding this group.

Scan
----

The first main phase of the pipeline involves scanning a subset of the native source files to determine certain parameters:

* Ensure source files are compatible with one of the available converters for Kerchunk/Zarr etc.:
* Calculate expected memory (for job allocation later.)
* Calculate estimated chunk sizes and other values.
* Determine suggested file type, including whether to use JSON or Parquet for Kerchunk references.
* Identify Identical/Concat dims for use in **Compute** phase.
* Determine any other specific parameters for the dataset on creation and concatenation.

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

Compute
-------

Building the Cloud/reference product for a dataset requires a multi-step process:

Example for Kerchunk:

* Create Kerchunk references for each archive-type file.
* Save cache of references for each file prior to concatenation.
* Perform concatenation (abort if concatenation fails, can load cache on second attempt).
* Perform metadata corrections (based on updates and removals specified at the start)
* Add Kerchunk history global attributes (creation time, pipeline version etc.)
* Reconfigure each chunk for remote access (replace local path with https:// download path)

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

Validate
--------

Cloud products must be validated against equivalent Xarray objects from CF Aggregations (CFA) where possible, or otherwise using the original NetCDF as separate Xarray Datasets.

* Ensure all variables present in original files are present in the cloud products (barring exceptions where metadata has been altered/corrected)
* Ensure array shapes are consistent across the products.
* Ensure data representations are consistent (values in array subsets)

The validation step produced a two-sectioned report that outlines validation warnings and errors with the data or metadata
around the project. See the documentation on the validation report for more details.

It is advised to run the validator for all projects in a group to determine any issues
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

Next Steps
----------

Cloud products that have been validated are moved to a ``complete`` directory with the project code as the name, plus the revision identifier `abX.X` - learn more about this in the deep dive section.
These can then be linked to a catalog or ingested into the CEDA archive where appropriate.
