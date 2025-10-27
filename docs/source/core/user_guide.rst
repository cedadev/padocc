================
Basic User Guide
================

This section of the documentation outlines the core phases of the padocc pipeline and how to run them. For features beyond just running the core phases, please see either the CLI examples section or the All Operations section. 

Pipeline Functions
==================

The following descriptions are for main pipeline functions, most of which are parallelisable with the ``--parallel`` flag.

Initialise a group
------------------

The pipeline takes a CSV (or similar) input file from which to instantiate a ``GroupOperation``, which includes:
 - creating subdirectories for all associated datasets (projects)
 - creating multiple group files with information regarding this group.


**Initialisation at the Command Line**

.. code-block:: console

    $ padocc init -G my-new-group -i path/to/input_file.csv

**Initialisation using Python**

.. code:: python

    >>> from padocc import GroupOperation

    >>> # Access your working directory from the external environment - if already defined
    >>> import os
    >>> workdir = os.environ.get("WORKDIR")

    >>> my_group = GroupOperation('my_new_group',workdir=workdir)
    >>> my_group.init_from_file(input_file)

.. note::

    PADOCC 1.3.6 added the ``padocc_sh`` command which uses the ``Pdb`` debug tool to open a python terminal that already has core padocc modules imported. The ``WORKDIR`` attribute will also be picked up from your environment, so you do not need to pass this to padocc if you already have it set. See the section in bespoke features for details on the PADOCC shell.

An example of the output for this command, when the ``-v`` flag is added can be found below. The test data is composed of two ``rain`` datasets each with 5 NetCDF files filles with arbitrary data. You can access this test data through the `github repo<https://github.com/cedadev/padocc>_`. Under ``padocc/tests/data``:

.. code-block:: console

    INFO [PADOCC-CLI-init]: Starting initialisation
    INFO [PADOCC-CLI-init]: Copying input file from relative path - resolved to <your-directory-structure>/file.csv
    INFO [PADOCC-CLI-init]: Creating project directories
    INFO [PADOCC-CLI-init]: Creating directories/filelists for 1/2
    INFO [PADOCC-CLI-init]: Updated new status: init - Success
    INFO [PADOCC-CLI-init]: Creating directories/filelists for 2/2
    INFO [PADOCC-CLI-init]: Updated new status: init - Success
    INFO [PADOCC-CLI-init]: Created 12 files, 4 directories in group rain-example
    INFO [PADOCC-CLI-init]: Written as group ID: rain-example

Scanning a Project
------------------

(See the PADOCC Terms and Operators section for what consitutes a ``Project``)

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

.. code-block:: console
    
    $ padocc scan -G my-group -C kerchunk

Or in a python/padocc shell:

.. code:: python

    mygroup.run('scan',mode='kerchunk')

The above demonstrates why the command line tool is easier to use for phased operations, as most of the configurations are known and handled using the various flags. Interactive operations (like checking specific project properties etc.) are not covered by the CLI tool, so need to be completed using an interactive environment.

Running a Computation
---------------------

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

.. code-block:: console

    padocc compute -G my-group -v

.. code:: python

    # Typical flags on the CLI can be passed here too.
    mygroup.run('compute', verbose=1)

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

.. code-block:: console

    $ padocc validate -G my-group --valid bypass.json

.. code:: python

    # Typical flags on the CLI can be passed here too.
    mygroup.run('compute', verbose=1, error_bypass='bypass.json')

Here we are passing an **error bypass** file to the validation, that will allow for certain known errors to be bypassed. For example, the validator will often report that all variables/dimensions present in a different order between the native file and the cloud product. This is not often an issue, so can be ignored. The error still registers in the final data report, but it will have a ``skip`` label attached. See the Validation Report section in **Bespoke Features** for more details.