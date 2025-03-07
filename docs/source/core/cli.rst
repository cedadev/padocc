========================
PADOCC Command Line Tool
========================

.. image:: _images/pipeline.png
   :alt: Stages of the PADOCC workflow

The command-line tool ``padocc`` allows quick deployments of serial and parallel processing jobs for your projects and groups within the padocc workspace. The core phases are most readily executed using the command line tool.

.. note::

    For information on how to setup the padocc environment, please see the Installation section of this documentation. Some general tips would be to:

    1. Ensure you have the padocc package installed and the command line tool is accessible. You can check this by running ``which padocc`` in your terminal.

    2. Set the working directory ``WORKDIR`` environment variable. All pipeline directories and files will be created under this directory, which includes all the groups you define. It is suggested to have only one working directory where possible, although if a distinction is needed for different groups of datasets, using multiple working directories can be done with user discretion.

Using the CLI Tool
==================

General Command Form
--------------------
The general form of a command for padocc should be to call the command line tool ``padocc`` with a minimum of the ``phase`` argument specified afterwards. E.g:

.. code-block:: console

    $ padocc init


In almost all cases, other arguments will be necessary for any particular operation you would like to perform.

.. code-block:: console

    usage: padocc phase [-h] [-f] [-v] [-d] [-T] [-b BYPASS] [-w WORKDIR] [-G GROUPID] [-s SUBSET] [-r REPEAT_ID] [-p PROJ_CODE] [-C MODE] [-i INPUT] [-n NEW_VERSION] [-t TIME_ALLOWED] [--mem-allowed MEM_ALLOWED] [-M MEMORY] [-B] [-e VENVPATH] [-A] [--allow-band-increase]

Padocc CLI Flags
----------------
The flags above show all the different possible options for operating the pipeline. Listed here are some of the more common flags that can be applied to most or all of the different ``phased`` operations for padocc.

.. code-block:: console

  -h, --help            show this help message and exit
  -f, --forceful        Force overwrite of steps if previously done
  -v, --verbose         Print helpful statements while running (add more v's for greater verbosity)
  -d, --dryrun          Perform dry-run (i.e no new files/dirs created)
  -T, --thorough        Thorough processing - start from scratch

  -b BYPASS, --bypass-errs (See the Deep Dive section for info on this feature)
  -w WORKDIR, --workdir WORKDIR
                        Working directory for pipeline (if not specified as an environment variable.)
  -G GROUPID, --groupID GROUPID
                        Group identifier label

Other flags listed in the command above are described in the Complex Operation section of this documentation.

Create a group from scratch (optional)
--------------------------------------
This optional first step allows you to create empty groups in the workspace that can be properly initialised later.

.. code-block:: console

    $ padocc new -G my-new-group

There is no particular advantage to creating empty groups but this may be beneficial for organisation of multiple new groups where the data is still being collected.

Special Functions
=================

The following accepted options to the ``phase`` argument act as shortcuts to specific functions in padocc available via an interactive session. These functions are now available via the CLI in a limited capacity, and use the ``--special`` kwarg as a catch-all for providing configuration info to these functions.
 - ``list``: Lists all groups in the current workspace and their contents.
 - ``status``: Shows status of all projects in a group (requires ``-G`` flag)
 - ``add``: Enables adding projects to a group, including via the moles tags option (requires ``-G``, moles enabled via ``--special moles``)
 - ``check``: Check an attribute in all projects across the group (requires ``-G``, supply attribute via ``--special <attribute>``)
 - ``complete``: Enables the completion workflow for complete projects (requires ``-G``, supply completion directory via ``--special <dir>``)

Pipeline Functions
==================

The following descriptions are for main pipeline functions, most of which are parallelisable with the ``--parallel`` flag.

Initialise a group
------------------

The pipeline takes a CSV (or similar) input file from which to instantiate a ``GroupOperation``, which includes:
 - creating subdirectories for all associated datasets (projects)
 - creating multiple group files with information regarding this group.

.. code-block:: console

    $ padocc init -G my-new-group -i path/to/input_file.csv

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

.. code-block:: console
    
    $ padocc scan -G my-group -C kerchunk

Alternatively, you can run any of the phases interactively in a python shell/notebook environment:

.. code:: python

    mygroup = GroupOperation(
        'my-group',
        workdir='path/to/pipeline/directory'
    )
    # Assuming this group has already been initialised from a file.

    mygroup.run('scan',mode='kerchunk')

The above demonstrates why the command line tool is easier to use for phased operations, as most of the configurations are known and handled using the various flags. Interactive operations (like checking specific project properties etc.) are not covered by the CLI tool, so need to be completed using an interactive environment.

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

.. code-block:: console

    padocc compute -G my-group

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

    $ padocc validate -G my-group

Next Steps
----------

Cloud products that have been validated are moved to a ``complete`` directory with the project code as the name, plus the revision identifier `abX.X` - learn more about this in the Extra section.
These can then be linked to a catalog or ingested into the CEDA archive where appropriate.
