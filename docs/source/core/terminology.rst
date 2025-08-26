==========================
PADOCC Terms and Operators
==========================

Terminology Guide
=================

This section is designed as a quick reference for commonly used terms in PADOCC.

* __Project__: The term for a set of native files to be aggregated into a single cloud product. PADOCC does not use ``dataset`` for this term as that can refer to the individual files in some instances.
* __Group__: A group of projects to be acted upon in parallel. The grouping of projects is arbitrary and defined only by the user. Projects can be transferred between groups if needed.
* __Product__: The final output of PADOCC for a given project, typically this will be a kerchunk file/store or a zarr store. This is typically referred to as a Cloud product, or just product for short.
* __Revision__: The version number and cloud format identifier given by PADOCC to each output product (See below.)
* __Aggregation__: The process of combining multiple source files into a larger dataset, either virtual or 'real'
* __Virtualisation__: Aggregation of multiple source files into an explicitly virtual dataset, using either ``Kerchunk`` or ``VirtualiZarr``.

Revision Numbers
----------------

The PADOCC revision numbers for each product are auto-generated using the following rules.

 * All projects begin with the revision number ``1.0``.
 * The first number denotes major updates to the product, for instance where a data source file has been replaced.
 * The second number denotes minor changes like alterations to attributes and metadata.
 * The cloud format ``k`` or ``z`` comes before the version number, as well as an ``r`` letter which indicates that the file is ``remote-enabled``. This occurs automatically for kerchunk files that have had 'download links' applied - from the command line this can be done as part of the completion workflow.

Virtualisation in PADOCC (12.08.2025)
=====================================

PADOCC v1.4.0 will see the release of the first PADOCC version that now incorporates some elements of the Zarr-developer package ``VirtualiZarr``. Incorporation is currently limited so to not remove certain features that already exist in PADOCC.

VirtualiZarr is only implemented in the aggregation phase, where kerchunk references are combined, and even then this only occurs where the CFA creation has been successful. This is due to the strict ordering requirements for files input to VirtualiZarr, where usually the correct ordering of input files is not guaranteed by PADOCC, and the standard kerchunk aggregator module does not require it.

VirtualiZarr is not currently used for creating and caching individual kerchunk files as there is currently no way to disable the inline encoding when virtualising to kerchunk, and VirtualiZarr is not able to re-load cached kerchunk files with inline encoded variables.

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

Projects in PADOCC
==================

To differentiate syntax of datasets/datafiles with other packages that have varying definitions of those terms,
PADOCC uses the term ``Project`` to refer to a set of files to be aggregated into a single 'Cloud Product'. 

The ``ProjectOperation`` class within PADOCC allows us to access all information about a specific dataset, including
fetching data from files within the pipeline directory. This class also inherits from several Mixin classes which 
act as containers for specific behaviours for easier organisation and future debugging.

The Project Operator class
--------------------------

The 'core' behaviour of all classes is contained in the ``ProjectOperation`` class.
This class has public UI methods like ``info`` and ``help`` that give general information about a project, 
and list some of the other public methods available respectively.

.. code-block:: console

    Project Operator:
    > project.info()                       - Get some information about this project
    > project.get_version()                - Get the version number for the output product
    > project.save_files()                 - Save all open files related to this project
    Dataset Handling:
    > project.dataset                      - Default product Filehandler (pointer) property
    > project.dataset_attributes           - Fetch metadata from the default dataset
    > project.kfile                        - Kerchunk Filehandler property
    > project.kstore                       - Kerchunk (Parquet) Filehandler property
    > project.cfa_dataset                  - CFA Filehandler property
    > project.zstore                       - Zarr Filehandler property
    > project.update_attribute()           - Update an attribute within the metadata
    Status Options:
    > project.get_last_run()               - Get the last performed phase and time it occurred
    > project.get_last_status()            - Get the status of the previous core operation.
    > project.get_log_contents()           - Get the log contents of a previous core operation
    Extra Properties:
    > project.outpath                      - path to the output product (Kerchunk/Zarr)
    > project.outproduct                   - name of the output product (minus extension)
    > project.revision                     - Revision identifier (major + minor version plus type indicator)
    > project.version_no                   - Get major + minor version identifier
    > project.cloud_format[EDITABLE]       - Cloud format (Kerchunk/Zarr) for this project
    > project.file_type[EDITABLE]          - The file type to use (e.g JSON/parq for kerchunk).
    > project.source_format                - Get the driver used by kerchunk
    > project.get_stac_representation()    - Provide a mapper, fills with values from the project to create a STAC record.

Key Functions:
 - Acts as an access point to all information and data about a project (dataset).
 - Can adjust values within key files (abstracted) by setting specific parameters of the project instance and then using ``save_files``.
 - Enables quick stats gathering for use with group statistics calculations.
 - Can run any process on a project from the Project Operator.

PADOCC Core Mixins
==================

The core module comes enabled with a host of containerised "behaviour" classes, that other classes in padocc can inherit from. 
These classes apply specifically to different components of padocc (typically ``ProjectOperation`` or ``GroupOperation``) and should not be used on their own.

Directory Mixin
---------------

**Target: Project or Group**

The directory mixin class contains all behaviours relating to creating directories within a project (or group) in PADOCC.
This includes the inherited ability for any project to create its parent working directory and group directory if needed, as well
as a subdirectory for cached data files. The switch values ``forceful`` and ``dryrun`` are also closely tied to this 
container class, as the creation of new directories may be bypassed/forced if they exist already, or bypassed completely in a dry run.

Status Mixin
-----------------

**Target: Project Only**

Previously, all evaluations were handled by an assessor module (pre 1.3), but this has now been reorganised
into a mixin class for the projects themselves, meaning any project instance has the capacity for self-evaluation. The routines
grouped into this container class relate to the self analysis of details and parameters of the project and various 
files:
 - get last run: Determine the parameters used in the most recent operation for a project.
 - get last status: Get the status of the most recent (completed) operation.
 - get log contents: Examine the log contents for a specific project.

This list will be expanded in the full release version 1.3 to include many more useful evaluators including
statistics that can be averaged across a group.

Properties Mixin
----------------

**Target: Project Only**

A collection of dynamic properties about a specific project. The Properties Mixin class abstracts any
complications or calculations with retrieving specific parameters; some may come from multiple files, are worked out on-the-fly
or may be based on an external request. Properties currently included are:
 - Outpath: The output path to a 'product', which could be a zarr store, kerchunk file etc.
 - Outproduct: The name of the output product which includes the cloud format and version number.
 - Revision/Version: Abstracts the construction of revision and version numbers for the project.
 - Cloud Format: Kerchunk/Zarr etc. - value stored in the base config file and can be set manually for further processing.
 - File Type: Extension applied to the output product, can be one of 'json' or 'parquet' for Kerchunk products.
 - Source Format: Format(s) detected during scan - retrieved from the detail config file after scanning.

The properties mixin also enables a manual adjustment of some properties, like cloud format or file type, but also enables
minor and major version increments. This will later be wrapped into an ``Updater`` module to enable easier updates to 
Cloud Product data/metadata.

Dataset Mixin
-------------

**Target: Project Only**

This class handles all elements of the 'cloud product properties'. 
Each project may include one or more cloud products, each handled by a filehandler of the correct type.
The default cloud product is given by the ``dataset`` property defined in this mixin, while other specific products are given by specific properties.
The behaviours for all dataset objects are contained here in one place for ease of use, and ease of integration of features between the ``ProjectOperation`` and ``filehandler`` objects.