Projects in PADOCC
==================

To differentiate syntax of datasets/datafiles with other packages that have varying definitions of those terms,
PADOCC uses the term ``Project`` to refer to a set of files to be aggregated into a single 'Cloud Product'. 

The ``ProjectOperation`` class within PADOCC allows us to access all information about a specific dataset, including
fetching data from files within the pipeline directory. This class also inherits from several Mixin classes which 
act as containers for specific behaviours for easier organisation and future debugging.

Directory Mixin
---------------

The directory mixin class contains all behaviours relating to creating directories within a project (or group) in PADOCC.
This includes the inherited ability for any project to create its parent working directory and group directory if needed, as well
as a subdirectory for cached data files. The switch values ``forceful`` and ``dryrun`` are also closely tied to this 
container class, as the creation of new directories may be bypassed/forced if they exist already, or bypassed completely in a dry run.

Evaluations Mixin
-----------------

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

The Project Operator class
--------------------------

The 'core' behaviour of all classes is contained in the ``ProjectOperation`` class.
This class has public UI methods like ``info`` and ``help`` that give general information about a project, 
and list some of the other public methods available respectively.

Key Functions:
 - Acts as an access point to all information and data about a project (dataset).
 - Can adjust values within key files (abstracted) by setting specific parameters of the project instance and then using ``save_files``.
 - Enables quick stats gathering for use with group statistics calculations.
 - Can run any process on a project from the Project Operator.