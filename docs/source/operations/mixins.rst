PADOCC Core Mixin Behaviours
============================

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