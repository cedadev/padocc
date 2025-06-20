=========================
Beneficial Extra Features
=========================

Remote connection to object storage
===================================

**New for v1.3.3!**

``padocc`` now has the capability to write to s3 storage endpoints for zarr stores, as well as using s3 object storage as the immediate storage medium for zarr datasets. This means that zarr stores generated via padocc can be written to object storage on creation, without filling up local disk space. Future updates will also include transfer mechanisms for Kerchunk datasets, where the kerchunk data must be edited then transferred.

Remote s3 configuration
-----------------------

The following configuration details must be passed to one of the entrypoints for remote s3 connections for padocc:
  - The ``add_project`` function when creating a new project.
  - The ``add_s3_config`` function for an existing project.

Remote s3 config:

.. code::

  {
    "s3_url":"http://<tenancy-name-o>.s3.jc.rl.ac.uk",
    "bucket_id":"my-existing-bucket",
    "s3_kwargs":None,
    "s3_credentials":"/path/to/credentials/json"
  }

For JASMIN object store tenancies see the `Object Store Services Portal <https://accounts.jasmin.ac.uk/services/object_store/>`_, plus the `documentation page <https://help.jasmin.ac.uk/docs/short-term-project-storage/using-the-jasmin-object-store/>`_ for how to set up s3 credentials. It is best to keep the credentials in a separate file as this config info will be copied to all projects being accessed.

Once this config has been added to the project, any subsequent compute operation will generate zarr data in the given object store space. Note: The creation may induce errors if interrupted halfway through. Simply delete the content on the object store and start again - this is a bug and will be fixed in due course.

PADOCC Mechanics
================

Revision Numbers
----------------

The PADOCC revision numbers for each product are auto-generated using the following rules.

 * All projects begin with the revision number ``1.0``.
 * The first number denotes major updates to the product, for instance where a data source file has been replaced.
 * The second number denotes minor changes like alterations to attributes and metadata.
 * The cloud format ``k`` or ``z`` comes before the version number, as well as an ``r`` letter which indicates that the file is ``remote-enabled``. This occurs automatically for kerchunk files that have had 'download links' applied - from the command line this can be done as part of the completion workflow.

The Validation Report
---------------------

The ``ValidateDatasets`` class produces a validation report for both data and metadata validations. 
This is designed to be fairly simple to interpret, while still being machine-readable. 
The following headings which may be found in the report have the following meanings:

1. Metadata Report (with Examples)
These are considered non-fatal errors that will need either a minor correction or can be ignored.

* ``variables.time: {'type':'missing'...}`` - The time variable is missing from the specified product.
* ``dims.all_dims: {'type':'order'}`` - The ordering of dimensions is not consistent across products.
* ``attributes {'type':'ignore'...}`` - Attributes that have been ignored. These may have already been edited.
* ``attributes {'type':'missing'...}`` - Attributes that are missing from the specified product file.
* ``attributes {'type':'not_equal'...}`` - Attributes that are not equal across products.

2. Data Report
These are typically considered **fatal** errors that require further examination, possibly new developments to the pipeline or changes to the native data structures.

* ``size_errors`` - The size of the array is not consistent between products.
* ``dim_errors`` - Arrays have inconsistent dimensions (where not ignored).
* ``dtype/precision`` - Variables/Dimensions have been cast to new dtypes/precisions, most often 32-bit to 64-bit precision.
* ``dim_size_errors`` - The dimensions are consistent for a variable but their sizes are not.
* ``data_errors`` - The data arrays do not match across products, this is the most fatal of all validation errors. The validator should give an idea of which array comparisons failed.
* ``data_errors: {'type':'growbox_exceeded'...}`` - The variable in question could not be validated as no area could be identified that is not empty of values.

BypassSwitch Options
--------------------

Certain non-fatal errors may be bypassed using the Bypass flag:
::

  Format: -b "D"

  Default: "D" # Highlighted by a '*'

  "D" - * Skip driver failures - Pipeline tries different options for NetCDF (default).
      -   Only need to turn this skip off if all drivers fail (KerchunkDriverFatalError).
  "F" -   Skip scanning (fasttrack) and go straight to compute. Required if running compute before scan
          is attempted.
  "L" -   Skip adding links in compute (download links) - this will be required on ingest.
  "S" -   Skip errors when running a subset within a group. Record the error then move onto the next dataset.

Custom Pipeline Errors
----------------------

**A summary of the custom errors that are experienced through running the pipeline.**

.. automodule:: padocc.core.errors
    :members:
    :show-inheritance: