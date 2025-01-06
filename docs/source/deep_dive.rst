===================================
A Deeper Dive into PADOCC Mechanics
===================================

Revision Numbers
----------------

The PADOCC revision numbers for each product are auto-generated using the following rules.

 * All projects begin with the revision number ``1.1``.
 * The first number denotes major updates to the product, for instance where a data source file has been replaced.
 * The second number denotes minor changes like alterations to attributes and metadata.
 * The letters prefixed to the revision numbers identify the file type for the product. For example a zarr store has the letter ``z`` applied, while a Kerchunk (parquet) store has ``kp``.

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
These are considered **fatal** errors that need a major correction or possibly a fix to the pipeline itself.

* ``size_errors`` - The size of the array is not consistent between products.
* ``dim_errors`` - Arrays have inconsistent dimensions (where not ignored).
* ``dim_size_errors`` - The dimensions are consistent for a variable but their sizes are not.
* ``data_errors`` - The data arrays do not match across products, this is the most fatal of all validation errors. 
The validator should give an idea of which array comparisons failed.
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