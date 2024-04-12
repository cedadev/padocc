.. kerchunk*builder documentation master file, created by
   sphinx*quickstart on Thu Jan 25 10:40:18 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Padocc - User Documentation
============================

**padocc** (Pipeline to Aggregate Data for Optimised Cloud Capabilites) is a Python package (package name **kerchunk-builder**) for aggregating data to enable methods of access for cloud-based applications.

The pipeline makes it easy to generate data-aggregated access patterns in the form of Reference Files or Cloud Formats across different datasets simultaneously with validation steps to ensure the outputs are correct.

Vast amounts of archival data in a variety of formats can be processed using the package's group mechanics and automatic deployment to a job submission system.

Currently supported input file formats:
 - NetCDF/HDF
 - GeoTiff (**coming soon**)
 - GRIB (**coming soon**)
 - MetOffice (**future**)

*padocc* is capable of generating both reference files with Kerchunk (JSON or Parquet) and cloud formats like Zarr.

The pipeline consists of four central phases, with an additional phase for ingesting/cataloging the produced Kerchunk files. This is not part of the code-base of the pipeline currently but could be added in a future update.

.. image:: _images/pipeline.png
   :alt: Stages of the Kerchunk Pipeline

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   Introduction <pipeline-overview>
   Getting Started <start>
   Example CCI Water Vapour <cci_water>
   Padocc Flags/Options <execution>
   Assessor Tool Overview <assess-overview>
   Error Codes <errors>
   Developer's Guide <dev-guide>

.. toctree::
   :maxdepth: 2
   :caption: CLI Tool Source:

   Assessor Source <assess>
   Control Scripts Source <execution-source>

.. toctree::
   :maxdepth: 2
   :caption: Pipeline Source:
   
   Initialisation <init>
   Scanning <scan>
   Compute <compute>
   Validate <validate>
   Allocations <allocation>
   Utils <extras>



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
