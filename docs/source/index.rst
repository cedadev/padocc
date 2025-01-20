.. kerchunk*builder documentation master file, created by
   sphinx*quickstart on Thu Jan 25 10:40:18 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PADOCC - User Documentation
============================

**padocc** (Pipeline to Aggregate Data for Optimised Cloud Capabilites) is a Python package for aggregating data to enable methods of access for cloud-based applications.

The pipeline makes it easy to generate data-aggregated access patterns in the form of Reference Files or Cloud Formats across different datasets simultaneously with validation steps to ensure the outputs are correct.

Vast amounts of archival data in a variety of formats can be processed using the package's group mechanics and automatic deployment to a job submission system.

Currently supported input file formats:
 - NetCDF/HDF
 - GeoTiff
 - GRIB
 - MetOffice (**future**)

*padocc* is capable of generating both reference files with Kerchunk (JSON or Parquet) and cloud formats like Zarr. 
Additionally, PADOCC creates CF-compliant aggregation files as part of the standard workflow, which means you get CFA-netCDF files as standard!
You can find out more about Climate Forecast Aggregations `here <https://cedadev.github.io/CFAPyX/>`_, these files are denoted with the extension ``.nca`` and can be opened using xarray with ``engine="CFA"`` if you have the ``CFAPyX`` package installed.

The pipeline consists of three central phases, with an additional phase for ingesting/cataloging the produced Kerchunk files. 
These phases represent operations that can be applied across groups of datasets in parallel, depending on the architecture of your system.
For further information around configuring PADOCC for parallel deployment please contact `daniel.westwood@stfc.ac.uk <daniel.westwood@stfc.ac.uk>`_.

The ingestion/cataloging phase is not currently implemented for public use but may be added in a future update.

.. image:: _images/pipeline.png
   :alt: Stages of the PADOCC workflow

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   Inspiration <inspiration>
   Steps to Run Padocc <phases>
   Getting Started <start>
   Example Operation <cci_water>
   A Deep Dive <deep_dive>

.. toctree::
   :maxdepth: 1
   :caption: Operations:

   The Project Operator <projects>
   The Group Operator <groups>
   SHEPARD <shepard>

.. toctree::
   :maxdepth: 1
   :caption: PADOCC Source:
   
   Projects <project_source>
   Groups <group_source>
   Phases <phase_source>
   Filehandlers, Logs, and Utilities <misc_source>
   
Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


Acknowledgements
================
PADOCC was developed at the Centre for Environmental Data Analysis, supported by the ESA CCI Knowledge Exchange program and contributing to the development of the Earth Observation Data Hub (EODH).

.. image:: _images/ceda.png
   :width: 300
   :alt: CEDA Logo

.. image:: _images/esa.png
   :width: 300
   :alt: ESA Logo
