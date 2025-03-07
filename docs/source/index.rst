.. kerchunk*builder documentation master file, created by
   sphinx*quickstart on Thu Jan 25 10:40:18 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PADOCC - User Documentation
============================

**padocc** (Pipeline to Aggregate Data for Optimised Cloud Capabilites) is a Python package for aggregating data to enable methods of access for cloud-based applications.

The ``padocc`` tool makes it easy to generate data-aggregated access patterns in the form of Reference Files or Cloud Formats across **many** datasets simultaneously with validation steps to ensure the outputs are correct.

Vast amounts of archival data in a variety of formats can be processed using the package's group mechanics and automatic deployment to a job submission system.

**Latest Release: v1.3.3 07/03/2025**: This release now adds a huge number of additional features to both projects and groups (see the CLI and Interactive sections in this documentation for details). Several alpha-stage features are still untested or not well documented, please report any issues to the `github repo <https://github.com/cedadev/padocc>`_. See the `release notes <https://github.com/cedadev/padocc/releases/tag/v1.3.3>`_ for details on newly added features.

Formats that can be generated
-----------------------------
**padocc** is capable of generating both reference files with Kerchunk (JSON or Parquet) and cloud formats like Zarr. 
Additionally, PADOCC creates CF-compliant aggregation files as part of the standard workflow, which means you get CFA-netCDF files as standard!
You can find out more about Climate Forecast Aggregations `here <https://cedadev.github.io/CFAPyX/>`_, these files are denoted with the extension ``.nca`` and can be opened using xarray with ``engine="CFA"`` if you have the ``CFAPyX`` package installed.

General usage
-------------
The pipeline consists of three central phases, with an additional phase for ingesting/cataloging the produced Kerchunk files. These phases represent operations that can be applied across groups of datasets in parallel, depending on the architecture of your system. The recommended way of running the core phases is to use the `command line tool<core/cli>`.

To check the status of various elements of the pipeline, including the progress of any group/project in your working directory, it is recommended that you make use of padocc through an `interactive<core/interactive>` interface like a Jupyter Notebook or Shell. Simply import the necessary components and start assessing your projects and groups.

For further information around configuring PADOCC for parallel deployment please contact `daniel.westwood@stfc.ac.uk <daniel.westwood@stfc.ac.uk>`_.

The ingestion/cataloging phase is not currently implemented for public use but may be added in a future update.

.. image:: _images/pipeline.png
   :alt: Stages of the PADOCC workflow

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   Inspiration <core/inspiration>
   Installation <core/installation>
   Command Line Tool <core/cli>
   Interactive Notebook/Shell <core/interactive>
   Extra Features <core/extras>
   Extras for CEDA Staff <core/ceda_staff>
   Complex (Parallel) Operation <core/complex_operation>

.. toctree::
   :maxdepth: 1
   :caption: Operations:

   The Project Operator <operations/projects>
   The Group Operator <operations/groups>
   Core Mixins <operations/mixins>
   SHEPARD <operations/shepard>

.. toctree::
   :maxdepth: 1
   :caption: PADOCC API Reference:
   
   Projects <source_code/project_source>
   Groups <source_code/group_source>
   Phases <source_code/phase_source>
   Filehandlers, Logs, and Utilities <source_code/misc_source>
   
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
