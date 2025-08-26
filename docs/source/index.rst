.. kerchunk*builder documentation master file, created by
   sphinx*quickstart on Thu Jan 25 10:40:18 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PADOCC - User Documentation
============================

**padocc** (Pipeline to Aggregate Data for Optimised Cloud Capabilites) is a Python package for aggregating data to enable methods of access for cloud-based applications.

The ``padocc`` tool makes it easy to generate data-aggregated access patterns in the form of Reference Files or Cloud Formats across **many** datasets simultaneously with validation steps to ensure the outputs are correct.

Vast amounts of archival data in a variety of formats can be processed using the package's group mechanics and automatic deployment to a job submission system.

**Latest Release: v1.4.0 <date>**: Version 1.4 has several updates to dependencies for PADOCC, as well as new features for virtualization using VirtualiZarr, added Validation checks and the first release of the __SHEPARD__ module. (See the `release notes <https://github.com/cedadev/padocc/releases/tag/v1.4.0>`_ for details). 

Formats that can be generated
-----------------------------
**padocc** is capable of generating both reference files with Kerchunk (JSON or Parquet) and cloud formats like Zarr. 
Additionally, PADOCC creates CF-compliant aggregation files as part of the standard workflow, which means you get CFA-netCDF files as standard!
You can find out more about Climate Forecast Aggregations `here <https://cedadev.github.io/CFAPyX/>`_, these files are denoted with the extension ``.nca`` and can be opened using xarray with ``engine="CFA"`` if you have the ``CFAPyX`` package installed.

General usage
-------------
The pipeline consists of three central phases, plus many different operations that can be applied to different datasets depending on use cases. These phases represent operations that can be applied across groups of datasets in parallel, depending on the architecture of your system. The recommended way of running the core phases is to use the `command line tool<core/cli>`. For a list of operations that go beyond the core phases, see the section entitled All Operations.

To check the status of various elements of the pipeline, including the progress of any group/project in your working directory, it is recommended that you make use of padocc through an `interactive<core/interactive>` interface like a Jupyter Notebook or Shell. Simply import the necessary components and start assessing your projects and groups.

For further information around configuring PADOCC for parallel deployment please contact `daniel.westwood@stfc.ac.uk <daniel.westwood@stfc.ac.uk>`_.

The ingestion/cataloging phase is not currently implemented for public use but may be added in a future update.

.. image:: _images/pipeline.png
   :alt: Stages of the PADOCC workflow

.. toctree::
   :maxdepth: 1
   :caption: Getting Started:

   Inspiration <core/inspiration>
   PADOCC Terms and Operators <core/terminology>
   Installing PADOCC <core/installation>
   Basic User Guide <core/user_guide>
   Command Line Tool Examples<core/cli>
   Extras for CEDA Staff <core/ceda_staff>

.. toctree::
   :maxdepth: 1
   :caption: Detailed View:

   All Operations <detailed/all_operations>
   Bespoke Features <detailed/features>
   Parallel Deployment <detailed/parallel>
   SHEPARD Deployment <detailed/shepard>

.. toctree::
   :maxdepth: 1
   :caption: PADOCC API Reference:
   
   Projects <source_code/project_source>
   Groups <source_code/group_source>
   SHEPARD <source_code/shepard_source>
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
