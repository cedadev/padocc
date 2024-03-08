.. kerchunk*builder documentation master file, created by
   sphinx*quickstart on Thu Jan 25 10:40:18 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to the Kerchunk Pipeline documentation!
===============================================

**kerchunk-builder** is a Python package for creating sets of Kerchunk files from an archive of NetCDF/HDF/Tiff files. The pipeline makes it easy to create multiple Kerchunk files for different datasets in parallel with validation steps to ensure the outputs are correct.

The pipeline consists of four central phases, with an additional phase for ingesting/cataloging the produced Kerchunk files. This is not part of the code-base of the pipeline currently but could be added in a future update.

.. image:: _images/pipeline.png
   :alt: Stages of the Kerchunk Pipeline

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   Introduction <pipeline-overview>
   Getting Started <start>
   Example CCI Water Vapour <cci_water>
   Pipeline Flags/Options <execution>
   Assessor Tool Overview <assess-overview>
   Error Codes <errors>

.. toctree::
   :maxdepth: 2
   :caption: Advanced:

   Pipeline Source <pipeline-source>
   Assessor Source <assess>
   Control Script Source <execution-source>



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
