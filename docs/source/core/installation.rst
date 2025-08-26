===============
Getting Started
===============

.. note::

    Ensure you have local modules enabled such that you have python 3.11 or higher installed in your local environment.

pip installation
================

As of version 1.3, padocc is now a package on pypi! This means you can simply install with ``pip install padocc``. If you need to install from the source (e.g to access test data) please see below.

Install from source
===================

Step 1: Git clone the repository
--------------------------------

If you need to clone the repository, either simply clone the main branch of the repository (no branch specified) or check the latest version of the repository at github.com/cedadev/padocc, which you can clone using:
::

    git clone git@github.com:cedadev/padocc.git

.. note::

    The instructions below are specific to version 1.3 and later. To obtain documentation for pre-1.3, please contact `daniel.westwood@stfc.ac.uk <daniel.westwood@stfc.ac.uk>`_.

Step 2: Set up Virtual Environment
----------------------------------

Step 2 is to create a virtual environment and install the necessary packages with pip. This can be done inside the local repo you've cloned as ``local`` or ``kvenv`` which will be ignored by the repository, or you can create a venv elsewhere in your home directory i.e ``~/venvs/build_venv``. If you are using the pipeline version in ``cedaproc`` there should already be a virtual environment set up.

.. code-block:: console

    python -m venv name_of_venv;
    source name_of_venv/bin/activate;
    pip install ./;


Environment configuration
=========================

Create a config file to set necessary environment variables. (Suggested to place these in the local `config/` folder as this will be ignored by git). Eg:

.. code-block:: console

    export WORKDIR=/path/to/kerchunk-pipeline

For parallel deployment with Lotus (Lotus 2 on JASMIN), an additional ``LOTUS_CFG`` environment variable is needed. See the section on parallel deployment for details.

Assembling Pipeline Inputs
==========================

In order to successfully run the pipeline you need the following input files:
 - An input csv file with an entry for each dataset with fields:
    - ``project_code, pattern/filename, updates*, removals*``
    - If a pattern is not known or cannot be expressed, the path to a file containing a list of paths to all the NetCDF files can be used instead.
    - updates and removals should be paths to json files which contain information on global metadata replacements. An example can be found below.

It is also helpful to create a setup/config bash script to set all your environment variables which include:
 - WORKDIR: The working directory for the pipeline (where to store all the cache files)