Getting Started
===============

.. note::

    Ensure you have local modules enabled such that you have python 3.x installed in your local environment. A version of the pipeline source code exists at ``/gws/nopw/j04/cedaproc/kerchunk_builder`` so please see if this can be used before cloning the repository elsewhere.

Step 0: Git clone the repository
--------------------------------
If you need to clone the repository, either simply clone the main branch of the repository (no branch specified) or check the latest version of the repository at github.com/cedadev/kerchunk-builder, which you can clone using:
::

    git clone git@github.com:cedadev/kerchunk-builder.git --branch v1.0.1

Step 1: Set up Virtual Environment
----------------------------------

Step 1 is to create a virtual environment and install the necessary packages with pip. This can be done inside the local repo you've cloned as ``local`` or ``kvenv`` which will be ignored by the repository, or you can create a venv elsewhere in your home directory i.e ``~/venvs/build_venv``. If you are using the pipeline version in ``cedaproc`` there should already be a virtual environment set up.

.. code-block:: console

    python -m venv name_of_venv;
    source name_of_venv/bin/activate;
    pip install -r requirements.txt;


Step 2: Environment configuration
---------------------------------
Create a config file to set necessary environment variables. (Suggested to place these in the local `config/` folder as this will be ignored by git). Eg:

.. code-block:: console

    export WORKDIR = /path/to/kerchunk-pipeline
    export SRCDIR  = /gws/nopw/j04/cedaproc/kerchunk_builder/kerchunk-builder
    export KVENV   = $SRCDIR/kvenv


Now you should be set up to run the pipeline properly. For any of the pipeline scripts, running ```python <script>.py -h # or --help``` will bring up a list of options to use for that script as well as the required parameters.

Step 3: Assembling pipeline inputs
----------------------------------

In order to successfully run the pipeline you need the following input files:
 - An input csv file with an entry for each dataset with fields:
    - ``project_code, pattern/filename, updates*, removals*``
    - If a pattern is not known or cannot be expressed, the path to a file containing a list of paths to all the NetCDF files can be used instead.
    - updates and removals should be paths to json files which contain information on global metadata replacements. An example can be found below.

It is also helpful to create a setup/config bash script to set all your environment variables which include:
 - WORKDIR: The working directory for the pipeline (where to store all the cache files)
 - SRCDIR: Path to the kerchunk-builder repo where it has been cloned.
 - KVENV: Path to a virtual environment for the pipeline.

Step 4: Commands to run the pipeline
------------------------------------

Some useful option/flags to add:

.. code-block:: python

    -v # Verbose 
       #  - add multiple v's for debug messages
    -f # Forceful 
       #  - perform step even if output file already exists
    -b # Bypass 
       # See bypass section in pipeline flags explained.
    -Q # Quality
       #  - thorough run - use to ignore cache files and perform checks on all netcdf files
    -r # repeat_id
       #  - default uses main, if you have created repeat_ids manually or with assess.py, specify here.
    -d # dryrun
       #  - Skip creating any new files in this phase

The pipeline is run using the ``single_run.py`` or ``group_run.py`` scripts as a command-line interface (CLI) tool.

.. code-block:: python

    # 4.1 Initialise from your CSV file:
    python group_run.py init <group_name> -i path/to/file.csv

    # 4.2 Perform scanning of netcdf files:
    python group_run.py scan <group_name>

.. note::

    You should check after every ``scan``, ``compute`` and ``validate`` that your SLURM jobs are running properly:
    
    ``squeue -u <jasmin_username>``

    And once the SLURM jobs are complete you should check error logs to see which jobs were successful and which failed for different reasons. See Step 5 for details on how to run the **Assessor** [assess.py] script.

.. code-block:: python

    # 4.3 Perform computation (example options: ignore cache and show debug messages):
    python group_run.py compute <group_name> -vQ

    # 4.4 Perform validation (example options: using repeat_id long, set time and memory to specific values, forceful overwrite if outputs already present):
    python group_run.py validate <group_name> -r long -t 120:00 -M 4G -vf

Step 5: Assess pipeline results
-------------------------------

5.1 General progress
--------------------
To see the general status of the pipeline for a given group:
::

    python assess.py <group> progress

An example use case is to write out all datasets that require scanning to a new label (repeat_label):
::
    
    python assess.py <group> progress -p scan -r <label_for_scan_subgroup> -W

The last flag ```-W``` is required when writing an output file from this program, otherwise the program will dryrun and produce no files.

5.2 Check errors
----------------

Check what repeat labels are available already using
::

    python assess.py <group> errors -s labels

Show what jobs have previously run
::

    python assess.py <group> errors -s jobids

For showing all errors from a previous job run
::

    python assess.py <group> errors -j <jobid>

For selecting a specific type of error to investigate (-i) and examine the full log for each example (-E)
::

    python assess.py test errors -j <jobid> -i "type_of_error" -E
