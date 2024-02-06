Getting Started
===============

Note: Ensure you have local modules enabled such that you have python 3.x installed in your local environment.

Step 0: Git clone the repository
--------------------------------
The Kerchunk builder will soon be updated to version 1.0.1, which you can clone using:

::
    git clone git@github.com:cedadev/kerchunk-builder.git --branch v1.0.1
::
Step 1: Set up Virtual Environment
----------------------------------

Step 1 is to create a virtual environment and install the necessary packages with pip. This can be done inside the local repo you've cloned as ```local``` or ```build_venv``` which will be ignored by the repository, or you can create a venv elsewhere in your home directory i.e ```~/venvs/build_venv```

::
    python -m venv name_of_venv;
    source name_of_venv/bin/activate;
    pip install -r requirements.txt;
::

Step 2: Environment configuration
---------------------------------
Create a config file to set necessary environment variables. (Suggested to place these in a local `templates/` folder as this will be ignored by git). Eg:

::
    export WORKDIR=/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline;
    export GROUPDIR=/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline/groups/CMIP6_rel1_6233;
    export SRCDIR=/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder;
    export KVENV=/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/build_venv;
::

Now you should be set up to run the pipeline properly. For any of the pipeline scripts, running ```python <script>.py -h # or --help``` will bring up a list of options to use for that script as well as the required parameters.