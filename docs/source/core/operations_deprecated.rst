=======================
All Non-core Operations
=======================

Here we provide a list of all operations performed via the command line interface that are not considered ``core-phased`` operations (i.e are not one of ``scan``, ``compute`` or ``validate``). This section is continuously expanding with new operations and features being added. To request a feature, make an issue on the `github repo <https://github.com/cedadev/padocc>`_ and add the 'feature' type (can also set a 'label' for priority - ideally all feature requests should be 'desired' only).

.. Note:: 

    Most operations can be filtered to apply to only a subset of the group, most commonly using the repeat_id ``-r`` flag, where you must have already created the repeat subset using one of the interactive methods e.g ``group.repeat_by_status()``.

Completion workflow
===================

At the point of completing the main pipeline phases, a project or set of projects may be 'completed'. This involves moving all product files/stores out of the pipeline into a specified directory, and applying any relevant filename changes or configurations. This can now be combined with the ``-T`` flag to enable download-link replacement at the point of completing the files.

.. Note::
    
    (Download-link replacement applies to Kerchunk and Kerchunk-Parquet to change all the paths to use dap.ceda.ac.uk, so the kerchunk references work remotely). It is now convention that remotely-accessible kerchunk files have the ``r`` revision number applied after download-link replacement.

.. code:: python

    $ padocc complete -G <group_name> -T --shortcut <completion_directory>
    # Will move all projects under this group into the specified directory AND apply download links to all relevant files.

List groups
===========

A simple operation to list available groups in your workspace. This, like all other operations on this page, requires the ``$WORKDIR`` environment variable to be set, or alternatively the use of the ``-w`` flag to define where the pipeline files are located.

.. code:: python

    $ padocc list

Delete Projects/Group
=====================

After all projects in a group have been removed, this operation allows all associated files to be removed from the pipeline, in both the ``in_progress`` project-space and ``groups`` group-space. Individual projects may also be removed using this operation.

.. code:: python

    $ padocc delete -G <group> -p <project_code>
    # To remove a specific project (will be prompted to confirm deletion)

    $ padocc delete -G <group>
    # This will delete ALL files for this group (will be prompted only for the group as a whole.)

Get Logs
========

Fetch a specific log for a specific project that has run previously, or simply fetch logs for all projects in a group, specifying the phase with logs you're trying to fetch.

.. code:: python

    $ padocc get_log -G <group> -p <project_code> --shortcut <phase>
    # Obtain the log file for the 'phase' specified by the shortcut.

    $ padocc get_log -G <group> --shortcut <phase>
    # Gets every log file for this 'phase' across all projects, will present one at a time.

Add Projects
============

Projects can be added using either a CSV or JSON file, consistent with other methods for adding new data.

.. code:: python

    $ padocc add -G <group> -i <input_file> --shortcut moles
    # Use for adding content consistent with the 'moles_esgf_tag' option (CEDA Staff only - see CEDA Staff section).

    $ padocc add -G <group> -i <input_file.json>
    # Only for files matching the 'base_cfg' style of input, which describes a single project.

Group Status
============

Projects report their current status using a ``status_log`` file in the pipeline. This file is updated after every completed operation or phase, and is updated as ``Pending`` upon starting a parallelised phase. The group contains a method to collect all statuses from all projects and display this to the user. The command line operation is as follows:

.. code:: python

    $ padocc status -G <group>
    # Will showcase all statuses of all projects

Currently no filters can be applied from the command-line, but this will be updated in future versions. All filters can be applied using the ``group.summarise_status()`` method interactively.

Group Summary
=============

Data statistics from each project can be extracted and combined using the ``summarise`` operation. An example output is shown below.

.. code:: python

    $ padocc summarise -G <group>

    Summary Report: example_group_1
    Project Codes: 3

    Source Files: 44831 [Avg. 14943.67 per project]
    Source Data: 16.08 GB [Avg. 5.36 GB per project]
    Cloud Data: 97.32 MB [Avg. 32.44 MB per project]

    Cloud Formats: ['kerchunk']
    Source Formats: ['hdf5']
    File Types: ['json']

    Chunks per File: 39.00 [Avg. 13.00 per project]
    Total Chunks: 582803.00 [Avg. 194267.67 per project]

Currently no filters can be applied from the command-line, but this will be updated in future versions. All filters can be applied using the ``group.summarise_data()`` method interactively.

Check Attributes
================

An attribute can be checked across all projects within a group using this operation. The value of the attribute will be shown for each project. This applies to attributes in either the ``base_cfg`` or ``detail_cfg`` files which serve as the persistent storage for project attributes.

.. code:: python

    $ padocc check_attr -G <group> --shortcut remote

    > project_1: True
    > project_2: False
    > project_3: None

    # Will return None for any project where that parameter cannot be found.


Set Attributes
==============

This operation applies only to memory-loaded attributes of the project, not directly to the ``base_cfg`` or ``detail_cfg`` files which act as persistent storage for project attributes. Below is a demonstration of how this setter can be used.

.. code:: python

    $ padocc set_attr -G <group> --shortcut remote:True
    # Will set all 'remote' properties in all projects.

Report
======

PADOCC now supports obtaining reports from the validator using the command line. This can be used to obtain a specific report for a specific project, or combine all reports into a single document, which has the advantage of combining errors that apply to all projects that can be easily dismissed. Currently there is no mechanism to determine which errors apply to which projects. Please create an issue on the `github repo <https://github.com/cedadev/padocc>`_ if this is something you would like to see.

.. code:: python

    $ padocc report -G <group> -p <project>
    # Display the report for a single project

    $ padocc report -G <group>
    # Combine all reports from this group into a single report - useful for quickly identifying if all projects are passable.