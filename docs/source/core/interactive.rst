=============================================
Running padocc interactively (Notebook/Shell)
=============================================

The fastest way to access various parameters, properties and attributes of projects and groups in the pipeline is to directly manipulate padocc's ``ProjectOperation`` and ``GroupOperation`` classes. These can also be used to execute the core ``phased`` operations of the pipeline (e.g init, scan, compute.) but the command line tool is a little more efficient, since the command line flags account for most options you'd need to pass the classes in the case of interactive operations.

See the source code documentation for any or all of these components described here, which give an explanation of additional arguments that can be used for each method.

Using the GroupOperation class
==============================

This class should let you accomplish any and all extra operations or assessments within the pipeline that should be needed, including getting access to individual projects. To see all the available methods along with a brief description, use ``GroupOperation.help()`` in a python shell.

1. Access a group and view properties
-------------------------------------

.. code:: python

    >>> from padocc import GroupOperation

    >>> # Access your working directory from the external environment - if already defined
    >>> import os
    >>> workdir = os.environ.get("WORKDIR")

    >>> my_group = GroupOperation('my-group',workdir) # Add any extra options (e.g verbose) here.

This creates a group instance within the current working directory. If this group already exists, all existing files that document the group properties are loaded into the group at this point. If not then this group is created with all blank values, and no files are created on the file system until you use ``my_group.save_files()`` or initialise the group in another way.

Now that we have connected to the existing group instance within the pipeline, we can inspect some of the properties of the group, including an info display.

.. code:: python
    
    >>> my_group.info() # Returns basic properties in a dictionary
    {'my_group': {'workdir': '/Users/daniel.westwood/cedadev/padocc/padocc/tests/auto_testdata_dir', 'groupdir': '/Users/daniel.westwood/cedadev/padocc/padocc/tests/auto_testdata_dir/groups/my_group', 'projects': 2, 'logID': None}}

    >>> my_group # Get a YAML-like representation of the properties
    rain-example:
        groupdir: /Users/daniel.westwood/cedadev/padocc/padocc/tests/auto_testdata_dir/groups/rain-example
        logID: null
        projects: 2
        workdir: /Users/daniel.westwood/cedadev/padocc/padocc/tests/auto_testdata_dir

    >>> len(my_group) # Find how many projects are in this group
    2

2. Get project Summaries
------------------------
**see Evaluations Mixin in API reference**

Through the group object, you can obtain a quick summary of the status for each project within the group.

.. code:: python

    >>> my_group.summarise_status()

    Group: rain-example
      Total Codes: 2

    Pipeline Current:

       init      : 2     [100.%] (Variety: 1)
        - Success : 2 (IDs = [0, 1])


    Pipeline Complete:

       complete  : 0     [0.0 %]

From this summary we can see this example group has two projects (under total codes). Both are in the 'init' phase with a 'success' status (variety shows we only have one type of status in this phase). And the ID's show which projects are in this area. There is also a percentage calculation for all active phases to show what proportion of projects sit currently within each phase.

.. note::

    The status functions rely on the most recent run of each project as a determination, so if a project was previously 'complete' but the scan was rerun, it will appear back in the scan section. The thought behind this is that all subsequent steps would need to be rerun if you redo a previous step.

You can imagine if we have 100+ projects in a single group, this function can show exactly where every project is and the situation. Since the recorded status includes custom pipeline errors (See the Extras section), a problem with any project can be swiftly diagnosed. 

We can also use the ``summarise_data`` method after scanning has been completed to get an idea of how much data each project represents.

.. code:: python

    >>> my_group.summarise_data()

    Summary Report: rain-example
    Project Codes: 2

    Source Files: Unknown
    Source Data: Unknown
    Cloud Data: Unknown

    Cloud Formats: ['kerchunk']
    File Types: ['json']

Again we can see this group contains two projects, that are initialised but have not been scanned. The cloud format and file types here are the default values applied to all projects (these can be adjusted per project interactively, or using the ``-C`` flag when running via the command line). After scanning, the Source formats should also be present, which refer to the kerchunk *driver* used in scanning (ncf3, hdf5 or otherwise)

Once the scan has been computed, the source files will represent the total number and average number of files for each project, the same with source and cloud data, which represent the total size in memory of the data from all files in each project.

3. Group subsets/repeats
------------------------

**see Evaluations Mixin in API reference**

It is sometimes useful to run subsections of groups through some phases of the pipeline, for example where some projects in a group require a different cloud format (e.g. Zarr), or where some projects have previously failed a stage in the pipeline. The group operator allows projects to be grouped based on their current status and/or where they are in the pipeline.

.. code:: python

    >>> my_group.repeat_by_status('Success','main_copy')
    # Here we group all projects under 'Success' into a new group called 'main_copy'

    # All subsets in a group are stored as filehandlers under group.proj_codes[repeat_id]
    >>> my_group.proj_codes['main_copy'].get()
    ['padocc-test-1','padocc-test-2']

We can now refer to just these two projects using the 'main-copy' repeat_id, including using the CLI tool with the flag ``-r repeat_id``. This is especially important if a group has many projects with different requirements. The ``repeat_by_status`` method also allows arguments to specify:
 - ``phase`` so we could pick only successful projects in one phase to proceed to the next.
 - ``old_repeat_id`` so we can further subdivide subsets.

The ``remove_by_status`` works very much the same, but will delete projects matching the criteria from the group. This is useful for where a project has been deemed ``unconvertable`` i.e that it is not suitable for any of the cloud formats presented by padocc.

Subsets can also be merged using the ``merge_subsets`` method.

.. code:: python

    >>> my_group.merge_subsets(['main','main_copy'],'main_copy_2')
    # Merge main and main copy into a new subset called main_copy_2

    >>> my_group.proj_codes['main_copy_2'].get()

We can see the list of active subsets using the following

.. code:: python

    >>> my_group.proj_codes
    {'main': <PADOCC List Filehandler: main.txt  >,
    'main_copy': <PADOCC List Filehandler: main_co...>,
    'main_copy_2': <PADOCC List Filehandler: main_co...>}

    # A subset can be removed easily as well
    >>> my_group.remove_subset('main_copy_2')

4. Merging and Unmerging Groups
-------------------------------

.. note::

    Due to issues with the ``transfer-project`` method, this section is not advised for general use yet. If you would like to see this feature added with haste, please contact `daniel.westwood@stfc.ac.uk <daniel.westwood@stfc.ac.uk>`_.

5. Manipulating Projects
------------------------

**see Modifiers Mixin in API reference**

Projects can be extracted and manipulated via the GroupOperation class directly. See the section further down on ProjectOperation methods specifically - this section covers project manipulations at the group-level.

.. note::

    A project can be extracted from a group, simply by indexing the group either numerically or with the intended project code.

    .. code:: python
        
        >>> proj = my_group[0] # CORRECT

    The following is specially noted as this behaviour may have unintended consequences. Manipulating a project should be done using an extracted instance, NOT by referring to the project within the group. This is because the group creates a project instance as a representation of the current filesystem state every time it is indexed. This representation can be adjusted in memory but any adjustments MUST be saved, or they will not persist.

    .. code:: python

        >>> my_group[0].cloud_format = 'zarr' # INCORRECT

    While the above will not generate any errors, the ``cloud_format`` change will not persist, as it is applied to a copy returned by the indexing operation (``my_group[0]``), which is then immediately lost as the project instance is not saved. The project must be extracted (so the ``save_files`` option can be used) in order to make any changes persistent.

    You do not need to re-add the project to the group (i.e ``my_group[0] = proj``) because the indexing operation always creates a fresh copy of the project. The indexing is simply shorthand for the method ``my_group.get_project()``.

    Credit for this 'feature' discovery is given to Dave Poulter at CEDA.

A new project can be added to the group, if the base config information can be supplied.

.. code:: python

    >>> base_config = {
        'proj_code':'new_id', 
        'pattern':'directory/*.nc',
        'updates':'path/to/updates.json',
        'removals':'path/to/removals.json'
    }
    >>> my_group.add_project(base_config)

The ``pattern`` attribute can be replaced with a path to a file containing a list of the source files. ``updates`` can be used where attributes in the final datasets should be changed to a different value (this can also be accomplished later using the ProjectOperation). The same is true for the ``removals`` in the case for attributes that should be omitted. 

``substitutions`` can also be provided here if for example the filelist being given contains a set of files that have since moved (i.e their absolute paths have changed). Since this change applies to the datasets in the filelist, the above example would then become:

.. code:: python

    >>> base_config = {
        'proj_code':'new_id', 
        'pattern':'directory/*.nc',
        'updates':'path/to/updates.json',
        'removals':'path/to/removals.json'
        'substitutions':{
            'datasets':{
                'old_dir':'new_dir'
            }
        }
    }
    >>> my_group.add_project(base_config)

You can easily check this project has been added by inspecting the group in a few ways. Adding a project creates an inaccessible ProjectOperation instance within the group, which is then immediately saved, along with the adjustments to group files (i.e the ``proj_codes['main']`` list).

.. code:: python

    >>> my_group
    rain-example:
      groupdir: /Users/daniel.westwood/cedadev/padocc/padocc/tests/auto_testdata_dir/groups/rain-example
      logID: null
      projects: 3 # Now increased to three
      workdir: /Users/daniel.westwood/cedadev/padocc/padocc/tests/auto_testdata_dir

    >>> my_group.proj_codes['main'].get()
    ['padocc-test-1', 'padocc-test-2','new_id']

Deleting a project can be done in the same way and is also automatically saved (if not in dryrun mode) with the requested project code removed from all relevant files. Since this option is only available interactively, you will then be prompted to confirm deletion of the project, to double check before you delete the wrong one!

If you want to skip the double check, add ``ask=False`` to the ``remove_project`` operation.

A project can also be transferred between two group instances using the following.

.. code:: python
    
    >>> # Using another already-initialised group called group2
    >>> my_group.transfer_project('padocc-test-3',my_group2)

.. note::

    Developer note (05/02/25): The transfer project mechanism is currently in alpha deployment, and is known to exhibit inconsistent behaviour when trying to transfer a project to a new uninitialised group. This is an ongoing issue.

Using the ProjectOperation class
================================

As stated above, projects can be extracted and manipulated via the GroupOperation class directly. See the note from the section above about why project extraction is crucial and why properties of a project should not be adjusted by referencing the group class.

1. General access to a project
------------------------------

The easiest way to access a specific project within a group is by extracting it from the group itself, as all necessary configurations will be passed on from the group.

.. code:: python

    >>> proj = my_group[0] # Could also index by 'proj_code'

Alternatively, a project can be instantiated outside the group class, if the configurations are given accordingly.

.. code:: python

    >>> from padocc import ProjectOperation
    >>> proj = ProjectOperation('padocc-test-1',workdir, groupID='my_group')

Note here that if you have added any extra flags to the group (verbose, dryrun etc.), these will also need to be added to the project if you have initialised it this way. If you pull it from the group, the configurations will be transferred without further input.

The project class has similar UI features to the group class. There is a ``help`` class method (called from an instance OR by ``ProjectOperation.help()``), as well as an ``info`` section.

.. code:: python

    >>> proj.info()
    {'padocc-test-1': {'Group': 'rain-example',
    'Phase': 'init',
    'File count': 5,
    'Revision': 'kj1.1'}}
    >>> proj
    padocc-test-1:
      File count: 5
      Group: rain-example
      Phase: init
      Revision: kj1.1

See the Extra Details section on how the revisions system works. By default all revisions start as 1.1 with a prefix that denotes the default cloud format 'kerchunk' and file type 'json'.

2. Editable properties
----------------------

Specific properties of any project are 'editable', meaning they can be altered through the use of padocc tools, rather than by editing the files themselves (this should be avoided as some data would become out-of-sync, not to mention the difficulty with editing individual files in a large directory structure).

.. code:: python

    >>> proj.cloud_format = 'zarr'
    # This will automatically adjust the 'file_type' parameter to None where it is normally 'json' as default.

    # This change will be persistent only for this instance, unless the project files are saved.
    >>> proj.save_files()

    # After saving files, any new project instances extracted from the group will read from the filesystem including those new changes.

    >>> proj.minor_version_increment()
    # After any attribute changes to the dataset, the minor version should be updated

    >>> proj.version_no
    '1.2'

.. note::
    
    Developer's Note (05/02/25): The Major version increment is reserved for changes to the data in a dataset (i.e when a source file is replaced), however this is not currently implemented as this feature is still in active development. If source files change during the operation of the pipeline, you will need to restart the processing of the project, making previous files obsolete. (You may also need to add the ``-f`` or ``forceful=True`` option to overwrite old content).

3. Datasets attached to the project
-----------------------------------

**see Datasets Mixin in API reference**

As part of the project, several filehandler objects exist for accessing cloud products that have been created. Each project has a ``kfile`` , ``kstore`` and ``zstore`` filehandler attached that can be accessed as a property. These filehandlers have numerous helpful methods that can be used to inspect the results of the pipeline (see the Filehandlers API reference), but here we focus on the ability to open the dataset.

Additionally, the ``dataset`` property of each project points at whichever of the above three options aligns with the current selection of ``cloud_format`` and ``file_type``. There is also a ``cfa_dataset`` property dataset (see the details of CFA in the Inspiration section)

All these filehandlers can be used to open the product as an xarray dataset for analysis/testing purposes.

.. code:: python

    >>> proj.dataset.open_dataset()
    [xarray dataset representation]

Additional configurations - if required - can be passed to xarray as kwargs to the ``open_dataset`` method above for any of the filehandlers. Be aware that if you try to access a product that does not exist, there will likely be a ``FileNotFoundError`` within the pipeline.

The dataset mixin also adds an ``update_attribute`` method which allows direct manipulation of the attributes for any of the above filehandlers if needed. This is supplemental to the ``updates`` and ``removals`` options provided on initialisation.

.. code:: python

    >>> proj.update_attribute('target_id', 0, target='kfile')
    >>> proj.kfile.close()
    # The standard project 'save_files' does not cover the dataset filehandlers

Here we are editing the ``target_id`` attribute of the kerchunk dataset. Removals are currently not supported, but will almost certainly be included in a future release.

4. Status of the project
------------------------

**See Status Mixin in API reference**

Each project includes a few useful functions (also used by methods in the group object) to assess the current status.

.. code:: python

    >>> proj.get_last_run()
    [None,None] # Our project has not been run through a phase yet

    >>> proj.get_last_status()
    'init,Success,15:04 02/05/25,' # Project was initialised with success at the given timestamp.

For any previous phase, the logs from that run can be extracted using ``get_`` or ``show_log_contents`` for a specific phase, where ``get`` returns a string representation of the logs, and ``show`` displays them to the screen, along with some extra details and a 'rerun' command. 

.. code:: python

    >>> proj.get_log_contents('scan')
    [will give all logs recorded in this phase]

5. STAC Representation of a project
-----------------------------------

Finally, to extract the metadata around a project as a STAC record, a mapper can be provided that maps pipeline-based properties to values in the resulting STAC record, which can then be given to the ``get_stac_representation`` method.

.. code:: python

    >>> stac_mapper = {
        "cloud_format": ("property@cloud_format",None),
        "source_fmt": ("detail_cfg@source_format",None)
    }
    >>> stac_record = proj.get_stac_representation(stac_mapper)

In the above basic example, we extract two attributes from the pipeline, one being the cloud format property of the project (which is referenced with the above syntax) and the other being a value from the detail_cfg file. The tuple input is required, as a default option must be given in the case that a property cannot be retrieved.

The stac mapper can be applied to all members of a group using ``group.get_stac_representation`` which will return a combined dictionary of all the stac records from all the projects.
