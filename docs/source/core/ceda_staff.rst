============================
Extra Details for CEDA Staff
============================

**Last Updated: 4th March 2025**

The following content has been documented to help CEDA staff users specifically, and involves integration of other packages.

Add Products to Catalog
=======================

This section is under review and will be filled once a standardised method for cataloging products has been established. This has been implemented in PADOCC but not standardised, so no specific instructions are available.

CCI: Fill group using Moles ESGF Tag results
============================================

For CCI projects, it can be faster and easier to initialise an empty group and the fill this group via the ``esgf_drs.json`` file created using the ``cci-tag-scanner`` package. The process for doing this is documented here. See the ``cci-tag-scanner`` `repo <https://github.com/cedadev/cci-tag-scanner>`_ for instructions on how to run the moles tagging script.

1. Create an empty group
------------------------

This can be done interactively or using the console.

.. code::

    $ padocc new -G my_group

Or interactively...

.. code:: python

    >>> from padocc import GroupOperation

    >>> # Access your working directory from the external environment - if already defined
    >>> import os
    >>> workdir = os.environ.get("WORKDIR")

    >>> my_group = GroupOperation('my_group',workdir)

2. Add new projects using the ``moles_esgf.json`` contents
----------------------------------------------------------

Either the content can be provided directly or the filepath, but in either case it must be done interactively.

.. code:: python

    >>> my_group.add_project('moles_esgf.json', moles_tags=True)
    INFO [group-operation]: Rejected UNKNOWN_DRS - /neodc/esacci/fire/data/burned_area/Sentinel3_SYN/pixel/v1.1 - not all files are friendly.
    INFO [group-operation]: Rejected esacci.fire.mon.l3s.ba.multi-sensor.multi-platform.syn.v1-1.pixel - not all files are friendly.
    DEBUG [group-operation]: Creating file "main.txt"
    DEBUG [group-operation]: Creating operator for project esacci.fire.mon.l4.ba.multi-sensor.multi-platform.syn.v1-1.grid
    DEBUG [group-operation]: Constructing the config file for esacci.fire.mon.l4.ba.multi-sensor.multi-platform.syn.v1-1.grid
    DEBUG [group-operation]: Creating file "base-cfg.json"
    DEBUG [group-operation]: Skipped setting value in detail-cfg.json
    DEBUG [group-operation]: Creating file "allfiles.txt"
    DEBUG [group-operation]: Skipped setting value in status_log.csv
    DEBUG [group-operation]: Skipped setting value in kj1.1a.json
    DEBUG [group-operation]: No 1.3.2 related file issues.
    DEBUG [group-operation]: Skipped setting value in faultlist.csv
    DEBUG [group-operation]: Skipped setting value in datasets.csv
    >>> my_group[0]
    DEBUG [group-operation]: Creating operator for project esacci.fire.mon.l4.ba.multi-sensor.multi-platform.syn.v1-1.grid
    DEBUG [group-operation]: Creating file "status_log.csv"
    DEBUG [group-operation]: content length: 10
    esacci.fire.mon.l4.ba.multi-sensor.multi-platform.syn.v1-1.grid:
    File count: 10
    Group: my_group
    Phase: init
    Revision: kj1.1

In the above example, the ``UNKNOWN_DRS`` option was ignored since the DRS was not issued (normally meaning non-data files like READMEs), as well as the first DRS, which contained only a set of ``.tar.gz`` files which are not processable by padocc. The third option with the drs ``esacci.fire.mon.l4.ba.multi-sensor.multi-platform.syn.v1-1.grid`` was identified as valid and all subsequent files were created. 

It was then possible to identify this project as the 0th member of this group, with 10 files identified from the input source. In this way, it is possible to add many projects to this group from one moles tags file. And multiple groups can be merged, which adds further options for creating groups.

CCI: Alter dataset attributes using CCI Tagger JSONs
====================================================

In many cases the CCI tagger json files contain expected default values for different datasets. Padocc has now implemented an ``apply_defaults`` method per-project which can be used to reassign values in the cloud dataset.

In contrast to the first CCI-specific case, this section must be performed via the interactive shell, and it requires opening the JSON file and passing the correct content:

.. code:: python

    >>> import json
    >>> with open('fire_syn_v1.1_input.json') as f:
    ...     refs = json.load(f)

    >>> defaults = refs['defaults']
    >>> p = my_group[0]
    >>> p.apply_defaults(defaults)

This will apply the default attributes to the 'dataset' filehandler, which is specified by the ``cloud_format``. If you wish to apply these attributes to a specific product, use the ``target`` kwarg to specify e.g ``kfile``, ``zstore``. This function can also be used to remove specific values, especially if you're using the defaults to correct a naming issue.

.. code:: python

    # Quick example of how you can extract the current value of any property from the main dataset.
    >>> defaults = {'PRODUCT_VERSION':p.dataset_attributes['product_version']}
    >>> p.apply_defaults(defaults, remove = ['product_version'])

This will effectively rename the ``product_version`` parameter to ``PRODUCT_VERSION``. Also, performing the functions using the ``apply_defaults`` method will automatically update the base ``CFA`` dataset alongside the target dataset.