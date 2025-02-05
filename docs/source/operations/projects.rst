Projects in PADOCC
==================

To differentiate syntax of datasets/datafiles with other packages that have varying definitions of those terms,
PADOCC uses the term ``Project`` to refer to a set of files to be aggregated into a single 'Cloud Product'. 

The ``ProjectOperation`` class within PADOCC allows us to access all information about a specific dataset, including
fetching data from files within the pipeline directory. This class also inherits from several Mixin classes which 
act as containers for specific behaviours for easier organisation and future debugging.

The Project Operator class
--------------------------

The 'core' behaviour of all classes is contained in the ``ProjectOperation`` class.
This class has public UI methods like ``info`` and ``help`` that give general information about a project, 
and list some of the other public methods available respectively.

.. code-block:: console

    Project Operator:
    > project.info()                       - Get some information about this project
    > project.get_version()                - Get the version number for the output product
    > project.save_files()                 - Save all open files related to this project
    Dataset Handling:
    > project.dataset                      - Default product Filehandler (pointer) property
    > project.dataset_attributes           - Fetch metadata from the default dataset
    > project.kfile                        - Kerchunk Filehandler property
    > project.kstore                       - Kerchunk (Parquet) Filehandler property
    > project.cfa_dataset                  - CFA Filehandler property
    > project.zstore                       - Zarr Filehandler property
    > project.update_attribute()           - Update an attribute within the metadata
    Status Options:
    > project.get_last_run()               - Get the last performed phase and time it occurred
    > project.get_last_status()            - Get the status of the previous core operation.
    > project.get_log_contents()           - Get the log contents of a previous core operation
    Extra Properties:
    > project.outpath                      - path to the output product (Kerchunk/Zarr)
    > project.outproduct                   - name of the output product (minus extension)
    > project.revision                     - Revision identifier (major + minor version plus type indicator)
    > project.version_no                   - Get major + minor version identifier
    > project.cloud_format[EDITABLE]       - Cloud format (Kerchunk/Zarr) for this project
    > project.file_type[EDITABLE]          - The file type to use (e.g JSON/parq for kerchunk).
    > project.source_format                - Get the driver used by kerchunk
    > project.get_stac_representation()    - Provide a mapper, fills with values from the project to create a STAC record.

Key Functions:
 - Acts as an access point to all information and data about a project (dataset).
 - Can adjust values within key files (abstracted) by setting specific parameters of the project instance and then using ``save_files``.
 - Enables quick stats gathering for use with group statistics calculations.
 - Can run any process on a project from the Project Operator.