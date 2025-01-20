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

Key Functions:
 - Acts as an access point to all information and data about a project (dataset).
 - Can adjust values within key files (abstracted) by setting specific parameters of the project instance and then using ``save_files``.
 - Enables quick stats gathering for use with group statistics calculations.
 - Can run any process on a project from the Project Operator.