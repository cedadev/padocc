Padocc Filehandlers
======================

Filehandlers are an integral component of PADOCC on the filesystem. The filehandlers
connect directly to files within the pipeline directories for different groups and projects
and provide a seamless environment for fetching and saving values to these files.

Filehandlers act like their respective data-types in most or all methods. 
For example the ``JSONFileHandler`` acts like a dictionary, but with extra methods to close and save
the loaded data. Filehandlers can also be easily migrated or removed from the filesystem as part of other
processes.

.. automodule:: padocc.core.filehandlers
    :members:
    :show-inheritance:

=========
Utilities
=========

.. automodule:: padocc.core.utils
    :members:
    :show-inheritance:

=======
Logging
=======

.. automodule:: padocc.core.logs
    :members:
    :show-inheritance: