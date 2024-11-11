===================
User guide (module)
===================

As of ``padocc 1.3``, the recommended way to access projects/groups and perform analyses of specific features is to directly import padocc components following this guide.
There is also a guide for using the Command-Line Interface (padocc_cli) which you can find linked from the documentation homepage.

Manipulating a Group
--------------------

Any operation to be applied to a specific group can be accessed via the ``GroupOperation`` class, imported as below from padocc's ``operations`` module.

.. code-block:: python

    from padocc.operations import GroupOperation

    my_group = GroupOperation(
        'my_group',
        workdir='workdir', # The directory to create pipeline files.
        verbose=1 # INFO.
    )

Alternatively to using an integer value for the logs, you can specify a specific logging level provided it is one of ``logging.INFO``, ``logging.WARN`` or ``logging.DEBUG``.

Some examples of basic features of the group can be found below:

.. include:: group_examples.ipynb
   :parser: myst_nb.docutils_

    