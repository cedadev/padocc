Developer Guide - Adding new functionality
==========================================

Several utility functions ``(pipeline.utils)`` have been configured to make adding new features
as easy as possible. Some importable functions of key importance:
 - get_proj_dir: Gets the project directory string from command line parameters
 - get/set_proj_file: Open any json formatted project file given a project directory and get/set data.
 - format_str: Pretty formatting, sets the string size to a given ``length`` by concatenating or adding whitespace.
 - get/set_codes: Get a list of project codes from any file or sub-directory file in a groupdir.
 - open_kerchunk: Open a kerchunk file with __protection__ from errors and taking into account different types or conditions.

Also a quick note on some ``pipeline.logs`` features.
 - FalseLogger(): Test class for when you don't care about proper logging (not to be used in production).

1. New Modes in Assessor
========================

New modes can be added to the assessor with the following method:
 - Create a new function that takes ``args`` and ``logger`` as inputs.
 - Add this function to the ``operations`` dict, with an appropriate activation string.
 - Add any new command line inputs to ArgParse (try to reuse or expand existing flags where possible)

2. New Processors for Compute
=============================

See ``pipeline.compute.compute_config`` to configure a new type of processor. You will need:
 - A configuration function to ensure all required parameters are set.
 - A processor class (that most likely inherits from ``pipeline.compute.ProjectProcessor``)
 - A method of activating that processor (``override_type`` option is always available, but could this processor be triggered automatically based on a scan result?) 

