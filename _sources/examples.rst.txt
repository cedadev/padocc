Worked Examples
===============

=================================
2. Example Group of UKCP Datasets 
=================================

Consider a set of 180 UKCP datasets for which we want to create some Kerchunk files.

This set of datasets all have the following properties:
 - sub_collection: land-rcm
 - domain: uk
 - resolution: 12km
 - scenario: rcp85

Which is reflected in the path to the NetCDF files:
`/badc/ukcp18/data/land-rcm/uk/12km/rcp85/`

Below this directory we have directories for __ensemble_member__, __variable_id__, __frequency__ and __version__. For this example we will be using:
 - frequency: day
 - version: v20190731
 - ensemble_member(s) \[x12]: 01, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 15
 - variable_id(s) \[x15]: clt, hurs, huss, pr, prsn, psl, rls, rss, sfcWind, snw, tas, tasmax, tasmin, uas, vas

Hence there will be 12x15 = 180 Kerchunk datasets at the end of this process.

1. Initialising the Pipeline
----------------------------
The easiest way to set up the pipeline for running this group is to create a CSV-type file with the details below for each dataset:
```project_code, pattern, updates*, removals*```

Updates and Removals can be ignored (left blank) unless a specific metadata change is known and required. 
If this is the case, these two can be set as paths to different JSON files which contain the relevant information in the proper format:
::
    { # my_updates.json
        'id': '<new_id_value',
        'history': '<new_history_value>
    }
    { # my_removals.json
        'version_no': True # Value is irrelevant, only matters that this attribute is present.
    }
::

E.g

::
    UKCP18_land-rcm_12km_rcp85_01_clt_day_v20190731, /badc/ukcp18/data/land-rcm/uk/12km/rcp85/01/clt/day/v20190731/*, path/to/updates.json, path/to/removals.json
::

For all 180 datasets in this group. A method of pattern matching and extracting the key information from the path to add to the project code should be used if possible.

To initialise the pipeline with this input CSV file:

```
python group_run.py init UKCP_12km_day -i /path/to/csvfile.csv
```

2. Scan the datasets
--------------------

```python group_run.py scan UKCP_12km_day```

Parameters that may be necessary at this point:
 - time flag: If scan jobs fail for ```slurmstepd``` errors, use the time flag to change the job time allotment: (```-t 30:00``` for 30 mins)