# Summary of Useful features/additions to Padocc
A list of features that would be potentially useful to add to Padocc in the future for various applications

## 0. Indexing of Kerchunk files (post-validation)
An elasticsearch index to hold records/data of all Kerchunk files produced in the pipeline (beyond EODH STAC),
for all projects i.e CCI, EODH etc.
See `extensions/templates/index-template.md` for the ideal structure of the index records. `index_cat.py` script
for adding functionality for pushing to this index as a part of the pipeline. Will need an ID generator function 
(see cedadev/flight-pipeline) for an example.

## 1. Validation Improvements - Priority: High
Running locally seems to always have better success on validation than when using SLURM. This 
is most likely due to issues when receiving the NetCDF chunks from dap service from within a
node in the batch cluster. This is a place for possible solutions to be considered.

### 1.0 Ingestion Required - Priority: High
Old ingestion method can be found at ...

New ingestion method requires ingest.py to be created, which automatically will add download links where required.
Needs configuration of ingester and a few extra handlers for edge cases, plus logging integration.

### 1.1 Serialised Validation (Low Priority)
Instead of opening the Kerchunk and Xarray datasets simultaneously, open the Xarray dataset first
from the original (local) files. Perform tests on all the variables as is currently done and 
RECORD THE RESULTS. This can be of the form:
{
  'filenum': number,
  'dimstamp': object?
  'variable':{
    'shape': [shape],
    'min-box-size': [shape],
    'mean-box': value,
    'min-box': value,
    'max-box': value
  }
}

Once these values are recorded, the Xarray dataset is no longer required. The Kerchunk dataset can 
then be opened. A copy should be made when creating selections within a data testing function, so that
we still have the original xarray dataset to retry fetching the data.

### 1.2 Remote connection at latest stage. (High Priority)
Move the add_dap_links to the 'ingest' phase of the pipeline (or final validation step run_successful)
So the validation step is all done locally. The reason being that the network issues produce significant
issues when trying to validate kerchunk files. Typically it's the mean calculation that fails for ValidationErrors.

Requires:
 - Remove function add_download_links from compute processor (or bypass)
 - May need to look at open_kerchunk function remote_protocol default in validate, ensure this will work for either
 remote or local links. (Already has something )
 - Add in add_download_links before run_successful.

## 2. COG Format Output (For Google Earth Engine [GEE]) Priority: Medium
Would require the following changes:
 - New processor class for COG files (child of ProjectProcessor): rioxarray can convert NetCDF to GeoTiff, GDAl looks like
 the tool to use for converting to COG but a python library may not exist to do that part of the conversion?
 https://corteva.github.io/rioxarray/stable/
 - New configuration in compute_config function
 - Method for determining when to use COG (currently can utilise override_type flag)

## 3. Merge/Unmerge Groups - Priority: Low
For sets of groups under a single working directory, it would be useful to be able to
merge/unmerge groups, i.e redistribute project codes under each group to new arrangements.

For merging:
 - Use assess.py to take a list of groups and a new groupname to merge everything to.
 - Merge all group files together.
 - Copy all project directories to the correct new places in the new group.

For unmerging:
 - Read in a json file that has the names of multiple groups and paths to txt files for each 
   group.
 - Read in each text file which contains a list of all the project codes to move.
 - Move all individual project files to new group directories under in_progress.
 - Create new group files from the input text files and old group media.