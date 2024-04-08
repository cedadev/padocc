# Summary of Useful features/additions to Padocc
A list of features that would be potentially useful to add to Padocc in the future for various applications

## 1. COG Format Output
Would require the following changes:
 - New processor class for COG files (child of ProjectProcessor)
 - New configuration in compute_config function
 - Method for determining when to use COG (currently can utilise override_type flag)

## 2. Merge/Unmerge Groups
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

# 3. 