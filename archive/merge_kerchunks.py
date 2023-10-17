# Merging Kerchunks

import json
import os

#  - Take two existing kerchunk files, one being the primary and a secondary full of replacements
#  - Map all secondary changes into the primary file
#  - Save as a .mg.json to then be renamed later on.

def replace_kerchunk():

    # Open Primary file

    # Open secondary file

    # How to match up files? 
    #  - Match by key of chunkdict

    #  - Determine number of files that came before, with the index of the latest file
    #  - Ensure ordering by sorting filenames by integer value
    return None

def eat_kerchunk(primary, secondary, last, prim_files, sec_files):

    # Open Primary file
    # Open Secondary file

    # Take as input the last file before new patch needs to be added
    # Determine chunk index of that file
    # Determine number of new chunks. (temporal)

    new_chunks = len(sec_files)
    last_index = 0
    for x, p in enumerate(sorted(prim_files)):
        if p == last:
            last_index = x

    newsize = len(prim_files) + new_chunks

    with open(primary) as f:
        refs = json.load(f)

    with open(secondary) as f:
        new_refs = json.load(f)

    for key in refs['refs'].keys():
        if '.z' in key:
            # Handle changing sizes/shapes to correct version
            pass
        else:
            try:
                var, coords = key.split('/')
                time_index = int(coords.split('.')[0])
                if time_index > last_index:
                    # Set the new position
                    new_index = time_index + new_chunks
                    new_pos_key = f'{var}/{new_index}.' + '.'.join(coords.split('.')[1:])
                    refs['refs'][new_pos_key] = refs['refs'][key]

                    # Add the new chunk data to the original spot
                    new_chunk = time_index - last_index - 1
                    rep_pos_key = f'{var}/{new_chunk}.' + '.'.join(coords.split('.')[1:])
                    refs['refs'][key] = new_refs['refs'][rep_pos_key]
            except KeyError as err:
                print(err)
    # Update old chunks correctly with offset
    # Insert new chunks into chunkdict with correct offset
    # Save new chunkdict
    return None