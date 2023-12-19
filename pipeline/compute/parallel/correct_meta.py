# Correct shapes and chunks
import json


old = 4
new = 304
PATH = '/home/users/dwest77/Documents/kerchunk_dev/parquet/dev/batch/esacci9_full'
with open(f'{PATH}/.zmetadata') as f:
    refs = json.load(f)
    
meta = refs['metadata']

for key in meta.keys():
    if '.zarray' in key:
        # Correct chunks
        if meta[key]['shape'][0] == old:
            meta[key]['shape'][0] = new

refs['metadata'] = meta
with open(f'{PATH}/.zmetadata','w') as f:
    f.write(json.dumps(refs))