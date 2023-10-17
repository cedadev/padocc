import os
import xarray as xr
import json
from datetime import datetime
import sys
import uuid

OVERWRITE=True

def correct_shape(old, new, storepath):
    old = 5
    new = 10
    PATH = '/home/users/dwest77/Documents/kerchunk_dev/parquet/dev/batch/gargant'
    with open(f'{storepath}/.zmetadata') as f:
        refs = json.load(f)
        
    meta = refs['metadata']

    for key in meta.keys():
        if '.zarray' in key:
            # Correct chunks
            if meta[key]['shape'][0] == old:
                meta[key]['shape'][0] = new

    refs['metadata'] = meta
    with open(f'{storepath}/.zmetadata','w') as f:
        f.write(json.dumps(refs))

def direct_comparison(xattrs, kattrs):
    corrections = []
    for key in xattrs.keys():
        if key not in kattrs.keys():
            kattrs[key] = str(xattrs[key])
            corrections.append(key)
        else:
            if kattrs[key] != xattrs[key]:
                kattrs[key] = str(xattrs[key])
                corrections.append(key)
    return xattrs, kattrs, corrections

def specific_comparison(xattrs, kattrs, cfg):
    extras = {}
    if cfg:
        print('Found cfg file for extra attribute config')
        with open(cfg) as f:
            for r in f.readlines():
                values = r.replace('\n','').split(',')
                extras[values[0]] = values[1]

    missing = []
    corrections = []
    for key in xattrs.keys():
        if key not in kattrs.keys():
            kattrs[key] = str(xattrs[key])
            missing.append(key)
        else:
            if kattrs[key] != xattrs[key]:
                if 'end' in key:
                    kattrs[key] = str(xattrs[key])
                    corrections.append(key)
                elif 'start' in key:
                    pass
                elif key in extras.keys():
                    if extras[key] == 'replace':
                        kattrs[key] = str(xattrs[key])
                else:
                    missing.append(key)
            else:
                pass
    if len(missing) > 0:
        print('WARNING: Missing additional fields - check manually')
        print(' >> ' + ','.join(missing))
        return 0, 0, 0, None
    else:
        return xattrs, kattrs, corrections, True

def correct_attrs(proj_code, old, revision, textref, old_revision, new_revision, cfg=None, skip=False):
    # Default approach is to compare with xr.open_mfdataset
    # Correct any different metadata and save kerchunk attributes
    direct = False
    # Get Xarray Global Attributes
    if not skip:
        with open(textref) as f:
            xopens = [r.replace('\n','') for r in f.readlines()]
        if len(xopens) >= 2:
            xopens = [xopens[0], xopens[-1]]
            if xopens[0] == xopens[1]:
                skip = True
        else:
            skip = True
    if not skip:
        try:
            xattrs = xr.open_mfdataset(xopens).attrs
            direct = True
        except ValueError:
            xattrs = xr.open_dataset(xopens[-1]).attrs

    # Get Kerchunk Attributes
    with open(old_revision) as f:
        refs = json.load(f)
    kattrs = json.loads(refs['refs']['.zattrs'])

    if not skip:
        # Set all attributes if they are incorrect
        if direct:
            print('Using direct comparison')
            xattrs, kattrs, corrections = direct_comparison(xattrs, kattrs)
            success = True
        else:
            print('Using specific comparison')
            xattrs, kattrs, corrections, success = specific_comparison(xattrs, kattrs, cfg) 

        if not success:
            return None
        
        print('Corrected: ',end='')
        if not corrections:
            print(None)
        else:
            print(', '.join(corrections))

    # Set kerchunk specific attributes
    now = datetime.now()
    stamp = now.strftime("%y%m%dT%H%M%SZ")
    ymd = now.strftime("%d/%m/%y")
    if 'Kerchunk' in kattrs['history'].split('\n')[-1]:
        pass
    else:
        kattrs['history'] = kattrs['history'] + f"\nKerchunk file last updated by CEDA on {ymd} in the context of the CCI Knowledge Exchange Project"
    kattrs['kerchunk_revision'] = revision
    kattrs['kerchunk_creation_date'] = str(stamp)
    kattrs['tracking_id'] = str(uuid.uuid4())

    # Export new attributes
    refs['refs']['.zattrs'] = json.dumps(kattrs)
    if not os.path.isfile(new_revision) or OVERWRITE:
        with open(new_revision,'w') as f:
            f.write(json.dumps(refs))
        print('Written to',new_revision)
    return None

#correct_attrs(proj_code, old, revision, textref, old_revision, new_revision)
proj_code    = sys.argv[1]
old          = sys.argv[2]
revision     = sys.argv[3]
workdir      = sys.argv[4]

try:
    cfg = sys.argv[5]
except:
    cfg = None

textref      = f'{workdir}/metaref_filelists/{proj_code}.txt'
old_revision = f'{workdir}/kfiles_1.1/{proj_code}.json'
new_revision = f'{workdir}/kfiles_1.1/{proj_code.replace(old, revision)}.json'
if '.nc' in new_revision:
    new_revision = new_revision.replace('.nc','')
if not os.path.isfile(new_revision) or OVERWRITE:
    correct_attrs(proj_code, old, revision, textref, old_revision, new_revision,cfg=cfg, skip=True)
else:
    print('skipped existing file')