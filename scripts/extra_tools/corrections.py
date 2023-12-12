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

def correct_attrs(proj_code, revision, old_file, new_file, fl, cfg=None, skip=False):
    # Default approach is to compare with xr.open_mfdataset
    # Correct any different metadata and save kerchunk attributes
    direct = False
    # Get Xarray Global Attributes
    if len(fl) == 1:
        skip = True
    if not skip:
        print('Opening Xarray Datasets')
        xattr0 = xr.open_dataset(fl[0]).attrs
        xattr1 = xr.open_dataset(fl[1]).attrs
        skip=True

    # Get Kerchunk Attributes
    with open(old_file) as f:
        refs = json.load(f)
    kattrs = json.loads(refs['refs']['.zattrs'])

    kattrs['time_coverage_end'] = xattr1['time_coverage_end']
    
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
    stamp = now.strftime("%d%m%yT%H%M%S")
    ymd = now.strftime("%d/%m/%y")
    kattrs['history'] = kattrs['history'] + f"\nKerchunk file last updated by CEDA on {ymd} in the context of the CCI Knowledge Exchange Project"
    kattrs['kerchunk_revision'] = revision + 'b'
    kattrs['kerchunk_creation_date'] = str(stamp)
    kattrs['tracking_id'] = str(uuid.uuid4())
    print('Added Kerchunk Attributes')

    # Export new attributes
    refs['refs']['.zattrs'] = json.dumps(kattrs)
    if not os.path.isfile(new_file) or OVERWRITE:
        with open(new_file,'w') as f:
            f.write(json.dumps(refs))
        print('Written to',new_file)
    return None

def find_firstlast(workdir, proj_code, getall=False):
    filelist = f'{workdir}/{proj_code}/allfiles.txt'
    if os.path.isfile(filelist):
        with open(filelist) as f:
            content = [r.replace('\n','') for r in f.readlines()]
        if getall:
            return content
        if content[0] != content[-1]:
            return [content[0], content[-1]]
        else:
            return [content[0]]
    else:
        print('File not found - ',filelist)

#correct_attrs(proj_code, old, revision, textref, old_file, new_file)
config_file = sys.argv[1]

OVERWRITE = ('-f' in sys.argv)

if True:#os.path.isfile(config_file):
    #with open(config_file) as f:
        #cfg_attrs = json.load(f)

## Revisions
# Initially generated       1.0a       - uncorrected version
# Post metadata corrections 1.0b - corrected, untested version
# Post testing version      1.0

    cfg_attrs = {
        'proj_code':'ESACCI-L4_FIRE-BA-MODIS-20010101-20200120-fv5.1',
        'revision':'kr1.2',
        'workdir': '/gws/nopw/j04/esacci_portal/kerchunk/pipeline/in_progress/'
    }

    proj_code    = cfg_attrs['proj_code']
    revision     = cfg_attrs['revision']
    workdir      = cfg_attrs['workdir']

    # Assume no metaref
    fl = find_firstlast(workdir, proj_code)

    old_file = f'{workdir}/{proj_code}/kerchunk-{revision}a.json'
    new_file = f'{workdir}/{proj_code}/kerchunk-{revision}b.json'

    if not os.path.isfile(new_file) or OVERWRITE:
        correct_attrs(proj_code, revision, old_file, new_file, fl, cfg=cfg_attrs, skip=False)
    else:
        print('skipped existing file')
else:
    print(f'Config file not found - {config_file}')