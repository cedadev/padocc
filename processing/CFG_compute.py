# Main script for processing, runs all other parts as needed including submitting batch jobs for large parquet sets.
import sys
import os
import json

from serial.CFG_create_kerchunk import Indexer

def rundecode(cfgs):
    """
    cfgs - list of command inputs depending on user input to this program
    """
    flags = {
        '-w':'workdir'
    }
    kwargs = {}
    for x in range(0,int(len(cfgs)),2):
        try:
            flag = flags[cfgs[x]]
            kwargs[flag] = cfgs[x+1]
        except KeyError:
            print('Unrecognised cmdarg:',cfgs[x:x+1])

    return kwargs

def setup_compute(proj_code, workdir=None, **kwargs):
    if os.getenv('KERCHUNK_DIR'):
        workdir = os.getenv('KERCHUNK_DIR')

    cfg_file = f'{workdir}/in_progress/{proj_code}/base-cfg.json'
    if os.path.isfile(cfg_file):
        with open(cfg_file) as f:
            cfg = json.load(f)
    else:
        print(f'Error: cfg file missing or not provided - {cfg_file}')
        return None
    
    detail_file = f'{workdir}/in_progress/{proj_code}/detail-cfg.json'
    if os.path.isfile(detail_file):
        with open(detail_file) as f:
            detail = json.load(f)
    else:
        print(f'Error: cfg file missing or not provided - {detail_file}')
        return None
    
    if detail['type'] == 'JSON':
        Indexer(proj_code, cfg=cfg, detail=detail, **kwargs).create_refs()
    else:
        pass

if __name__ == '__main__':
    proj_code = sys.argv[1]
    kwargs = rundecode(sys.argv[2:])

    setup_compute(proj_code, **kwargs)