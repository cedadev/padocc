import sys
import json
import os

config = {
    'proj_code': None,
    'workdir': None,
    'proj_dir':None,
    'pattern': None,
    'update': None,
    'remove': None
}

if __name__ == '__main__':
    csvfile = sys.argv[1]

    groupdir = os.environ['GROUPDIR']
    groupid = csvfile.split('/')[-2]

    # Open csv and gather data
    with open(f'{groupdir}/{csvfile}') as f:
        datasets = {r.strip().split(',')[0]:r.strip().split(',')[1:] for r in f.readlines()[:]}

    # Configure for each dataset
    params = list(config.keys())
    proj_codes = list(datasets.keys())
    for dsk in proj_codes:
        ds = datasets[dsk]
        cfg = dict(config)
        cfg[params[0]] = dsk
        for x, p in enumerate(params[1:]):
            cfg[p] = ds[x]

        # Save config file
        if not os.path.isdir(cfg['proj_dir']):
            os.makedirs(cfg['proj_dir'])
    
            with open(f'{cfg["proj_dir"]}/base-cfg.json','w') as f:
                f.write(json.dumps(cfg))
        
        else:
            print(f'{cfg["proj_code"]} already exists - skipping')

    print(f'Exported {len(proj_codes)} dataset config files')

    if not os.path.isdir(f'{groupdir}/{groupid}'):
        os.makedirs(f'{groupdir}/{groupid}')
    with open(f'{groupdir}/{groupid}/proj_codes.txt','w') as f:
        f.write('\n'.join(proj_codes))

    print('Written as group ID:',groupid)