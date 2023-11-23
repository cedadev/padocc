import os
import json

workdir = '/gws/nopw/j04/esacci_portal/kerchunk/pipeline'

def get_updates():
    inp = None
    valsdict = {}
    while inp != 'exit':
        inp = input('Attribute: ("exit" to escape):')
        if inp != 'exit':
            val = input('Value: ')
            valsdict[inp] = val
    return valsdict

def get_removals():
    valsarr = []
    while inp != 'exit':
        inp = input('Attribute: ("exit" to escape):')
        if inp != 'exit':
            valsarr.append(inp)
    return valsarr

def init_config(workdir):
    proj_code = input('Project Code: ')
    pattern   = input('Wildcard Pattern: (leave blank if not applicable) ')
    if pattern == '':
        filelist  = input('Path to filelist: ')
        pattern   = None
    else:
        filelist  = None

    if os.getenv('KERCHUNK_DIR'):
        workdir = os.getenv('KERCHUNK_DIR')

    print('Working dir currently set to',workdir)
    print('NOTE: Workdir should not include "in_progress" in the path.')
    override_wd = input('Do you wish to override? (y/n) ')
    if override_wd == 'y':
        workdir = input('Enter custom working dir path: ')

    proj_dir = f'{workdir}/in_progress/{proj_code}'
    if os.path.isdir(proj_dir):
        print('Error: Directory already exists -',proj_dir)
        return None
    else:
        os.makedirs(proj_dir)

    config = {
        'proj_code': proj_code,
        'workdir': workdir,
        'proj_dir':proj_dir
    }
    do_updates = input('Do you wish to add overrides to metadata values? (y/n): ')
    if do_updates == 'y':
        config['update'] = get_updates()
    
    do_removals = input('Do you wish to remove known attributes from the metadata? (y/n): ')
    if do_removals == 'y':
        config['remove'] = get_removals(remove=True)

    if pattern:
        config['pattern'] = pattern

    with open(f'{proj_dir}/base-cfg.json','w') as f:
        f.write(json.dumps(config))
    print(f'Written cfg file at {proj_dir}/base-cfg.json')

if __name__ == '__main__':
    print('Kerchunk Pipeline Config Initialiser')
    init_config(workdir)