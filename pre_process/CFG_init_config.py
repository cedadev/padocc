import os
import json

workdir = '/gws/nopw/j04/esacci_portal/kerchunk/pipeline'

def init_config(workdir):
    proj_code = input('Project Code: ')
    pattern   = input('Wildcard Pattern: (leave blank if not applicable) ')
    if pattern == '':
        filelist  = input('Path to filelist: ')
        pattern   = None
    else:
        filelist  = None

    print('Default working dir is',workdir)
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

    if pattern:
        config['pattern'] = pattern

    with open(f'{proj_dir}/base-cfg.json','w') as f:
        f.write(json.dumps(config))
    print(f'Written cfg file at {proj_dir}/base-cfg.json')

if __name__ == '__main__':
    print('Kerchunk Pipeline Config Initialiser')
    init_config(workdir)