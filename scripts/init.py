import os
import json

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

def init_config(args):
    if hasattr(args,'input'):
        load_from_input_file(args)
    else:
        get_input(args)

def load_from_input_file(args):
    if os.path.isfile(args.input):
        with open(args.input) as f:
            refs = json.load(f)

        proj_dir = refs['proj_dir']
        if not os.path.isdir(proj_dir):
            os.makedirs(proj_dir)
        if not os.path.isfile(f'{proj_dir}/base-cfg.json'):
            os.system(f'cp {args.input} {proj_dir}/base-cfg.json')
    else:
        print(f'Error: Input file {args.input} does not exist')
        return None

def get_input(args):

    # Get basic inputs
    proj_code = input('Project Code: ')
    pattern   = input('Wildcard Pattern: (leave blank if not applicable) ')
    if pattern == '':
        filelist  = input('Path to filelist: ')
        pattern   = None
    else:
        filelist  = None

    if os.getenv('WORKDIR'):
        workdir = os.getenv('WORKDIR')

    if args.workdir and args.workdir != workdir:
        print('Environment workdir does not match provided address')
        print('ENV:',workdir)
        print('ARG:',args.workdir)
        choice = 'Choose to keep the ENV value or overwrite with the ARG value: (E/A) ':
        if choice == 'E':
            pass
        elif choice == 'A':
            os.environ['WORKDIR'] = args.workdir
            workdir = args.workdir
        else:
            print('Invalid input, exiting')
            return None

    proj_dir = f'{workdir}/in_progress/{proj_code}'
    if os.path.isdir(proj_dir):
        if args.forceful:
            pass
        else:
            print('Error: Directory already exists -',proj_dir)
            return None
    else:
        os.makedirs(proj_dir)

    config = {
        'proj_code': proj_code,
        'workdir'  : workdir,
        'proj_dir' : proj_dir
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
    print('Kerchunk Pipeline Config Initialiser - run using master scripts')