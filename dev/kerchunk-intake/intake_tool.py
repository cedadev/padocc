"""
Tool for assembling intake catalog given a CMIP project address.
Example: /badc/cmip6/data/CMIP6/../../../../*.nc

Steps:
 - Take given project address and extract attributes for intake catalog entry
 - Assemble file list
 - Run kerchunk tools cli.py and set output file to store in specific location
 - Open intake catalog and add entry
 - Store Intake/:
   - <Catalog>/
     - Kerchunk_Files/
       - <ID>.json
     - filelists/
       - <ID>.txt
     - Catalog.json
     - Catalog.csv

"""
import os, sys
import intake
import json
import pandas as pd

KERCHUNK_INTAKE = '/home/users/dwest77/Documents/intake_dev/kerchunk-intake'
KERCHUNKTOOLS = '~/Documents/kerchunk_dev/kerchunk-tools/kerchunk_tools/'

f = open(f'{KERCHUNK_INTAKE}/templates/catalog_template.json','r')
cat_template = json.load(f)
f.close()

g = open(f'{KERCHUNK_INTAKE}/templates/columns.txt','r')
columns = [l.replace('\n','') for l in g.readlines()]
columns = columns[:12]
g.close()

intake_store = '/gws/nopw/j04/cedaproc/kerchunk-store'

def vprint(message, verbose=True):
    if verbose:
        print(message)

def create_catalog(catalog_json, catalog_csv, cat_id, description=None, title=None):
    from datetime import datetime
    catalog = dict(cat_template)
    catalog["id"] = cat_id
    if description:
        catalog["description"] = description
    else:
        catalog["description"] = cat_id
    if title:
        catalog["title"] = title
    catalog["catalog_file"] = catalog_csv

    f = open(catalog_json,'w')
    f.write(json.dumps(catalog))
    f.close()

def check_update_catalog(cat_csv, path_attrs, kfile):
    """
    Takes intake catalog as parameter:
     - Search using all parameters for existing kerchunk file reference.
     - If file is known, return False and program exits with message
     - If file unknown, add entry to record then return True to proceed with kerchunk file generation.
    """

    if len(path_attrs) > len(columns)-2:
        # Condense last attribute into single last entry
        last = '.'.join(path_attrs[-2:])
        path_attrs = path_attrs[:-2] + [last]

    data = path_attrs[:] + [kfile, "reference"]
    print(data)

    # Might need to thing about synchronised write issues between threads
    # Maybe write to separate temp files and add a closing job to combine them all and add to existing records
    # For now just add to existing csv

    try:
        os.system(f'touch {cat_csv}')
        f = open(cat_csv,'w')
        entry = ','.join(data)
        f.write(entry)
        f.close()
    except:
        vprint('[INFO] CSV Write Issue') 

def get_cmdargs(args):
    try:
        proj_path = args[1]
    except:
        print('Argument Error: Intake Tool.py requires project path arg')
        return None

    if '-f' in args:
        try:
            f = open(proj_path,'r')
            proj_path = f.readlines()[0].replace('\n','')
            f.close()
        except:
            print(f'Argument Error: Invalid project path or file read - {proj_path}')
            return None
    return proj_path

def makedirs(path):
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except:
            print(f'DirectoryError: Unable to make dir {path}')
            sys.exit()

def main(args):
    override  = True
    proj_path = get_cmdargs(args)
    if not proj_path:
        return None

    # Check data directory exists
    vprint('[INFO] Checking Data Dir')
    cmip6_path = '/badc/cmip6/data/'
    if not proj_path.startswith(cmip6_path):
        vprint('[INFO] Unsupported path format')
        return None

    # Extract attributes from directory path
    vprint('[INFO] Extracting Attributes')
    path_attrsr = proj_path.replace(cmip6_path,'').split('/') # Get all but last as it should be blank
    if path_attrsr[-1] == '':
        path_attrsr = path_attrsr[:-1]
        
    path_attrs = []
    for p in path_attrsr:
        if p != '':
            path_attrs.append(p)
            
    cat_id  = '_'.join(path_attrs[1:3])
    proj_id = '_'.join(path_attrs)

    makedirs(f'{intake_store}/{cat_id}')

    vprint('[INFO] Creating Filelist')
    filelist = f'{intake_store}/{cat_id}/filelists/{proj_id}.txt'
    kfile = f'{cat_id}/kerchunk_files/{proj_id}.json'


    # Ensure directories exist
    if not os.path.isfile(filelist) or override:
        makedirs(f'{intake_store}/{cat_id}/filelists/')
        try:
            os.system(f'ls -d {proj_path}* > {filelist}')
        except:
            pass

    if not os.path.isfile(os.path.join(intake_store, kfile)) or override:
        makedirs(f'{intake_store}/{cat_id}/kerchunk_files/')

        cache_dir = ''
        
        vprint('[INFO] Run Kerchunk Tools Pipeline with Appropriate Settings')
        kwargs = f'-f {filelist} -o {kfile} -p {intake_store}' # Add cache dir
        
        os.system(f'python {KERCHUNKTOOLS}cli.py create {kwargs}')

    else:
        vprint('[INFO] Skip existing kerchunk file')

    vprint('[INFO] Adding Project to Intake Catalog')

    catalog_json  = f'{intake_store}/{cat_id}/{cat_id}_catalog.json'
    catalog_csv   = f'{intake_store}/{cat_id}/{cat_id}_catalog.csv'
    catalog_csv_p = f'{intake_store}/{cat_id}/temp/{proj_id}.csv'


    # Replace with database entry submission - cmip6 postgres database
    if not os.path.isfile(catalog_csv_p) or override:

        if not os.path.isfile(catalog_json):
            vprint('[INFO] No Catalog found, creating a new catalog file')
            create_catalog(catalog_json, catalog_csv, cat_id)
        makedirs(f'{intake_store}/{cat_id}/temp/')

        check_update_catalog(catalog_csv_p, path_attrs, kfile)

    else:
        vprint('[INFO] Skip creating existing entry')

    vprint('[INFO] --- End of Log ---')
    print(f'{intake_store}/{cat_id}/{cat_id}_catalog.csv')
    print(f'{intake_store}/{cat_id}/temp/{proj_id}.csv')

main(sys.argv)

# this is a comment