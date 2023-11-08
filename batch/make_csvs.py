import sys
import json
import os
import random

config = {
    'proj_code': None,
    'workdir': None,
    'proj_dir':None,
    'pattern': None,
    'update': None,
    'remove': None
}

general = '/badc/cmip6/data/CMIP6/'

# List 100 random CMIP datasets

def get_CMIP_data_recursive(path):
    contents = os.listfiles(path)
    randsel = contents[random.randint(0,len(contents)-1)]
    if randsel.endswith('.nc'):
        return path
    else:
        return get_CMIP_data_recursive(os.path.join(path, randsel))
    
print(get_CMIP_data_recursive(general))