__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

# Code to handle indexing detail-cfg files from the pipeline into a STAC-like Elasticsearch index of created Kerchunk files

# NOTE: Ingestion and indexing are final steps of the pipeline (post-validation),
# but detail-cfg could be uploaded during the pipeline, with the caveat that a setting be required to show pipeline status
# but this would mean multiple updates?

# Any Elasticsearch index should also include the group name (possibly part of each record?) for search purposes.

#from ceda_elasticsearch_tools import BulkClient
import json
import os

from ceda_elastic_py import SimpleClient
from pipeline.utils import get_codes, get_last_run, get_proj_file, get_proj_dir
from datetime import datetime
import glob
phase = {
    'total':None,
    'complete':None,
    'pending':None,
    'issue':None
}

group_template = {
    'group':'CMIP6_rel1_6233',
    'collection': 'CMIP6',
    'last_upload':None,
    'total_codes':'5774',
    'scan': phase,
    'compute': phase,
    'validate': phase,
    'blacklist': 92
}

dataset_template = {
    'DRI':None,
    'group':None,
    'collection':None,
    'last_upload':None,
    'phase':None,

    'netcdf_data':'netcdf_data',
    'kerchunk_data':'kerchunk_data',
    'num_files':'num_files',
    'format':'type',
    'chunks_per_file':'chunks_per_file'
}

class PadoccClient(SimpleClient):

    def __init__(self, index, **kwargs):
        super().__init__(index, **kwargs)

def catalog_groups(group_ids,test=None):
    # Determine values for each group
    # Fill template dict accordingly
    # Add records using client


    #pad = PadoccClient('padocc-groups','group')

    pad = BulkClient('padocc-groups','group',es_config='/Users/daniel.westwood/cedadev/padocc/config/es_config.json')
    pad.add_records(test)
    pass

def catalog_products(workdir, groupID):
    # Take group ID
    # Catalog all products within the group
    # Create collections within the ES index.
    products = get_codes(groupID, workdir, 'proj_codes/main')
    product_entries = []
    if False:
        for p in products:
            proj_dir = get_proj_dir(p, workdir, groupID)
            details  = get_proj_file(proj_dir, 'detail-cfg.json')

            record = dict(dataset_template)
            for i in record:
                if record[i]:
                    if record[i] in details:
                        record[i] = details[record[i]]
            record['DRI'] = p
            record['group'] = groupID
            record['collection'] = ''
            record['last_upload'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
            try:
                record['phase'] = details['last_run'][0]
            except:
                record['phase'] = 'Unknown'

            # LOCAL PATH TESTING!
            with open(f'testing/records/{p}_rec.json','w') as f:
                f.write(json.dumps(record))
    else:
        for i in glob.glob('testing/records/*_rec.json'):
            with open(i) as f:
                product_entries.append(json.load(f))
    pc = PadoccClient('padocc-products', es_config='es_settings.json')

    pc.push_records(product_entries)


if __name__ == '__main__':
    workdir = '/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline'
    catalog_products(workdir, 'CMIP6_rel1_6233')