__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

# Code to handle indexing detail-cfg files from the pipeline into a STAC-like Elasticsearch index of created Kerchunk files

# NOTE: Ingestion and indexing are final steps of the pipeline (post-validation),
# but detail-cfg could be uploaded during the pipeline, with the caveat that a setting be required to show pipeline status
# but this would mean multiple updates?

# Any Elasticsearch index should also include the group name (possibly part of each record?) for search purposes.

from ceda_elasticsearch_tools import BulkClient
import json
import os

from datetime import datetime

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
    'group':'CMIP6_rel1_6233',
    'collection':'CMIP6',
    'last_upload':None,
    'phase':None,
    'status':None,

    'netcdf_data':'Unknown',
    'kerchunk_data':'Unknown',
    'num_files':'Unknown',
    'format':'Unknown',
    'chunks_per_file':'Unknown'
}

def catalog_groups(group_ids,test=None):
    # Determine values for each group
    # Fill template dict accordingly
    # Add records using client


    #pad = PadoccClient('padocc-groups','group')

    pad = BulkClient('padocc-groups','group',es_config='/Users/daniel.westwood/cedadev/padocc/config/es_config.json')
    pad.add_records(test)
    pass

def catalog_products(groupID):
    # Take group ID
    # Catalog all products within the group
    # Create collections within the ES index.
    pass


if __name__ == '__main__':
    test = group_template
    test['last_upload'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    catalog_groups(None, test=[test])