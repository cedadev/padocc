__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"


import hashlib
from typing import Union
from elasticsearch import Elasticsearch

def catalog_ceda(
        location: str, 
        name: str,
        api_key: str, 
        cloud_format: str,
        collection: str,
        remote: bool,
        version: str,
        index: str = 'ceda-cloud-products',
        hosts: Union[list,None] = None
    ):
    """
    Catalog the output product of this project.
    """

    hosts = hosts or ['https://elasticsearch.ceda.ac.uk']

    full_location = f'{location}/{name}'
    id = hashlib.sha1(full_location.encode(errors="ignore")).hexdigest()

    simple_json = {
        'id': id,
        'filename': name,
        'directory': full_location,
        'cloud_format': cloud_format,
        'collection': collection,
        'remote':remote,
        'version':version,
    }

    # Push ceda record
    es_client = Elasticsearch(
        hosts=hosts,
        headers={'x-api-key':api_key}
    )

    es_client.update(
        index=index,
        id=id,
        body={'doc': simple_json, 'doc_as_upsert': True}
    )
    return True