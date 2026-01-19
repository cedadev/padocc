__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import requests
import json
import os

def open_json(file):
    with open(file) as f:
        return json.load(f)

try:
    from httpx_auth import OAuth2ClientCredentials
    import httpx

    creds = open_json(os.environ.get('AUTH_CREDENTIALS'))

    auth = OAuth2ClientCredentials(
        "https://accounts.ceda.ac.uk/realms/ceda/protocol/openid-connect/token",
        client_id=creds["id"],
        client_secret=creds["secret"]
    )

    client = httpx.Client(
        verify=False,
        timeout=180,
    )
except:
    # Disabled auth for pushing to STAC API
    auth = None
    client = None

STAC_API = 'https://api.stac.ceda.ac.uk'

REPORT_ASSET = {
    "href": "https://dap.ceda.ac.uk",
    "type": "application/json",
    "title": "PADOCC Aggregation Data Report",
    "roles": ["reference"]
}

KERCHUNK_ASSET = {
    "open_zarr_kwargs": {
        "decode_times": True
      },
      "roles": [
        "reference",
        "data"
      ],
      "href": None,
      "type": "application/json"
}

def update_stac(stac_api, collection, stac_record, item_id):
    print(f'{stac_api}/collections/{collection}/items/{item_id}', end=' > ')
    print(client.put(
        f'{stac_api}/collections/{collection}/items/{item_id}',
        json=stac_record,
        auth=auth
    ))

def add_report_asset(stac_record, report_path):

    new_asset = dict(REPORT_ASSET)
    new_asset['href'] = "https://dap.ceda.ac.uk" + report_path

    stac_record['assets']['padocc_report'] = new_asset
    return stac_record

def add_kerchunk_asset(stac_record, kerchunk_path):
    new_asset = dict(KERCHUNK_ASSET)
    new_asset['href'] = "https://dap.ceda.ac.uk" + kerchunk_path

    stac_record['assets']['reference_file'] = new_asset
    stac_record['properties']['aggregation_status'] = 'OK'
    return stac_record

def get_stac(file_id, stac_api, collection):
    return requests.get(f'{stac_api}/collections/{collection}/items/{file_id}').json()

def add_to_stac(kpath, collection, stac_api=STAC_API):

    if client is None or auth is None:
        raise ValueError('Unauthorised - please provide an "AUTH_CREDENTIALS" file via environment variable "$AUTH_CREDENTIALS"')
     
    if 'kr' not in kpath:
        raise ValueError('Unrecognised version scheme for STAC upload - "kr" revision not found')
    
    proj_id = kpath.split('kr')[-1].rstrip('_.')

    try:
        item_record = get_stac(proj_id, stac_api, collection)
    except:
        raise ValueError(f'Unable to fetch STAC record {STAC_API}/collections/{collection}/items/{proj_id}')
    
    item_record = add_kerchunk_asset(item_record, kpath)
    revision = kpath.replace(proj_id,'').replace('.json','').lstrip('_.')

    report_dot = '/'.join(kpath.split('/')[:-2]) + '/reports/' + proj_id + '.' + revision.replace('r','') + '_report.json'
    report_us = '/'.join(kpath.split('/')[:-2]) + '/reports/' + proj_id + '_' + revision.replace('r','') + '_report.json'

    report = None
    if os.path.isfile(report_dot):
        report = report_dot
    elif os.path.isfile(report_us):
        report = report_us

    if report is None:
        print(f'WARNING: No report found for {kpath}')
    else:
        item_record = add_report_asset(item_record, report)

    update_stac(stac_api, collection, item_record, proj_id)

