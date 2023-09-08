from minio import Minio
import os

bucket_id = "kct-test-s3-quobyte-cmip6a"
s3_config = {
    "token":"",
    "secret":"",
    "endpoint_url":""
}

s3cfg_headers = {
    "access_key" : "token",
    "host_base"  : "endpoint_url",
    "secret_key" : "secret"
}

with open('/home/users/dwest77/.s3cfg') as f:
    content = f.readlines()

for line in content:
    for key in s3cfg_headers.keys():
        if key in line:
            s3_config[s3cfg_headers[key]] += line.split('=')[-1].replace('\n','').replace(' ','')

print(s3_config)

def file_in_bucket(fname, bucket_id, recursive=False):
    return fname in [obj.object_name for obj in mc.list_objects(bucket_id, recursive=recursive)]

def upload_kfiles(files):
    for fpath in files:
        if not file_in_bucket(fpath, bucket_id):
            fname = os.path.basename(fpath)
            bucket_path = f"{bucket_id}/{fname}" 
            file_uris.append(f"s3://{bucket_path}")

            mc.fput_object(bucket_id, fname, fpath)

def remove_item(itemname, bucket_id):
    mc.remove_object(bucket_id, itemname)


mc = Minio(s3_config["endpoint_url"], s3_config["token"], s3_config["secret"], secure=False)
if True:
    print([obj.object_name for obj in mc.list_objects(bucket_id)])
    mc.get_object(bucket_id)

if False:
    mc.make_bucket(bucket_id)
    print('Bucket Added')

if False:
    print(mc.list_buckets())
    print([obj.object_name for obj in mc.list_objects('kct-test-s3-quobyte-7')])

if False:
    fpath = "/home/users/dwest77/Documents/intake_dev/kerchunk-intake/tests/test_file.json"
    file_uris = []
    fname = os.path.basename(fpath)
    bucket_path = f"{bucket_id}/{fname}" 
    file_uris.append(f"s3://{bucket_path}")

    mc.fput_object(bucket_id, fname, fpath)

    assert file_in_bucket(fname, bucket_id)

    remove_item(fname, bucket_id)

    assert not file_in_bucket(fname, bucket_id)

if False: # Compile list of kerchunk files and upload to s3
    import pandas as pd
    import json

    test_csv = 'test_cat.csv'
    test_json = 'test_cat.json'

    kerchunk_store = '/gws/nopw/j04/cedaproc/kerchunk-store'

    csv_uri  = f"s3://{bucket_id}/{test_csv}"
    json_uri = f"s3://{bucket_id}/{test_json}"

    # Update json catalog
    if False:
        print('Updating CSV path in JSON file')
        f = open(test_json,'r')
        json_cat = json.load(f)
        f.close()

        json_cat['catalog_file'] = csv_uri

        f = open(test_json,'w')
        f.write(json.dumps(json_cat))
        f.close()
    if False:
        # Extract kerchunk paths and set new paths as s3 indexes locally
        print('Updating paths for Kerchunk files')
        df    = pd.read_csv(test_csv)
        uris  = df['uri'].to_list()
        suris = [ f"s3://{bucket_id}/{os.path.basename(uri)}" for uri in uris ]
        df['uri'] = df['uri'].replace(uris, suris)
        df.to_csv(test_csv, index=False)
    if False:
        print('Uploading kerchunk files to bucket: ',bucket_id)
        # Add all kerchunk files to the bucket
        for x in range(len(uris)):
            fname = os.path.basename(uris[x])
            fpath = f"{kerchunk_store}/ScenarioMIP_NCC/kerchunk_files/{fname}"
            if not file_in_bucket(fname, bucket_id):
                print(x, fname, fpath)
                mc.fput_object(bucket_id, fname, fpath)

    print('Uploading intake catalog to bucket: ', bucket_id)
    if not file_in_bucket(test_csv, bucket_id):
        mc.fput_object(bucket_id, test_csv, test_csv)

    if not file_in_bucket(test_json, bucket_id):
        mc.fput_object(bucket_id, test_json, test_json)
    print('Complete')



