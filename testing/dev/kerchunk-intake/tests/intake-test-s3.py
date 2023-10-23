import intake
import intake_esm
import s3fs
import xarray as xr
import fsspec
import matplotlib
import matplotlib.pyplot as plt

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

_xr_open_args = {"consolidated": False}
fssopts = {
    "key": s3_config["token"],
    "secret": s3_config["secret"],
    "client_kwargs": {"endpoint_url": s3_config["endpoint_url"]}
}
print(fssopts)
bucket_id = "kct-test-s3-quobyte-cmip6a"
test_cat  = "test_cat.json"
intake_uri = f"s3://{bucket_id}/{test_cat}"

def test_open_a_kerchunk_file():

    # This method works for opening an s3 kerchunk file
    kfile = "s3://kct-test-s3-quobyte-cmip6a/CMIP6_ScenarioMIP_NCC_NorESM2-LM_ssp126_r1i1p1f1_3hr_mrro_gn_v20191108.json"
    ref = s3fs.S3FileSystem(**fssopts)
    ref = ref.open(kfile, compression=None)

    mapper = fsspec.get_mapper(
        "reference://", 
        fo=ref, # string - local file, ref object - s3 file
        target_protocol="http", 
        remote_options=fssopts, 
        target_options={"compression": None}
    )
    xobj = xr.open_zarr(mapper, **_xr_open_args)

def test_open_intake_catalog():
    print('uri',intake_uri)
    test_catalog = intake.open_esm_datastore(intake_uri, storage_options={**fssopts})

    query = dict(
        variable_id='mrro')
    subcat = test_catalog.search(**query)

    storage_options = {
        'fo':'',
        'target_protocol':'http',
        'remote_options':fssopts,
        'target_options':{"compression":None}
    }

    dset_dict = subcat.to_dataset_dict(
        xarray_open_kwargs={"decode_times": True, "use_cftime": True},
        storage_options=storage_options
    )
    ds = dset_dict[list(subcat)[0]]
    xarr = ds.mrro[1]
    xarr.plot()
    plt.savefig('test1.png')

test_open_a_kerchunk_file()
#test_open_intake_catalog()