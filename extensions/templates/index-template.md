# Example Detail-cfg to Index mapping

Not all parameters from the detail-cfg are required or should be included in the index (elasticsearch) record for that product.

Below is a list of all parameters and which should be included/excluded. Some project codes (especially scan-skipped) do not 
have these values. Either add scan.summarise_json recording to compute_config, write a new section in assess.py to scan.summarise_json
for a set of files, or leave these blank in any created records.

```
{
    "data_represented": "0.0", # Maps to netcdf_data for early pipeline versions
    "chunks_per_file": "0.0",
    "num_files": "0",
    "total_chunks": "0.0",
    "addition": "0.000",
    "type": "JSON",
    "kerchunk_data": "0.00 KB",
    "netcdf_data": "0.00 MB",
    "estm_chunksize": "0.00 KB",
    "estm_spatial_res": "0.00 deg",
    "timings": { # Not required beyond pipeline scope
        "convert_estm": null,
        "concat_estm": null,
        "validate_estm": null,
        "convert_actual": null,
        "concat_actual": null,
        "validate_actual": null,
        "compute_actual": null
    },
    "variable_count": 0,
    "variables": [],
    "var_err": false,   # Not required beyond pipeline
    "file_err": false,  # Not required beyond pipeline
    "driver": "ncf3",   # Technically not required but indicates original data type (netcdf3 or hdf5)
    "virtual_concat": false,
    "combine_kwargs": {
        "concat_dims": [
            "time"
        ],
        "identical_dims": [
            "bnds",
            "height",
            "lat",
            "lat_bnds",
            "lon",
            "lon_bnds"
        ]
    },
    "special_attrs": [
        "creation_date",
        "tracking_id"
    ],
    "last_run": [   # Not required beyond pipeline
        "validate",
        "240:00"
    ],
    "links_added": false,  # Not required - this should be true for ALL records feeding into ingestion.
    "skipped": true,       # Not required beyond pipeline
    "quality_required": true  # Not required but could be mapped to a different attribute - time_consistent (false if different files have different time dim sizes.)
}
```