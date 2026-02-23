# PADOCC Package

[![PyPI version](https://badge.fury.io/py/padocc.svg)](https://pypi.python.org/pypi/padocc/)

Padocc (Pipeline to Aggregate Data for Optimal Cloud Capabilities) is a Data Aggregation pipeline for creating Kerchunk (or alternative) files to represent various datasets in different original formats.
Currently the Pipeline supports writing JSON/Parquet Kerchunk files for input NetCDF/HDF files. Further developments will allow GeoTiff, GRIB and possibly MetOffice (.pp) files to be represented, as well as using the Pangeo [Rechunker](https://rechunker.readthedocs.io/en/latest/) tool to create Zarr stores for Kerchunk-incompatible datasets.

[Example Notebooks at this link](https://mybinder.org/v2/gh/cedadev/padocc.git/main?filepath=showcase/notebooks)

[Documentation hosted at this link](https://cedadev.github.io/kerchunk-builder/)

![Kerchunk Pipeline](docs/source/_images/pipeline.png)

## Release 1.4.4

Release date: 22nd January 2026

See the ![release notes](https://github.com/cedadev/padocc/releases/tag/v1.4.4) for details.

This package acknowledges contributions by [Matt Brown](matbro@ceh.ac.uk) as a pre-release tester.

## Installation

To install this package, clone the repository using git clone, then follow the steps below to install the package with the necessary dependencies.

```
python -m venv .venv
source .venv/bin/activate
pip install poetry
poetry install
```

Alternatively, install from PyPi with:
```
pip install padocc==1.4.4
```

## Example Basic Usage.

Once installed, set a working directory environment variable. This location will be used to create all files within the PADOCC pipeline.

```
export WORKDIR=path/to/my/area
```

Note: You may also want to set the `LOTUS_CFG` environment variable, which must point to a lotus config file for use in parallel job deployment. See this link https://cedadev.github.io/padocc/detailed/parallel.html#lotus-2-configurations for more details.

1. Assemble the initialisation files.

You will need a text file containing all paths to the files you wish to aggregate per-dataset. (For files with a single variable in each, you will need a text file per variable.) Alternatively if all files can be described by a simple wildcard pattern i.e `path/to/files/*.nc` you may use this. These must go into a CSV file formatted as below for each row:

```
name_of_dataset,<path_to_text_file_OR_pattern>,,
```

Add a new row for each dataset/variable described by a set of input files.

Run the following commands in order (if you have >5 datasets in your CSV group you may want to look into parallelisation).

```
padocc init -G <group_name> -i <path_to_csv_file> -v
padocc scan -G <group_name> -v
padocc compute -G <group_name> -v
padocc validate -G <group_name> -v
```
If there are problems in the scan/compute phase please refer to the list of known errors here https://cedadev.github.io/padocc/detailed/features.html#custom-pipeline-errors. If the validate phase ends with Fatal errors you may need to recompute with alternative aggregators (V or K). Please try all the combinations to see if any aggregation works (`--aggregator V` or `K` in compute, with `-n` to increment version number).

Validations that result in Success or Warnings are OK and can proceed to completion. The report generated in validation is saved to the completion directory by default.

Note: Only do this once all groups are finished validation. Check this with `padocc status -G <group_name>`

```
padocc complete -G <group_name> --completion_dir path/to/outputs
```

If the data is NOT in the CEDA archive, you will need to add custom `--sub` and `--replace` to change the local filepaths of your input files to remote paths (wherever they are downloadable).

For all other queries please contact Daniel Westwood (daniel.westwood@stfc.ac.uk)


