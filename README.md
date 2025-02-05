# PADOCC Package

Padocc (Pipeline to Aggregate Data for Optimal Cloud Capabilities) is a Data Aggregation pipeline for creating Kerchunk (or alternative) files to represent various datasets in different original formats.
Currently the Pipeline supports writing JSON/Parquet Kerchunk files for input NetCDF/HDF files. Further developments will allow GeoTiff, GRIB and possibly MetOffice (.pp) files to be represented, as well as using the Pangeo [Rechunker](https://rechunker.readthedocs.io/en/latest/) tool to create Zarr stores for Kerchunk-incompatible datasets.

[Example Notebooks at this link](https://mybinder.org/v2/gh/cedadev/padocc.git/main?filepath=showcase/notebooks)

[Documentation hosted at this link](https://cedadev.github.io/kerchunk-builder/)

![Kerchunk Pipeline](docs/source/_images/pipeline.png)

## Pre-release b
Release date: 20th January 2025

This pre-release contains updated source code and source code documentation, but some of the main descriptors that are hand-written (not `source`) may be out of date. Please refer to the release notes for details on what has changed.

This package acknowledges contributions by [Matt Brown](matbro@ceh.ac.uk) as a pre-release tester.

## Installation

To install this package, clone the repository using git clone (and switch to the MigrationOO branch - `git checkout MigrationOO` if release v1.3 has not been released.)

Then follow the steps below to install the package with the necessary dependencies.

```
python -m venv .venv
source .venv/bin/activate
pip install poetry
poetry install
```

## Usage

Please refer to the `tests/` scripts for how to use the `GroupOperation` and `ProjectOperation` classes.
