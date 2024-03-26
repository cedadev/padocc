# kerchunk-builder

Now a repository under cedadev group!

The Kerchunk Pipeline (Soon to be renamed) is a Data Aggregation pipeline for creating Kerchunk files to represent various datasets in different original formats.
Currently the Pipeline supports writing JSON/Parquet Kerchunk files for input NetCDF/HDF files. Further developments will allow GeoTiff, GRIB and possibly MetOffice (.pp) files to be represented, as well as using the Pangeo [Rechunker](https://rechunker.readthedocs.io/en/latest/) tool to create Zarr stores for Kerchunk-incompatible datasets.

[Example Notebooks at this link](https://mybinder.org/v2/gh/cedadev/kerchunk-builder.git/main?filepath=showcase/notebooks)

[Documentation hosted at this link](https://cedadev.github.io/kerchunk-builder/)

![Kerchunk Pipeline](docs/source/_images/pipeline.png)