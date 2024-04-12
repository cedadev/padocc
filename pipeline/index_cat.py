__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

# Code to handle indexing detail-cfg files from the pipeline into a STAC-like Elasticsearch index of created Kerchunk files

# NOTE: Ingestion and indexing are final steps of the pipeline (post-validation),
# but detail-cfg could be uploaded during the pipeline, with the caveat that a setting be required to show pipeline status
# but this would mean multiple updates?

# Any Elasticsearch index should also include the group name (possibly part of each record?) for search purposes.