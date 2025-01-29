__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from .scan import ScanOperation
from .compute import KerchunkDS, ZarrDS, ComputeOperation
from .ingest import IngestOperation
from .validate import ValidateOperation

KNOWN_PHASES = ['init', 'scan', 'compute', 'validate']

phase_map = {
    'scan': ScanOperation,
    'compute': {
        'kerchunk': KerchunkDS,
        'zarr': ZarrDS,
        'CFA': ComputeOperation,
    },
    'validate': ValidateOperation
}