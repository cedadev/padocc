__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from padocc.phases import (
    ScanOperation, 
    KerchunkDS, 
    ZarrDS, 
    cfa_handler,
    ValidateOperation
)

phase_map = {
    'scan': ScanOperation,
    'compute': {
        'kerchunk': KerchunkDS,
        'zarr': ZarrDS,
        'CFA': cfa_handler,
    },
    'validate': ValidateOperation
}