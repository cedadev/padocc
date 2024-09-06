__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from padocc import ProjectOperation
from padocc.core import LoggedOperation, FalseLogger
import logging

from padocc.phases import (
    InitOperation,
    ScanOperation,
    KerchunkDS,
    ZarrDS,
    ValidateOperation,
    IngestOperation,
    CatalogOperation
)

COMPUTE = {
    'kerchunk':KerchunkDS,
    'zarr':ZarrDS
}

class Configuration(LoggedOperation):
    """
    Create a configuration instance as an easy interface to setting up all the
    automatic decision-making by padocc (e.g. type of output file if not known).
    Lower level classes are still usable, but the config class provides a good
    starting point to run the typical ``run`` processes.
    """
    def __init__(
            self, 
            workdir,
            logger : logging.logger | FalseLogger = FalseLogger(),
            **kwargs
        ) -> None:
        super().__init__(logger=logger, **kwargs)
        self.workdir = workdir

    def init_config(
            self,
            input_file,
            groupID=None,
            **kwargs
        ) -> None:
        
        io = InitOperation(self.workdir, groupID=groupID, **kwargs)
        io.run(input_file)

    def scan_config(
            self,
            proj_code,
            workdir,
            groupID=None,
            logger=None, 
            mode='kerchunk',
            **kwargs
        ) -> None:
        """
        Configure scanning and access main section, ensure a few key variables are set
        then run scan_dataset.
        
        :param args:        (obj) Set of command line arguments supplied by argparse.

        :param logger:      (obj) Logging object for info/debug/error messages. Will create a new 
                            logger object if not given one.

        :param fh:          (str) Path to file for logger I/O when defining new logger.

        :param logid:       (str) If creating a new logger, will need an id to distinguish this logger
                            from other single processes (typically n of N total processes.)

        :returns:   None
        """

        so = ScanOperation(proj_code, workdir, groupID=groupID, logger=logger, **kwargs)
        so.run(mode=mode)

    def compute_config(
            self, 
            proj_code, 
            workdir, 
            groupID, 
            mode=None,
            **kwargs
        ) -> None:
        """
        serves as main point of configuration for processing/conversion runs. Can
        set up kerchunk or zarr configurations, check required files are present.

        :param args:        (obj) Set of command line arguments supplied by argparse.

        :param logger:      (obj) Logging object for info/debug/error messages. Will create a new 
                            logger object if not given one.

        :param fh:          (str) Path to file for logger I/O when defining new logger.

        :param logid:       (str) If creating a new logger, will need an id to distinguish this logger
                            from other single processes (typically n of N total processes.)

        :param overide_type:    (str) Set as JSON/parq/zarr to specify output cloud format type to use.
        
        :returns:   None
        """
        if mode is None:
            self.logger.debug('Finding the suggested mode from previous scan where possible')
            proj_op = ProjectOperation(
                proj_code,
                workdir,
                groupID
            )

            mode = proj_op.get_mode()

        if mode is None:
            mode = 'kerchunk'

        if mode not in COMPUTE:
            raise ValueError(
                f'Mode "{mode}" not recognised, must be one of '
                f'"{list(COMPUTE.keys())}"'
            )
        
        ds = COMPUTE[mode]

        proj_op = ds(
            proj_code,
            workdir,
            groupID,
            logger=self.logger
        )
        proj_op.run()

