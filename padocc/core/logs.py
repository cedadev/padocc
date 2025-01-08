__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging
import os
from typing import Union, Optional

levels = [
    logging.WARN,
    logging.INFO,
    logging.DEBUG
]

SUFFIX_LIST = ['K','M','G']

SUFFIXES = {
    'K': 1000,
    'M': 1000000,
    'G': 1000000000
}


class FalseLogger:
    """
    Supplementary class where a logger is not wanted but is required for
    some operations.
    """
    def __init__(self):
        pass
    def debug(self, message: str):
        pass
    def info(self, message: str):
        pass
    def warning(self, message: str):
        pass
    def error(self, message: str):
        pass

class LoggedOperation:
    """
    Allows inherritance of logger objects without creating new ones.
    """
    def __init__(
            self, 
            logger : Union[logging.Logger,FalseLogger, None] = None,
            label  : Union[str,None] = None, 
            fh     : Union[str,None] = None, 
            logid  : Union[str,None] = None, 
            verbose: int = 0
        ) -> None:

        self._logid = logid
        self._verbose = verbose
        if hasattr(self, 'logger'):
            return
        if logger is None:
            self.logger = init_logger(
                self._verbose, 
                label,
                fh=fh, 
                logid=logid)
        else:
            self.logger = logger

def reset_file_handler(
        logger  : logging.Logger,
        verbose : int, 
        fh : str
    ) -> logging.Logger:
    """
    Reset the file handler for an existing logger object.

    :param logger:      (logging.Logger) An existing logger object.

    :param verbose:     (int) The logging level to reapply.

    :param fh:     (str) Address to new file handler.

    :returns:   A new logger object with a new file handler.
    """
    logger.handlers.clear()
    verbose = min(verbose, len(levels)-1)

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fdir = '/'.join(fh.split('/')[:-1])
    if not os.path.isdir(fdir):
        os.makedirs(fdir)
    if os.path.isfile(fh):
        os.system(f'rm {fh}')

    os.system(f'touch {fh}')

    fhandle = logging.FileHandler(fh)
    fhandle.setLevel(levels[verbose])
    fhandle.setFormatter(formatter)
    logger.addHandler(fhandle)

    return logger

def init_logger(
        verbose : int, 
        name  : str, 
        fh    : str = None, 
        logid : str = None
    ) -> logging.Logger:
    """
    Logger object init and configure with standardised formatting.
    
    :param verbose:     (int) Level of verbosity for log messages (see core.init_logger).

    :param name:        (str) The label to apply to the logger object.

    :param fh:          (str) Path to logfile for logger object generated in this specific process.

    :param logid:       (str) ID of the process within a subset, which is then added to the name
        of the logger - prevents multiple processes with different logfiles getting
        loggers confused.

    :returns:       A new logger object.
    
    """
    verbose = min(verbose, len(levels)-1)
    if logid is not None:
        name = f'{name}_{logid}'

    logger = logging.getLogger(name)

    if fh is not None:
        return reset_file_handler(logger, verbose, fh)

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])
    logger.propagate = False

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger