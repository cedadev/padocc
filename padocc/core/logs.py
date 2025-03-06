__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging
import os
from typing import Callable, Union

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
            forceful: bool = None, 
            dryrun  : bool = None, 
            thorough: bool = None,
            verbose: int = 0
        ) -> None:
        """
        Initialise a logged operation.

        :param logger:              (logging.Logger) Logger supplied to this Operation.

        :param label:               (str) The label to apply to the logger object.

        :param fh:                  (str) Path to logfile for logger object generated in this specific process.

        :param logid:               (str) ID of the process within a subset, which is then added to the name
            of the logger - prevents multiple processes with different logfiles getting
            loggers confused.

        :param verbose:         (int) Level of verbosity for log messages (see core.init_logger).

        :param forceful:        (bool) Continue with processing even if final output file 
            already exists.

        :param dryrun:          (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.

        :param thorough:        (bool) From args.quality - if True will create all files 
            from scratch, otherwise saved refs from previous runs will be loaded.
        """
        self._logid = logid
        self._verbose = verbose

        self._forceful = None
        self._dryrun   = None
        self._thorough = None

        self._set_fh_kwargs(forceful=forceful, dryrun=dryrun, thorough=thorough)

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

    @classmethod
    def help(cls, func: Callable = print):
        """
        No public methods.
        """
        pass

    @property
    def fh_kwargs(self) -> dict:
        return {
            'dryrun': self._dryrun,
            'forceful': self._forceful,
            'thorough': self._thorough,
        }
    
    @fh_kwargs.setter
    def fh_kwargs(self, value: dict) -> None:
        self._set_fh_kwargs(**value)

    def _set_fh_kwargs(
            self, 
            forceful: bool = None, 
            dryrun: bool = None, 
            thorough: bool = None
        ) -> None:
        """
        Set the FH Kwargs for all objects.

        Override but defaults to current value if not specified.

        :param forceful:        (bool) Continue with processing even if final output file 
            already exists.

        :param dryrun:          (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.

        :param thorough:        (bool) From args.quality - if True will create all files 
            from scratch, otherwise saved refs from previous runs will be loaded.
        """
        self._forceful = forceful or self._forceful
        self._dryrun   = dryrun or self._dryrun
        self._thorough = thorough or self._thorough

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

    logger.setLevel(levels[verbose])

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