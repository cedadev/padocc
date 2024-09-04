__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging
import os

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
    def __init__(self):
        pass
    def debug(self, message):
        pass
    def info(self, message):
        pass
    def warning(self, message):
        pass
    def error(self, message):
        pass

def reset_file_handler(logger, verbose, new_log):
    logger.handlers.clear()
    verbose = min(verbose, len(levels)-1)

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(new_log)
    fh.setLevel(levels[verbose])
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

def init_logger(verbose, name, fh=None, logid=None):
    """Logger object init and configure with formatting"""

    verbose = min(verbose, len(levels)-1)
    if logid != None:
        name = f'{name}_{logid}'

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if fh:
        fdir = '/'.join(fh.split('/')[:-1])
        if not os.path.isdir(fdir):
            os.makedirs(fdir)
        if os.path.isfile(fh):
            os.system(f'rm {fh}')

        os.system(f'touch {fh}')

        handle = logging.FileHandler(fh)
        handle.setLevel(levels[verbose])
        handle.setFormatter(formatter)
        logger.addHandler(handle)

    return logger