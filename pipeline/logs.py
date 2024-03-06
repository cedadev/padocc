__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import logging
import os
from pipeline.errors import MissingVariableError
from datetime import datetime

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

def log_status(phase, proj_dir, status, logger, jobid='', dryrun=''):
    """Find the status file for this project code, add a new entry for the status.
    - Entry should be of the form:
    - phase, status, time, jobid
    """

    # Create file if not already present
    status_log = f'{proj_dir}/status_log.csv'
    if not os.path.isfile(status_log):
        logger.debug(f'Creating status file {status_log}')
        os.system(f'touch {status_log}')
    
    # Open existing file
    with open(status_log) as f:
        lines = [r.strip() for r in f.readlines()]

    # Add new content from most recent run
    lines.append(f'{phase},{status},{datetime.now().strftime("%H:%M %D")},{jobid},{dryrun}')

    # Save content
    with open(status_log, 'w') as f:
        f.write('\n'.join(lines))
    logger.info(f'Updated new status: {phase} - {status}')

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

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(new_log)
    fh.setLevel(levels[verbose])
    logger.addHandler(fh)

    return logger

def init_logger(verbose, mode, name, fh=None):
    """Logger object init and configure with formatting"""
    verbose = min(verbose, len(levels)-1)

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if fh:
        handle = logging.FileHandler(fh)
        handle.setLevel(levels[verbose])
        logger.addHandler(handle)

    return logger