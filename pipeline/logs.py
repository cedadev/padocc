import logging
import os
from pipeline.errors import MissingVariableError

levels = [
    logging.WARN,
    logging.INFO,
    logging.DEBUG
]

def init_logger(verbose, mode, name):
    """Logger object init and configure with formatting"""
    verbose = min(verbose, len(levels)-1)

    logger = logging.getLogger(name)
    logger.setLevel(levels[verbose])

    ch = logging.StreamHandler()
    ch.setLevel(levels[verbose])

    formatter = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

def get_attribute(env: str, args, var: str):
    """Assemble environment variable or take from passed argument.
    
    Finds value of variable from Environment or ParseArgs object, or reports failure
    """
    if hasattr(args, var):
        return getattr(args, var)
    elif os.getenv(env):
        return os.getenv(env)
    else:
        raise MissingVariableError(type=f'${var}')