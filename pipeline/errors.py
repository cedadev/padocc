__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import json

def upload_err(proj_code, groupdir, error):
    with open(f'{groupdir}/ErrorSummary.json') as f:
        summ = json.load(f)
    summ[proj_code] = error
    with open(f'{groupdir}/ErrorSummary.json','w') as f:
        f.write(json.dumps(summ))

class BlacklistProjectCode(Exception):
    def __init__(self, verbose=0, proj_code=None, groupdir=None):
        self.message = 'Project Code listed in blacklist for bad data - will not be processed.'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'BlacklistProjectCode')

class MissingVariableError(Exception):
    def __init__(self, type='$', verbose=0, proj_code=None, groupdir=None):
        self.message = f'Missing variable: {type}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'MissingVariableError')

class ExpectTimeoutError(Exception):
    def __init__(self, required=0, current='', verbose=0, proj_code=None, groupdir=None):
        self.message = f'Scan requires minimum {required} - current {current}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'ExpectTimeoutError')

class ProjectCodeError(Exception):
    def __init__(self, verbose=0, proj_code=None, groupdir=None):
        self.message = f'Project Code Extraction Failed'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'ProjectCodeError')

class FilecapExceededError(Exception):
    def __init__(self, nfiles=0, verbose=0, proj_code=None, groupdir=None):
        self.message = f'Filecap exceeded: {nfiles} files attempted'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'FilecapExceededError')

class ChunkDataError(Exception):
    def __init__(self, verbose=0, proj_code=None, groupdir=None):
        self.message = f'Decoding resulted in overflow - received chunk data contains junk (attempted 3 times)'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'ChunkDataError')

class NoValidTimeSlicesError(Exception):
    def __init__(self, message='Kerchunk', verbose=0, proj_code=None, groupdir=None):
        self.message = f'No valid timeslices found for {message}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'NoValidTimeSlicesError')

class VariableMismatchError(Exception):
    def __init__(self, missing={}, verbose=0, proj_code=None, groupdir=None):
        self.message = f'Missing variables {missing} in Kerchunk file'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'VariableMismatchError')

class ShapeMismatchError(Exception):
    def __init__(self, var={}, first={}, second={}, verbose=0, proj_code=None, groupdir=None):
        self.message = f'Kerchunk/NetCDF mismatch for variable {var} with shapes - K {first} vs N {second}'
        super().__init__(self.message)
        if proj_code:
            self.save(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'ShapeMismatchError')

class TrueShapeValidationError(Exception):
    def __init__(self, message='Kerchunk', verbose=0, proj_code=None, groupdir=None):
        self.message = f'Kerchunk/NetCDF mismatch with shapes using full dataset - check logs'
        super().__init__(self.message)
        if proj_code:
            self.save(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'TrueShapeValidationError')

class NoOverwriteError(Exception):
    def __init__(self, verbose=0, proj_code=None, groupdir=None):
        self.message = 'Output file already exists and forceful overwrite not set.'
        super().__init__(self.message)
        if proj_code:
            self.save(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'NoOverwriteError')

class MissingKerchunkError(Exception):
    def __init__(self, message="No suitable kerchunk file found for validation.", verbose=0, proj_code=None, groupdir=None):
        self.message = message
        super().__init__(self.message)
        if proj_code:
            self.save(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'MissingKerchunkError')

class ValidationError(Exception):
    def __init__(self, message="Fatal comparison failure for Kerchunk/NetCDF", verbose=0, proj_code=None, groupdir=None):
        self.message = message
        super().__init__(self.message)
        if proj_code:
            self.save(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'ValidationError')

class SoftfailBypassError(Exception):
    def __init__(self, message="Kerchunk validation failed softly with no bypass - rerun with bypass flag", verbose=0, proj_code=None, groupdir=None):
        self.message = message
        super().__init__(self.message)
        if proj_code:
            self.save(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def save(self, proj_code, groupdir):
        upload_err(proj_code, groupdir,  'SoftfailBypassError')
