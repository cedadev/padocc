__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

import json
import os

def upload_err(proj_code, groupdir, error):
    summ_file = f'{groupdir}/ErrorSummary.json'
    if os.path.isfile(summ_file):
        with open(summ_file) as f:
            summ = json.load(f)
        summ[proj_code] = error
        with open(summ_file,'w') as f:
            f.write(json.dumps(summ))

class KerchunkException(Exception):
    def __init__(self, proj_code, groupdir):
        self.proj_code = proj_code
        self.groupdir  = groupdir
        super().__init__(self.message)
        if proj_code and groupdir:
            self.save()
    def save(self):
        upload_err(self.proj_code, self.groupdir, str(self))

class BlacklistProjectCode(KerchunkException):
    def __init__(self, verbose=0, proj_code=None, groupdir=None):
        """The project code you are trying to run for is on the list of project codes to ignore."""
        self.message = 'Project Code listed in blacklist for bad data - will not be processed.'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'BlacklistProjectCode'

class MissingVariableError(KerchunkException):
    def __init__(self, type='$', verbose=0, proj_code=None, groupdir=None):
        """A variable is missing from the environment or set of arguments."""
        self.message = f'Missing variable: {type}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'MissingVariableError'

class ExpectTimeoutError(KerchunkException):
    def __init__(self, required=0, current='', verbose=0, proj_code=None, groupdir=None):
        """The process is expected to time out given timing estimates."""
        self.message = f'Scan requires minimum {required} - current {current}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'ExpectTimeoutError'

class ProjectCodeError(KerchunkException):
    def __init__(self, verbose=0, proj_code=None, groupdir=None):
        """Could not find the correct project code from the list of project codes for this run."""
        self.message = f'Project Code Extraction Failed'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'ProjectCodeError'

class FilecapExceededError(KerchunkException):
    def __init__(self, nfiles=0, verbose=0, proj_code=None, groupdir=None):
        """During scanning, could not find suitable files within the set of files specified."""
        self.message = f'Filecap exceeded: {nfiles} files attempted'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'FilecapExceededError'

class ChunkDataError(KerchunkException):
    def __init__(self, verbose=0, proj_code=None, groupdir=None):
        """Overflow Error from pandas during decoding of chunk information, most likely caused by bad data retrieval."""
        self.message = f'Decoding resulted in overflow - received chunk data contains junk (attempted 3 times)'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'ChunkDataError'

class NoValidTimeSlicesError(KerchunkException):
    def __init__(self, message='Kerchunk', verbose=0, proj_code=None, groupdir=None):
        """Unable to find any time slices to test within the object."""
        self.message = f'No valid timeslices found for {message}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'NoValidTimeSlicesError'

class VariableMismatchError(KerchunkException):
    def __init__(self, missing={}, verbose=0, proj_code=None, groupdir=None):
        """During testing, variables present in the NetCDF file are not present in Kerchunk"""
        self.message = f'Missing variables {missing} in Kerchunk file'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'VariableMismatchError'

class ShapeMismatchError(KerchunkException):
    def __init__(self, var={}, first={}, second={}, verbose=0, proj_code=None, groupdir=None):
        """Shapes of ND arrays do not match between Kerchunk and Xarray objects - when using a subset of the Netcdf files."""
        self.message = f'Kerchunk/NetCDF mismatch for variable {var} with shapes - K {first} vs N {second}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'ShapeMismatchError'

class TrueShapeValidationError(KerchunkException):
    def __init__(self, message='Kerchunk', verbose=0, proj_code=None, groupdir=None):
        """Shapes of ND arrays do not match between Kerchunk and Xarray objects - when using the complete set of files."""
        self.message = f'Kerchunk/NetCDF mismatch with shapes using full dataset - check logs'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'TrueShapeValidationError'

class NoOverwriteError(KerchunkException):
    def __init__(self, verbose=0, proj_code=None, groupdir=None):
        """Output file already exists and the process does not have forceful overwrite (-f) set."""
        self.message = 'Output file already exists and forceful overwrite not set.'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'NoOverwriteError'

class MissingKerchunkError(KerchunkException):
    def __init__(self, message="No suitable kerchunk file found for validation.", verbose=0, proj_code=None, groupdir=None):
        """Kerchunk file not found."""
        self.message = message
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'MissingKerchunkError'

class ValidationError(KerchunkException):
    def __init__(self, message="Fatal comparison failure for Kerchunk/NetCDF", verbose=0, proj_code=None, groupdir=None):
        """One or more checks within validation have failed - most likely elementwise comparison of data."""
        self.message = message
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'ValidationError'

class SoftfailBypassError(KerchunkException):
    def __init__(self, message="Kerchunk validation failed softly with no bypass - rerun with bypass flag", verbose=0, proj_code=None, groupdir=None):
        """Validation could not be completed because some arrays only contained NaN values which cannot be compared."""
        self.message = message
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def __str__(self):
        return 'SoftfailBypassError'
