__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

class MissingVariableError(Exception):
    def __init__(self, type='$', verbose=0):
        self.message = f'Missing variable: {type}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class ExpectTimeoutError(Exception):
    def __init__(self, required=0, current='', verbose=0):
        self.message = f'Scan requires minimum {required} - current {current}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class ProjectCodeError(Exception):
    def __init__(self, verbose=0):
        self.message = f'Project Code Extraction Failed'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class FilecapExceededError(Exception):
    def __init__(self, nfiles=0, verbose=0):
        self.message = f'Filecap exceeded: {nfiles} files attempted'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class ExpectTimeoutError(Exception):
    def __init__(self, required=0, current='', verbose=0):
        self.message = f'Scan requires minimum {required} - current {current}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class ChunkDataError(Exception):
    def __init__(self, verbose=0):
        self.message = f'Decoding resulted in overflow - received chunk data contains junk (attempted 3 times)'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class NoValidTimeSlicesError(Exception):
    def __init__(self, message='Kerchunk', verbose=0):
        self.message = f'No valid timeslices found for {message}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class VariableMismatchError(Exception):
    def __init__(self, missing={}, verbose=0):
        self.message = f'Missing variables {missing} in Kerchunk file'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class ShapeMismatchError(Exception):
    def __init__(self, var={}, first={}, second={}, verbose=0):
        self.message = f'Kerchunk/NetCDF mismatch for variable {var} with shapes - K {first} vs N {second}'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class TrueShapeValidationError(Exception):
    def __init__(self, message='Kerchunk', verbose=0):
        self.message = f'Kerchunk/NetCDF mismatch with shapes using full dataset - check logs'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class NoOverwriteError(Exception):
    def __init__(self, verbose=0):
        self.message = 'Output file already exists and forceful overwrite not set.'
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class MissingKerchunkError(Exception):
    def __init__(self, message="No suitable kerchunk file found for validation.", verbose=0):
        self.message = message
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class ValidationError(Exception):
    def __init__(self, message="Fatal comparison failure for Kerchunk/NetCDF", verbose=0):
        self.message = message
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

class SoftfailBypassError(Exception):
    def __init__(self, message="Kerchunk validation failed softly with no bypass - rerun with bypass flag", verbose=0):
        self.message = message
        super().__init__(self.message)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
