__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import json
import os
import logging
import traceback

from typing import Optional, Union

def error_handler(
        err : Exception, 
        logger: logging.Logger, 
        phase: str,
        dryrun: bool = False,
        subset_bypass: bool = False,
        jobid: Optional[str] = None,
        status_fh: Optional[object] = None
    ):

    """
    This function should be used at top-level loops over project codes ONLY - 
    not within the main body of the package.

    1. Single slurm job failed - raise Error
    2. Single serial job failed - raise Error
    3. One of a set of tasks failed - print error for that dataset as traceback.
    """

    def get_status(tb: list) -> str:
        status = 'Failed - NoLogGiven'
        for j in range(1, len(tb)):
            index = (j*-1)
            if tb[index]:
                status = 'Failed - ' + tb[index].split(':')[0]
                break
        return status

    try:
        raise err
    except Exception:
        tb = traceback.format_exc().split('\n')

        if hasattr(err, 'get_str'):
            status = err.get_str()
        else:
            status = get_status(tb)

    if status_fh is not None:
        status_fh.update_status(phase, status, jobid=jobid)

    if subset_bypass:
        logger.error(tb)
        return status
    else:
        raise err


class KerchunkException(Exception):
    def __init__(self, proj_code: Union[str,None], groupdir: Union[str,None]) -> None:
        self.proj_code = proj_code
        self.groupdir  = groupdir
        if hasattr(self, 'message'):
            msg = getattr(self,'message')
        super().__init__(msg)

class PartialDriverError(KerchunkException):
    """All drivers failed (NetCDF3/Hdf5/Tiff) for one or more files within the list"""
    def __init__(
            self,
            filenums: Union[int,None] = None, 
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f"All drivers failed when performing conversion for files {filenums}"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'PartialDriverError'

class NaNComparisonError(KerchunkException):
    """When comparing NaN values between objects - different values found"""
    def __init__(
            self, 
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f"NaN values do not match between comparison objects"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'NaNComparisonError'

class RemoteProtocolError(KerchunkException):
    """All drivers failed (NetCDF3/Hdf5/Tiff) for one or more files within the list"""
    def __init__(
            self,
            filenums: Union[int,None] = None, 
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = f"All drivers failed when performing conversion for files {filenums}"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'PartialDriverError'

class KerchunkDriverFatalError(KerchunkException):
    """All drivers failed (NetCDF3/Hdf5/Tiff) - run without driver bypass to assess the issue with each driver type."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = "All drivers failed when performing conversion"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'KerchunkDriverFatalError'

class IdenticalVariablesError(KerchunkException):
    """All variables found to be suitably identical between files as to not stack or concatenate"""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = "All variables are identical across files"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'IdenticalVariablesError'
    
class XKShapeToleranceError(KerchunkException):
    """Attempted validation using a tolerance for shape mismatch on concat-dims, shape difference exceeds tolerance allowance."""
    def __init__(
            self,
            tolerance: int = 0, 
            diff: int = 0,
            dim: str = '',
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f"Shape difference ({diff}) exceeds allowed tolerance ({tolerance}) for dimension ({dim})"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'XKShapeToleranceError'

class BlacklistProjectCode(KerchunkException):
    """The project code you are trying to run for is on the list of project codes to ignore."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = 'Project Code listed in blacklist for bad data - will not be processed.'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'BlacklistProjectCode'

class MissingVariableError(KerchunkException):
    """A variable is missing from the environment or set of arguments."""
    def __init__(
            self,
            vtype: str = "$",
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'Missing variable: {vtype}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'MissingVariableError'

class ExpectTimeoutError(KerchunkException):
    """The process is expected to time out given timing estimates."""
    def __init__(
            self,
            required: int = 0,
            current: str = '',
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'Scan requires minimum {required} - current {current}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ExpectTimeoutError'
    
class ExpectMemoryError(KerchunkException):
    """The process is expected to run out of memory given size estimates."""
    def __init__(
            self,
            required: int = 0,
            current: str = '',
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'Scan requires minimum {required} - current {current}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ExpectTimeoutError'

class ProjectCodeError(KerchunkException):
    """Could not find the correct project code from the list of project codes for this run."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'Project Code Extraction Failed'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ProjectCodeError'

class FilecapExceededError(KerchunkException):
    """During scanning, could not find suitable files within the set of files specified."""
    def __init__(
            self,
            nfiles: int = 0,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'Filecap exceeded: {nfiles} files attempted'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'FilecapExceededError'

class ChunkDataError(KerchunkException):
    """Overflow Error from pandas during decoding of chunk information, most likely caused by bad data retrieval."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'Decoding resulted in overflow - received chunk data contains junk (attempted 3 times)'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ChunkDataError'

class NoValidTimeSlicesError(KerchunkException):
    """Unable to find any time slices to test within the object."""
    def __init__(
            self,
            message: str = 'kerchunk',
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'No valid timeslices found for {message}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'NoValidTimeSlicesError'

class VariableMismatchError(KerchunkException):
    """During testing, variables present in the NetCDF file are not present in Kerchunk"""
    def __init__(
            self,
            missing: Union[dict, None] = None,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        missing = missing or {}

        self.message = f'Missing variables {missing} in Kerchunk file'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'VariableMismatchError'

class ShapeMismatchError(KerchunkException):
    """Shapes of ND arrays do not match between Kerchunk and Xarray objects - when using a subset of the Netcdf files."""
    def __init__(
            self, 
            var: Union[dict,None] = None,
            first: Union[dict,None] = None, 
            second: Union[dict,None] = None,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        var = var or {}
        first = first or {}
        second = second or {}

        self.message = f'Kerchunk/NetCDF mismatch for variable {var} with shapes - K {first} vs X {second}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ShapeMismatchError'

class TrueShapeValidationError(KerchunkException):
    """Shapes of ND arrays do not match between Kerchunk and Xarray objects - when using the complete set of files."""
    def __init__(
            self,
            message: str = 'kerchunk',
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = f'{message} mismatch with shapes using full dataset - check logs'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'TrueShapeValidationError'

class NoOverwriteError(KerchunkException):
    """Output file already exists and the process does not have forceful overwrite (-f) set."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = 'Output file already exists and forceful overwrite not set.'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'NoOverwriteError'

class MissingKerchunkError(KerchunkException):
    """Kerchunk file not found."""
    def __init__(
            self, 
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = "No suitable kerchunk file found."
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'MissingKerchunkError'

class ValidationError(KerchunkException):
    """One or more checks within validation have failed - most likely elementwise comparison of data."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = "Fatal Validation Error"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ValidationError'
    
class ComputeError(KerchunkException):
    """Compute stage failed - likely due to invalid config/use of the classes"""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = "Invalid configuration for the Compute stage"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ComputeError'

class SoftfailBypassError(KerchunkException):
    """Validation could not be completed because some arrays only contained NaN values which cannot be compared."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = "Kerchunk validation failed softly with no bypass - rerun with bypass flag"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'SoftfailBypassError'
    
class ConcatenationError(KerchunkException):
    """Variables could not be concatenated over time and are not duplicates - no known solution"""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = "Variables could not be concatenated over time and are not duplicates - no known solution"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ConcatenationError'
    
class ConcatFatalError(KerchunkException):
    """Chunk sizes differ between refs - files cannot be concatenated"""
    def __init__(
            self, 
            var: Union[str,None] = None, 
            chunk1: Union[int,None] = None, 
            chunk2: Union[int,None] = None, 
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = f"Chunk sizes differ between refs for {var}: {chunk1} - {chunk2} - files cannot be concatenated"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ConcatFatalError'
    
class SourceNotFoundError(KerchunkException):
    """Source File could not be located."""
    def __init__(
            self,
            sfile: Union[str, None],
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = f"Source file could not be located: {sfile}"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'SourceNotFoundError'
    
# Potentially useful but currently unused.
class ArchiveConnectError(KerchunkException):
    """Connection to the CEDA Archive could not be established"""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = f"Connection verification to the CEDA archive failed - {proj_code}"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ArchiveConnectError'

class KerchunkDecodeError(KerchunkException):
    """Decoding of Kerchunk file failed - likely a time array issue."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.message = f"Decoding of Kerchunk file failed - likely a time array issue."
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'KerchunkDecodeError'
    
class FullsetRequiredError(KerchunkException):
    """This project must be validated using the full set of files."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        
        self.message = f"This project must be validated by opening the full set of files."
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'FullsetRequiredError'