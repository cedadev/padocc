__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import json
import logging
import os
import traceback
from typing import Optional, Union


def error_handler(
        err : Exception, 
        logger: logging.Logger, 
        phase: str,
        subset_bypass: bool = False,
        jobid: Optional[str] = None,
        status_fh: Optional[object] = None,
        agg_shorthand: str = '',
    ) -> str:

    """
    This function should be used at top-level loops over project codes ONLY - 
    not within the main body of the package.

    1. Single slurm job failed - raise Error
    2. Single serial job failed - raise Error
    3. One of a set of tasks failed - print error for that dataset as traceback.

    :param err:     (Exception) Error raised within some part of the pipeline.

    :param logger:  (logging.Logger) Logging operator for any messages.

    :param subset_bypass:   (bool) Skip raising an error if this operation
        is part of a sequence.

    :param jobid:   (str) The ID of the SLURM job if present.

    :param status_fh:   (object) Padocc Filehandler to update status.
    """

    def get_status(tb: list) -> str:
        status = 'Failed-NoLogGiven'
        for j in range(1, len(tb)):
            index = (j*-1)
            if tb[index]:
                status = 'Failed-' + tb[index].split(':')[0]
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

    # Quick aggregation shorthand
    status = agg_shorthand + status

    if status_fh is not None:
        status_fh.update_status(phase, status, jobid=jobid)
        status_fh.save()

    logger.error('\n'.join(tb))
    if subset_bypass:
        return 'Fatal'
    else:
        raise err

def worst_error(report: dict, bypass: dict = None) -> str:
    """
    Determine the worst error level and return as a string.
    """

    error = None

    bypass = bypass or {}
    
    if 'report' in report:
        report = report['report']

    priority = {
        'size_errors':'Fatal',
        'dim_size_errors':"Fatal", 
        'data_errors':"Fatal", 
        'dim_errors':"Fatal",
        'bypassed':"Warn",
        'dtype/precision':"Warn"
    }

    # Check all data issues that would be fatal.
    if 'data' in report:
        section = report['data']
        
        # Improvements are possible.
        vars = section.get('variables',{})
        dims = section.get('dimensions', {})
        for err, result in priority.items():
            for v in vars.get(err,{}).keys():
                try:
                    _ = bypass['data']['variables'][err][v]
                    report['data']['variables'][err][v]['SKIP'] = True
                except KeyError:
                    error = error or f'{result}-{err}'
                except TypeError:
                    report['data']['variables'][err][v] = report['data']['variables'][err][v] + '_SKIP'

            for d in dims.get(err,{}).keys():
                try:
                    _ = bypass['data']['dimensions'][err][d]
                    report['data']['dimensions'][err][d]['SKIP'] = True
                except KeyError:
                    error = error or f'{result}-{err}'
                except TypeError:
                    report['data']['dimensions'][err][d] = report['data']['dimensions'][err][d] + '_SKIP'

    if 'metadata' not in report:
        return err, report
    
    vars = report['metadata'].get('variables',None)
    if vars is not None:
        for etype in ['missing','order']:
            for k, v in vars.items():
                if v['type'] == etype:

                    try:
                        # error type not equal to bypass - error cannot be bypassed.
                        assert etype != bypass['metadata']['variables'][k]['type']
                        err = err or f'Warn-{k}_{etype}'
                    except AssertionError:
                        # Etype is equal to bypass - can bypass this error
                        report['metadata']['variables'][k]['SKIP'] = True
                    except KeyError:
                        # Error not in bypass - error cannot be bypassed
                        error = error or f'Warn-{k}_{etype}'
    
    dims = report['metadata'].get('dims', None)
    if dims is not None:
        for etype in ['order']:
            for k, v in dims.items():
                if v['type'] == etype:
                    try:
                        # error type not equal to bypass - error cannot be bypassed.
                        assert etype != bypass['metadata']['dims'][k]['type']
                        error = error or f'Warn-{k}_{etype}'
                    except AssertionError:
                        # Etype is equal to bypass - can bypass this error
                        report['metadata']['dims'][k]['SKIP'] = True
                    except KeyError:
                        # Error not in bypass - error cannot be bypassed
                        error = error or f'Warn-{k}_{etype}'
    
    attrs = report['metadata'].get('attributes',{})
    for attr_parent, attrset in attrs.items():
        for title, issue in attrset.items():
            etype = issue['type']
            try:
                # error type not equal to bypass - error cannot be bypassed.
                assert (etype != bypass['metadata']['attributes'][attr_parent].get(title,{}).get('type','') and not bypass['metadata']['attributes'][attr_parent].get('skip_all',False))
                error = error or f'Warn-{attr_parent}_{title}_{etype}'
            except AssertionError:
                # Etype is equal to bypass - can bypass this error
                report['metadata']['attributes'][attr_parent][title]['SKIP'] = True
            except KeyError:
                # Error not in bypass - error cannot be bypassed
                error = error or f'Warn-{attr_parent}_{title}_{etype}'
    return error, report
        
class KerchunkException(Exception):
    """
    General Exception type.
    """
    def __init__(self, proj_code: Union[str,None], groupdir: Union[str,None]) -> None:
        self.proj_code = proj_code
        self.groupdir  = groupdir
        if hasattr(self, 'message'):
            msg = getattr(self,'message')
        super().__init__(msg)

class PartialDriverError(KerchunkException): # Keep
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
    
class MissingDataError(KerchunkException): # Keep
    """Data missing from kerchunk product"""
    def __init__(
            self,
            reason,
            verbose: int = 0,
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'Data missing: {reason} - Kerchunk product incomplete'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'MissingDataError'

class AggregationError(KerchunkException): # Keep
    """Aggregation dimension(s) are not properly arranged"""
    def __init__(
            self,
            agg_dims,
            verbose: int = 0,
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = f'Aggregation is incorrectly ordered for dimension(s): {agg_dims}'
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'AggregationError'

class KerchunkDriverFatalError(KerchunkException): # Keep
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

class MissingVariableError(KerchunkException): # Keep
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

class ChunkDataError(KerchunkException): # Keep
    """Overflow Error from pandas during decoding of chunk information, most likely caused by bad data retrieval."""
    def __init__(
            self,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:
        self.message = 'Decoding resulted in overflow - received chunk data contains junk (attempted 3 times)'
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

class MissingKerchunkError(KerchunkException): # Keep
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
            report_err: Union[str,None] = None,
            verbose: int = 0, 
            proj_code: Union[str,None] = None, 
            groupdir: Union[str,None] = None
        ) -> None:

        self.err_msg = report_err

        self.message = self.err_msg
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'

    def get_str(self):
        return 'ValidationError'
    
class ComputeError(KerchunkException): # Keep
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
    
class ConcatFatalError(KerchunkException): # Keep
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

        self.message = f"Chunk sizes differ between refs for {var}"
        if chunk1 is not None: 
            self.message += f"{chunk1} - {chunk2}"
        self.message += " - files cannot be concatenated"
        super().__init__(proj_code, groupdir)
        if verbose < 1:
            self.__class__.__module__ = 'builtins'
    def get_str(self):
        return 'ConcatFatalError'
    
class SourceNotFoundError(KerchunkException): # Keep
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

class KerchunkDecodeError(KerchunkException): # Keep
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