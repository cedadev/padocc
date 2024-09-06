## Tool for scanning a netcdf file or set of netcdf files for kerchunkability

# Determine total number of netcdf chunks in first file
# Determine number of netcdf files

# Calculate total number of chunks and output

__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr
import os, sys
from datetime import datetime
import math
import json
import numpy as np
import re

from padocc.core import FalseLogger
from padocc.core.errors import *
from padocc import ProjectOperation, ComputeOperation

from padocc.operations.filehandlers import JSONFileHandler

def _format_float(value: int, logger=FalseLogger) -> str:
    """Format byte-value with proper units"""
    logger.debug(f'Formatting value {value} in bytes')
    if value:
        unit_index = 0
        units = ['','K','M','G','T','P']
        while value > 1000:
            value = value / 1000
            unit_index += 1
        return f'{value:.2f} {units[unit_index]}B'
    else:
        return None
    
def _safe_format(value: int, fstring: str) -> str:
    """Attempt to format a string given some fstring template.
    - Handles issues by returning '', usually when value is None initially."""
    try:
        return fstring.format(value=value)
    except:
        return ''
    
def _get_seconds(time_allowed: str) -> int:
    """Convert time in MM:SS to seconds"""
    if not time_allowed:
        return 10000000000
    mins, secs = time_allowed.split(':')
    return int(secs) + 60*int(mins)

def _format_seconds(seconds: int) -> str:
    """Convert time in seconds to MM:SS"""
    mins = int(seconds/60) + 1
    if mins < 10:
        mins = f'0{mins}'
    return f'{mins}:00'

def _perform_safe_calculations(std_vars: list, cpf: list, volms: list, nfiles: int, logger) -> tuple:
    """
    Perform all calculations safely to mitigate errors that arise during data collation.

    :param std_vars:        (list) A list of the variables collected, which should be the same across
                            all input files.

    :param cpf:             (list) The chunks per file recorded for each input file.

    :param volms:           (list) The total data size recorded for each input file.

    :param nfiles:          (int) The total number of files for this dataset

    :param logger:          (obj) Logging object for info/debug/error messages.

    :returns:   Average values of: chunks per file (cpf), number of variables (num_vars), chunk size (avg_chunk),
                spatial resolution of each chunk assuming 2:1 ratio lat/lon (spatial_res), totals of NetCDF and Kerchunk estimate
                data amounts, number of files, total number of chunks and the addition percentage.
    """
    kchunk_const = 167 # Bytes per Kerchunk ref (standard/typical)
    if std_vars:
        num_vars = len(std_vars)
    else:
        num_vars = None
    if not len(cpf) == 0:
        avg_cpf = sum(cpf)/len(cpf)
    else:
        logger.warning('CPF set as none, len cpf is zero')
        avg_cpf = None
    if not len(volms) == 0:
        avg_vol = sum(volms)/len(volms)
    else:
        logger.warning('Volume set as none, len volumes is zero')
        avg_vol = None
    if avg_cpf:
        avg_chunk = avg_vol/avg_cpf
    else:
        avg_chunk = None
        logger.warning('Average chunks is none since CPF is none')
    if num_vars and avg_cpf:
        spatial_res = 180*math.sqrt(2*num_vars/avg_cpf)
    else:
        spatial_res = None

    if nfiles and avg_vol:
        netcdf_data = avg_vol*nfiles
    else:
        netcdf_data = None

    if nfiles and avg_cpf:
        total_chunks = avg_cpf * nfiles
    else:
        total_chunks = None

    if avg_chunk:
        addition = kchunk_const*100/avg_chunk
    else:
        addition = None

    type = 'JSON'
    if avg_cpf and nfiles:
        kerchunk_data = avg_cpf * nfiles * kchunk_const
        if kerchunk_data > 500e6:
            type = 'parq'
    else:
        kerchunk_data = None

    return avg_cpf, num_vars, avg_chunk, spatial_res, netcdf_data, kerchunk_data, total_chunks, addition, type

class ScanOperation(ProjectOperation):

    def __init__(self, 
                 proj_code,
                 workdir,
                 groupID=None, 
                 dryrun=True,
                 forceful=False,
                 **kwargs
                 ):
        super().__init__(proj_code, workdir, groupID=groupID, dryrun=dryrun, forceful=forceful, **kwargs)

    @classmethod
    def help(cls):
        print('Not set up yet!')

    def run(self, mode='kerchunk') -> None:
        """Main process handler for scanning phase"""

        self.logger.debug(f'Assessment for {self.proj_code}')

        nfiles = len(self.allfiles)

        if nfiles < 3:
            self.detail_cfg = {'skipped':True}
            return None
        

        # Create all files in mini-kerchunk set here. Then try an assessment.
        limiter = int(nfiles/20)
        limiter = max(2, limiter)
        limiter = min(100, limiter)

        self.logger.info(f'Determined {limiter} files to scan (out of {nfiles})')

        scanners = {
            'zarr':self.scan_zarr,
            'kerchunk':self.scan_kerchunk
        }

        if mode in scanners:
            scanners[mode](limiter=limiter)

    def scan_kerchunk(self, limiter=None):
        """
        Function to perform scanning with output Kerchunk format.
        """
        self.logger.info('Starting scan process for Kerchunk cloud format')

        success = True

        # Redo this processor call.
        mini_ds = ComputeOperation(
            self.proj_code,
            workdir=self.workdir, 
            groupID=self.groupID,
            thorough=True, forceful=True, # Always run from scratch forcefully to get best time estimates.
            verbose=self._verbose, logid='0',
            limiter=limiter)

        try:
            mini_ds.create_refs()
            if not mini_ds.success:
                success = False
        except ConcatFatalError as err:
            return False
        
        escape, is_varwarn, is_skipwarn = False, False, False
        cpf, volms = [],[]

        std_vars   = None
        std_chunks = None
        ctypes   = []
        ctype    = None
        
        self.logger.info(f'Summarising scan results for {self.limiter} files')
        for count in range(self.limiter):
            try:
                volume, chunks_per_file, varchunks, ctype = self._summarise_json(count)
                vars = sorted(list(varchunks.keys()))

                # Keeping the below options although may be redundant as have already processed the files
                if not std_vars:
                    std_vars = vars
                if vars != std_vars:
                    self.logger.warning(f'Variables differ between files - {vars} vs {std_vars}')
                    is_varwarn = True

                if not std_chunks:
                    std_chunks = varchunks
                for var in std_vars:
                    if std_chunks[var] != varchunks[var]:
                        raise ConcatFatalError(var=var, chunk1=std_chunks[var], chunk2=varchunks[var])

                cpf.append(chunks_per_file)
                volms.append(volume)
                ctypes.append(ctype)

                self.info(f'Data recorded for file {count+1}')
            except Exception as err:
                raise err
            
        timings = {
            'convert_time' : mini_ds.convert_time,
            'concat_time'  : mini_ds.concat_time,
            'validate_time': mini_ds.validate_time
        }

        self._compile_outputs(std_vars, cpf, volms, timings, 
            ctypes, escape=escape, is_varwarn=is_varwarn, is_skipwarn=is_skipwarn
        )

    def scan_zarr(self, limiter=None):
        """
        Function to perform scanning with output Zarr format.
        """

        self.logger.info('Starting scan process for Zarr cloud format')

        # Need a refactor
        mini_ds = ZarrDSRechunker(
            args.proj_code,
            workdir=args.workdir, 
            thorough=True, forceful=True, # Always run from scratch forcefully to get best time estimates.
            version_no='trial-', verb=args.verbose, logid='0',
            groupID=args.groupID, limiter=limiter, logger=logger, dryrun=args.dryrun,
            mem_allowed='500MB')

        mini_ds.create_store()
        
        # Most of the outputs are currently blank as summaries don't really work well for Zarr.

        timings = {
            'convert_time' : mini_ds.convert_time,
            'concat_time'  : mini_ds.concat_time,
            'validate_time': mini_ds.validate_time
        }
        self._compile_outputs(
            mini_ds.std_vars, mini_ds.cpf, mini_ds.volm, timings,
            [], override_type='zarr')

    def _summarise_json(self, identifier) -> tuple:
        """
        Open previously written JSON cached files and perform analysis.
        """

        if type(identifier) == dict:
            # Assume refs passed directly.
            kdict = identifier['refs']
        else:

            fh_kwargs = {
                'dryrun':self._dryrun,
                'forceful':self._forceful,
            }

            fh = JSONFileHandler(self.dir, f'cache/{identifier}.json', self.logger, **fh_kwargs)
            kdict = fh['refs']

            self.logger.debug(f'Starting Analysis of references for {identifier}')

        if not kdict:
            return None, None, None, None

        # Perform summations, extract chunk attributes
        sizes  = []
        vars   = {}
        chunks = 0

        for chunkkey in kdict.keys():
            if bool(re.search(r'\d', chunkkey)):
                try:
                    sizes.append(int(kdict[chunkkey][2]))
                except ValueError:
                    pass
                chunks += 1
                continue

            if '/.zarray' in chunkkey:
                var = chunkkey.split('/')[0]
                chunksize = 0
                if var not in vars:
                    if type(kdict[chunkkey]) == str:
                        chunksize = json.loads(kdict[chunkkey])['chunks']
                    else:
                        chunksize = dict(kdict[chunkkey])['chunks']
                    vars[var] = chunksize

        return np.sum(sizes), chunks, vars

    def _compile_outputs(self, std_vars, cpf, volms, timings, ctypes, escape=None, is_varwarn=None, is_skipwarn=None, override_type=None):

        self.logger.info('Summary complete, compiling outputs')
        (avg_cpf, num_vars, avg_chunk, 
        spatial_res, netcdf_data, kerchunk_data, 
        total_chunks, addition, type) = _perform_safe_calculations(std_vars, cpf, volms, self.allfiles.get(), self.logger)

        details = {
            'netcdf_data'      : _format_float(netcdf_data, logger=self.logger), 
            'kerchunk_data'    : _format_float(kerchunk_data, logger=self.logger), 
            'num_files'        : self.allfiles.get(),
            'chunks_per_file'  : _safe_format(avg_cpf,'{value:.1f}'),
            'total_chunks'     : _safe_format(total_chunks,'{value:.2f}'),
            'estm_chunksize'   : _format_float(avg_chunk, logger=self.logger),
            'estm_spatial_res' : _safe_format(spatial_res,'{value:.2f}') + ' deg',
            'timings'        : {
                'convert_estm'   : timings['convert_time'],
                'concat_estm'    : timings['concat_time'],
                'validate_estm'  : timings['validate_time'],
                'convert_actual' : None,
                'concat_actual'  : None,
                'validate_actual': None,
            },
            'variable_count'   : num_vars,
            'variables'        : std_vars,
            'addition'         : _safe_format(addition,'{value:.3f}') + ' %',
            'var_err'          : is_varwarn,
            'file_err'         : is_skipwarn,
            'type'             : type
        }

        if escape:
            details['scan_status'] = 'FAILED'

        if len(set(ctypes)) == 1:
            details['driver'] = ctypes[0]

        if override_type:
            details['type'] = override_type

        existing_details = self.detail_cfg.get()
        if existing_details:
            for entry in details.keys():
                if details[entry]:
                    existing_details[entry] = details[entry]
        else:
            existing_details = details

        self.detail_cfg.set(existing_details)
        self.detail_cfg.save_file()

def scan_config(
        proj_code,
        workdir,
        groupID=None,
        logger=None, 
        mode='kerchunk',
        **kwargs) -> None:
    """
    Configure scanning and access main section, ensure a few key variables are set
    then run scan_dataset.
    
    :param args:        (obj) Set of command line arguments supplied by argparse.

    :param logger:      (obj) Logging object for info/debug/error messages. Will create a new 
                        logger object if not given one.

    :param fh:          (str) Path to file for logger I/O when defining new logger.

    :param logid:       (str) If creating a new logger, will need an id to distinguish this logger
                        from other single processes (typically n of N total processes.)

    :returns:   None
    """

    so = ScanOperation(proj_code, workdir, groupID=groupID, logger=logger, **kwargs)
    so.run(mode=mode)

if __name__ == '__main__':
    print('Kerchunk Pipeline Config Scanner - run using master scripts')