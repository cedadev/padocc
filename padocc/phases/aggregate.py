__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

# Padocc prototype Kerchunk Aggregator
import json
import numpy as np
import fsspec
import base64
import math
import glob
from typing import Union
import warnings
import xarray as xr
import logging
import struct
import os

from obstore.store import from_url
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import KerchunkJSONParser, HDFParser, NetCDF3Parser
from virtualizarr.registry import ObjectStoreRegistry
from kerchunk.combine import MultiZarrToZarr

from padocc.core.filehandlers import KerchunkFile
from padocc.core.errors import MissingDataError, ConcatFatalError
from padocc.core.logs import FalseLogger, init_logger

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification",
    category=UserWarning,
)

MAPPINGS = {
    '>f8':'>d',
    '<f8':'<d',
    '>f4':'>f',
    '<f4':'<f',
    '>i4':'>i',
    '<i4':'<i'
}

def virtualise(cache_dir: str, output_file: str, agg_dims: list, data_vars: list, nfiles: int, logger) -> None:

    logger.info('VirtualiZarr: Starting Concatenation')

    # Must be ordered in terms of mathematical position i.e 0,1,2 not 0,1,10,...
    cachefiles = []
    for i in range(nfiles):
        if not os.path.isfile(f'{cache_dir}/{i}.json'):
            raise MissingDataError(
                f'Cache file {i} not find'
            )
        cachefiles.append(f'{cache_dir}/{i}.json')
    cachekerchunk = [cf for cf in cachefiles if 'temp' not in cf]

    file_path = cachekerchunk[0]
    file_url = f"file://{file_path}"

    store = from_url("file://")
    registry = ObjectStoreRegistry({"file://": store})
    registry.register(file_url, store)
    parser = KerchunkJSONParser()

    vds = []
    for f in cachekerchunk:
        logger.info(f'VirtualiZarr: Parsing virtual dataset - {f}')
        try:
            vds.append(open_virtual_dataset(
                url=f,
                parser=parser,
                registry=registry
            ))
        except Exception as err:
            raise ValueError('Kerchunk Parsing failed')

    logger.info('VirtualiZarr: Combining Datasets')
    logger.debug(f'VirtualiZarr: Combining with agg_dims: {agg_dims}, data_vars: {data_vars}')
    logger.debug('VirtualiZarr: Coords: minimal, compat: override, combine_attrs: override')

    try:
        combined_vds = xr.combine_nested(vds, concat_dim=agg_dims, data_vars=data_vars, coords='minimal',compat='override', combine_attrs='override')
    except:
        raise ValueError('Kerchunk Concatenation failed.')

    logger.debug('VirtualiZarr: Virtualising combined dataset')
    try:
        combined_vds.virtualize.to_kerchunk(output_file, format='json')
    except:
        raise ValueError('Kerchunk serialisation failed.')

def mzz_combine(refs: list, output_file: str, concat_dims: list, identical_dims: list, zattrs: dict, fileset: list) -> None:
    """
    Combine kerchunk references using standard Kerchunk method.
    """

    # Calculate std dev for refs.
    #ref_std = np.std([len(r['refs'].keys()) for r in refs])

    try:
        mzz = MultiZarrToZarr(list(refs), concat_dims=concat_dims, identical_dims=identical_dims).translate()
    except ValueError as err:
        if 'chunk size mismatch' in str(err):
            raise ConcatFatalError(var=','.join(concat_dims))
        else:
            raise err
        
    files_in_kerchunk = {}
    for r, v in mzz['refs'].items():
        if len(v) == 3 and isinstance(v, list):
            if files_in_kerchunk.get(v[0]):
                files_in_kerchunk[v[0]] += 1
            else:
                files_in_kerchunk[v[0]] = 1

    #main_std = np.std([v for v in files_in_kerchunk.values()])

    # Unimplemented Standard Deviation calculation as this is uncertain.

    #if np.abs((main_std)-(ref_std)) > 5:
    #    raise MissingDataError(f'Unreasonable chunk std dev. difference {main_std:.1f}, expected {ref_std:.1f}')

    files_in_kerchunk = list(files_in_kerchunk.keys())

    # Compare std dev files to refs. Allow diff.

    for f in fileset:
        if f not in files_in_kerchunk:
            raise MissingDataError(f'File {f} missing')
        
    if zattrs is not None:
        mzz['refs']['.zattrs'] = json.dumps(zattrs)

    if isinstance(output_file, KerchunkFile):
        output_file.set(mzz)
        output_file.save()
    else:
        with open(output_file,'w') as f:
            f.write(json.dumps(mzz))

# Because at this point PADOCC does everything else, why not go one step further?

def check_chunk_sizes(var, arr, logger: logging.Logger) -> Union[None,list]:

    ndims = arr[0].get('chunks',[])
    if isinstance(ndims,list):
        ndims = len(ndims)
    else:
        ndims = 1

    for dim in range(ndims):
        chunksizes = np.array([a.get('chunks',0)[dim] for a in arr])
        shapes = np.array([a.get('shape',0)[dim] for a in arr])

        # Defunct - this fix did not yield useful results
        # if np.any(chunksizes > shapes):
        #     # 1.4.3 Fix - Adjust chunk sizes manually
        #     chunksizes[chunksizes > shapes] = shapes[chunksizes > shapes]
        #     # Unpack adjustments
        #     for i in range(len(chunksizes)):
        #         arr[i]['chunks'][dim] = chunksizes[i].item()

        if len(set(chunksizes)) > 1:
            logger.debug(set(chunksizes))
            raise ValueError(f'ConcatFatalError: {var}')
    return arr

def process_identical_vars(identical_dim_zarrays: dict, logger: logging.Logger):
    """
    Process identical vars. Once chunk sizes have been validated, simply take first
    value from identical variables.
    """
    refs_to_output = {}

    for var in identical_dim_zarrays.keys():

        logger.info(f'PADOCC-A: Identical Dimension: {var}')
        check_chunk_sizes(var, identical_dim_zarrays[var], logger)
        identical_dim_zarrays[var] = identical_dim_zarrays[var][0]

        refs_to_output[f'{var}/.zarray'] = identical_dim_zarrays[var]
    return refs_to_output

def process_agg_dims(agg_dim_zarrays: dict, logger: logging.Logger, ideal: int = 1000):
    """
    Process aggregation dimensions
    
    Catch uneven dtypes across the aggregation dimensions, allow for rechunking in b64.
    """
    chunk_bounds    = {}
    agg_dim_rechunk = {}
    agg_dim_index   = {}

    refs_to_output = {}

    for dim in agg_dim_zarrays.keys():

        logger.info(f'PADOCC-A: Aggregation Dimension: {dim}')
        
        arr = agg_dim_zarrays[dim]

        dtypes = set([a['dtype'] for a in arr])
        if len(dtypes) > 1:
            raise NotImplementedError('Varying aggregation dtypes not supported in PADOCC Aggregator yet.')

        arr = check_chunk_sizes(dim, arr, logger)

        standard_rechunk = True
        size_sum = 0
        chunk_count = []
        for a in arr:
            chunks = a['shape'][0]/a['chunks'][0]
            standard_rechunk = standard_rechunk and chunks%1==0
            chunk_count.append(int(chunks))

        arr = arr[0]
        chunk_bounds[dim] = [0] + list(np.cumsum(chunk_count))

        if standard_rechunk:
            logger.debug('PADOCC-A: Standard rechunking enabled')
            # Only able to combine existing chunks, not able to rearrange without difficulty.
            nchunks = chunk_bounds[dim][-1]
            comb_opts = [1]
            comb = 1
            while nchunks/comb > ideal:
                comb *= 2
                # Only allow equal combinations of chunks.
                if (nchunks/comb)%1==0:
                    comb_opts.append(comb)

            comb = comb_opts[-1]
            logger.debug(f'PADOCC-A: Combining chunks in groups of {comb}')
            if comb != 1:
                agg_dim_rechunk[dim] = comb
                arr['chunks'][0] = arr['chunks'][0] * comb
        else:
            if size_sum < 250000:
                logger.debug('PADOCC-A: Combined rechunking enabled')
                arr['chunks'] = size_sum

        # Shape is relative to chunksize
        arr['shape'] = [int(chunk_bounds[dim][-1]*arr['chunks'][0])] # Reset shape size for agg dims.
        agg_dim_zarrays[dim] = arr

        logger.info(f'PADOCC-A: {dim} Shape: {arr["shape"]}, Chunk Size: {arr["chunks"]}')

        agg_dim_index[dim] = {dim: 0}

        refs_to_output[f'{dim}/.zarray'] = agg_dim_zarrays[dim]

    return refs_to_output, chunk_bounds, agg_dim_rechunk, agg_dim_index

def process_agg_vars(
        agg_var_zarrays: dict, 
        agg_dim_zarrays: dict, 
        agg_dim_index: dict, 
        agg_dims: list, 
        example_ref: dict,
        b64vars: list, 
        logger: logging.Logger,
        ideal: int = 1000):

    """
    Process aggregation variables.
    
    Enable rechunking etc"""
    
    refs_to_output = {}
    agg_var_rechunk = {}
    agg_var_chunk_bounds = {}

    for var in agg_var_zarrays.keys():
        logger.info(f'PADOCC-A: Aggregation variable: {var}')
        arr = agg_var_zarrays[var]
        check_chunk_sizes(var, arr, logger)

        ndims = len(arr[0]['chunks'])

        standard_rechunk = True
        size_sum = 0
        chunk_count = []
        for a in arr:
            chunks = a['shape'][0]/a['chunks'][0]
            logger.debug(f'PADOCC-A: {var}: {len(chunk_count)} - {chunks}')
            standard_rechunk = standard_rechunk and chunks%1==0

            chunk_count.append(int(chunks))
            size_sum += a['shape'][0]

        chunk_bounds = [0] + list(np.cumsum(chunk_count))

        if ndims == 1 and var in b64vars:
            # Suitable for base64 rechunking.
            logger.debug('PADOCC-A: Suitable for base64 encoding.')

            # Rechunking by combining existing chunks in predictable way.
            if standard_rechunk:
                logger.debug('PADOCC-A: Standard rechunking enabled.')
                nchunks = chunk_bounds[-1]
                comb = 1
                while nchunks/comb > ideal:
                    comb += 1
                arr = arr[0]
                if comb != 1:
                    agg_var_rechunk[var] = comb
                    arr['chunks'] = [arr['chunks'][0] * comb]
            else:
                # Unable to rechunk in standard manner.
                if size_sum < 250000: # Mostly arbitrary limit for dimension size
                # Able to rechunk as a whole.
                    logger.debug('PADOCC-A: Combined rechunking enabled.')
                    agg_var_rechunk[var] = nchunks
                    arr = arr[0]
                    arr['chunks'] = size_sum
                    # Size set later from agg_dim_zarrays
                else:
                    logger.debug('PADOCC-A: No rechunking available.')
                    # Unable to rechunk at all
                    arr = arr[0]

        else:
            logger.debug('PADOCC-A: Not encoding with base64.')
            # Base64 viable but does not meet conditions
            arr = arr[0]

        # Only for 1d aggregations
        agg_var_chunk_bounds[var] = {
            agg_dims[0]: chunk_bounds
        }

        agg_indices = {}
        for dim in agg_dims:
            dim_ord = json.loads(example_ref['refs'][f'{var}/.zattrs'])['_ARRAY_DIMENSIONS'].index(dim)
            agg_indices[dim] = dim_ord
            arr['shape'][dim_ord] = agg_dim_zarrays[dim]['shape'][0]

        agg_dim_index[var] = agg_indices

        logger.info(f'PADOCC-A: {var} Shape: {arr["shape"]}, Chunk Size: {arr["chunks"]}')

        refs_to_output[f'{var}/.zarray'] = arr

    return refs_to_output, agg_var_rechunk, agg_dim_index, agg_var_chunk_bounds

def apply_rechunking(rechunk_cache: dict, agg_dim_rechunk: dict, logger: logging.Logger):

    refs_to_output = {}

    for dim, coords in rechunk_cache.items():
        logger.debug(f'PADOCC-A: Rechunking {dim}: {len(coords.keys())} by {len(coords["/0"])}')
        for coord, b64set in coords.items():

            combined_b64 = b64set[0]

            # Fill the last chunk with fillvalues?
            if len(b64set) < agg_dim_rechunk[dim]:
                # Method deemed unreliable
                raise ValueError('Unsupported chunk/shape size for rechunking')
                #b64set.append(b64set[-1])

            for b64 in b64set[1:]:
                combined_b64 += b64
            refs_to_output[f'{dim}{coord}'] = (b'base64:' + base64.b64encode(combined_b64)).decode()

    return refs_to_output

def remap_key(
        key: str, 
        file_ord: int,  
        agg_dim_rechunk: dict,
        label_dim_index: dict, 
        chunk_bounds: dict, 
        rechunk_cache: dict,
        data: Union[bytes,None],
        logger: logging.Logger
    ):
    
    array_label = key.split('/')[0]

    chunk_coords = [int(c) for c in key.split('/')[1].split('.')]
    for dim, index in label_dim_index.items():
        chunk_coords[index] += chunk_bounds[dim][file_ord]

    if array_label in agg_dim_rechunk and data is not None:
        rechunk_coord = math.floor(chunk_coords[0]/agg_dim_rechunk[array_label])
        
        if f'/{rechunk_coord}' not in rechunk_cache[array_label]:
            rechunk_cache[array_label][f'/{rechunk_coord}'] = []

        rechunk_cache[array_label][f'/{rechunk_coord}'].append(data)
        # This value is not yet added to the mzz combined dict.
        return None, rechunk_cache

    new_key = f'{array_label}/{".".join([str(c) for c in chunk_coords])}'
    return new_key, rechunk_cache

def _b64_summ(data: bytes, npdtype: str, b64_nth_value_set: list, file_ord: int):
    """
    Add offset from all previous b64 values.

    Defunct Additive Encoding function:
     - Discovered b64 encoding differences due to unit conversion requirements.
     - Decided not to implement unit conversion any further.
    """
    sdtype = MAPPINGS.get(npdtype, None)
    if sdtype is None:
        raise ValueError(
            f'Unable to decode unknown mapping: {npdtype}'
        )
    
    sum_offset = 0
    for v in b64_nth_value_set[:file_ord]:
        if v is not None:
            sum_offset += struct.unpack(sdtype, v)[0]
    value = struct.unpack(sdtype, data)[0] + sum_offset
    return struct.pack(sdtype, value)

def padocc_combine(
        ordered_refs: list[dict],
        native_files: list, 
        agg_dims: list, 
        agg_vars: list, 
        output_file: Union[str,None] = None,
        identical_vars: Union[list,None] = None, 
        zattrs: Union[dict,None] = None,
        b64vars: Union[list,None] = None,
        logger: Union[None, logging.Logger] = None
    ) -> Union[None,dict]:
    """
    Will only support existing aggregation dimensions for now.

    Variables to be encoded to base64.
     - Aggregation dimensions (check against size constraints for rechunking.)
     - Chunks under threshold.
     - Specific variables via b64 vars.
    """

    if logger is None:
        logger = init_logger(2, 'padocc_aggregator')

    if len(agg_dims) != 1:
        raise NotImplementedError(
            'Multi-dimensional aggregation not supported'
        )

    logger.info("PADOCC-A: Starting PADOCC aggregator")

    # Always attempt b64 encoding for agg dims
    b64vars = b64vars or []
    b64vars        = list(set(b64vars + agg_dims))
    identical_vars = identical_vars or []
    zattrs         = (zattrs or {}) | {'aggregation':'padocc'}

    combined_zattrs = ordered_refs[0]['refs']['.zattrs']
    if isinstance(combined_zattrs,str):
        combined_zattrs = json.loads(combined_zattrs)
    combined_zattrs.update(zattrs)

    mzz = {
        'version': ordered_refs[0]['version'],
        'refs': {
            '.zgroup': ordered_refs[0]['refs']['.zgroup'],
            '.zattrs': combined_zattrs
        }
    }

    # Initial values, where shape must be updated.
    agg_dim_zarrays       = {dim: [json.loads(r['refs'][f'{dim}/.zarray']) for r in ordered_refs] for dim in agg_dims}
    agg_var_zarrays       = {var: [json.loads(r['refs'][f'{var}/.zarray']) for r in ordered_refs] for var in agg_vars}

    pure_dims = {}
    # Pure dimensions do not have associated dim zarrays.
    identical_dim_zarrays = {}
    for var in identical_vars:
        for r in ordered_refs:
            zarrays = []
            if f'{var}/.zarray' in r['refs']:
                zarrays.append(json.loads(r['refs'][f'{var}/.zarray']))
            else:
                pure_dims[var] = 1

            if zarrays != []:
                identical_dim_zarrays[var] = zarrays

    for v in agg_dims + agg_vars + identical_vars:
        if v in pure_dims.keys():
            continue

        units = [json.loads(r['refs'][f'{v}/.zattrs']).get('units',None) for r in ordered_refs]
        if len(set(units)) > 1:
            raise NotImplementedError(
                'Unit conversion is not implemented in the PADOCC Aggregator'
            )

    # Process Identical Variables/Dimensions
    refs_to_output = process_identical_vars(identical_dim_zarrays, logger)
    for k, v in refs_to_output.items():
        mzz['refs'][k] = v

    # Process Aggregation Dimensions
    refs_to_output, chunk_bounds, agg_dim_rechunk, agg_dim_index = process_agg_dims(agg_dim_zarrays, logger)
    for k, v in refs_to_output.items():
        mzz['refs'][k] = v

    # Process Aggregation Variables
    refs_to_output, agg_var_rechunk, agg_dim_index, agg_var_chunk_bounds = process_agg_vars(agg_var_zarrays, agg_dim_zarrays, 
                                                    agg_dim_index, agg_dims,
                                                    ordered_refs[0], b64vars, logger)
    for k, v in refs_to_output.items():
        mzz['refs'][k] = v

    b64_encode = list(agg_dim_rechunk.keys()) + list(agg_var_rechunk.keys())

    rechunk_cache = {a:{} for a in agg_dim_rechunk.keys()} | {a:{} for a in agg_var_rechunk.keys()}

    #b64_0th_coords, b64_0th_values = {},{}
    #for v in b64_encode:
    #    coords = ".".join(['0' for i in aggregation_zarrays[v][0]["chunks"]])
    #    b64_0th_coords[v] = f'{v}/{coords}' # Extracts e.g time/0.0.0 for all b64 variables.

    #b64_nth_values = {v: [0 for i in range(len(ordered_refs))] for v in b64_encode}

    #additive_encoding = False
    for file_ord, ref in enumerate(ordered_refs):
        logger.info(f'PADOCC-A: Encoding file: {file_ord+1}/{len(ordered_refs)}')
        infile = None

        for key, value in ref['refs'].items():
            data = None

            # Lazy combine attrs - take first value (will update from common zattrs)
            if '.zattrs' in key or '.zarray' in key or '.zgroup' in key:
                if key not in mzz['refs']:
                    mzz['refs'][key] = value
                continue

            array_label = key.split('/')[0]
            if array_label in b64_encode:

                # Data Encoding - function

                if infile is None:
                    readfile = native_files[file_ord]
                    fs, path = fsspec.core.url_to_fs(readfile)
                    infile = fs.open(path, 'rb')

                infile.seek(int(value[1]))
                data = infile.read(int(value[2]))

                # Additive Encoding - Failed attempt
                #b64_nth_values[array_label][file_ord] = data

                #if key == b64_0th_coords[array_label] and array_label not in b64_0th_values:
                 #   b64_0th_values[array_label] = data

                #elif b64_0th_values[array_label] is not None and data == b64_0th_values[array_label]:
                #    additive_encoding = True
                    
                #if additive_encoding:
                 #   data = _b64_summ(data, agg_dim_zarrays[array_label]['dtype'], b64_nth_values[array_label], file_ord)

                value = (b'base64:' + base64.b64encode(data)).decode()

                # End Data Encoding

            if array_label in identical_vars:
                new_key = key
            elif array_label in agg_dims or array_label in agg_vars:
                if array_label in agg_dims:
                    rechunk_set = agg_dim_rechunk
                    bounds      = chunk_bounds
                else:
                    rechunk_set = agg_var_rechunk
                    bounds      = agg_var_chunk_bounds[array_label]

                # Remap key to determine new position
                new_key, rechunk_cache = remap_key(key, file_ord, rechunk_set,
                                                   agg_dim_index.get(array_label,{}), 
                                                   bounds, rechunk_cache, data, logger)
                # In the case of rechunking, do not assign value in this loop.
                if new_key is None:
                    continue
                
                if new_key in mzz['refs']:
                    raise ValueError(f'Overlapping chunk sections for key: {new_key}')
            mzz['refs'][new_key] = value
            if new_key == 'time_bounds/482119.0':
                pass

    # Apply collected rechunking
    refs_to_output = apply_rechunking(rechunk_cache, agg_dim_rechunk | agg_var_rechunk, logger)
    for k, v in refs_to_output.items():
        mzz['refs'][k] = v
            
    if output_file is not None:
        with open(output_file, 'w') as f:
            f.write(json.dumps(mzz))
    else:
        return mzz
