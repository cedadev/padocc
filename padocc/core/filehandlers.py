__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import json
import logging
import os
import re
import glob
from datetime import datetime
from typing import Iterator, Optional, Union, Any
import netCDF4

import fsspec
import xarray as xr
import yaml

from .errors import ChunkDataError, KerchunkDecodeError
from .logs import FalseLogger, LoggedOperation
from .utils import format_str, extract_json


class FileIOMixin(LoggedOperation):
    """
    Class for containing Filehandler behaviour which is exactly identical
    for all Filehandler subclasses.

    Identical behaviour
    -------------------

    1. Contains:
        'item' in fh

    2. Create/save file:

    Filehandlers intrinsically know the file they are attached to so there are
    no attributes passed to either of these.

        fh.create_file()
        fh.close()

    3. Get/set:

        contents = fh.get()
        fh.set(contents)
    
    """

    def __init__(
            self, 
            dir : str, 
            filename : str, 
            logger   : Optional[Union[logging.Logger,FalseLogger]] = None, 
            label    : Union[str,None] = None,
            fh       : Optional[str] = None,
            logid    : Optional[str] = None,
            dryrun   : bool = False,
            forceful : bool = False,
            thorough : bool = False,
            verbose  : int = 0
        ) -> None:
        """
        Generic filehandler for PADOCC operations involving file I/O operations.

        :param dir:     (str) The path to the directory in which this file can be found.

        :param filename: (str) The name of the file on the filesystem.

        :param logger:      (logging.Logger | FalseLogger) An existing logger object.

        :param label:       (str) The label to apply to the logger object.

        :param fh:          (str) Path to logfile for logger object generated in this specific process.

        :param logid:       (str) ID of the process within a subset, which is then added to the name
            of the logger - prevents multiple processes with different logfiles getting loggers confused.

        :param forceful:        (bool) Continue with processing even if final output file 
            already exists.

        :param dryrun:          (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.

        :param verbose:     (int) Level of verbosity for log messages (see core.init_logger).

        :returns: None
        """
        
        self._dir: str   = dir
        self._file: str = filename

        self._dryrun: bool   = dryrun
        self._forceful: bool = forceful
        self._extension: str = ''

        # All filehandlers are logged operations
        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose,
            dryrun=dryrun,
            forceful=forceful,
            thorough=thorough)
        
    @property
    def filepath(self) -> str:
        """
        Returns the full filepath attribute.
        """
        return f'{self._dir}/{self.file}'

    @property
    def file(self) -> str:
        """
        Returns the full filename attribute."""
        return f'{self._file}.{self._extension}'

    def file_exists(self) -> bool:
        """
        Return true if the file is found.
        """
        return os.path.isfile(self.filepath)

    def create_file(self) -> None:
        """
        Create the file if not on dryrun.
        """
        if not self._dryrun:
            self.logger.debug(f'Creating file "{self.file}"')
            os.system(f'touch {self.filepath}')
        else:
            self.logger.info(f'DRYRUN: Skipped creating "{self.file}"')

    def remove_file(self) -> None:
        """
        Remove the file on the filesystem
        if not on dryrun
        """
        if not self._dryrun:
            self.logger.debug(f'Deleting file "{self.file}"')
            os.system(f'rm {self.filepath}')
        else:
            self.logger.info(f'DRYRUN: Skipped deleting "{self.file}"')

    def move_file(
            self,
            new_dir: str,
            new_name: Union[str,None] = None,
            new_extension: Union[str, None] = None
        ) -> None:
        """
        Migrate the file to a new location.

        :param new_dir:     (str) New directory for filehandler being moved.

        :param new_name:    (str) New name for filehandler if required.

        :param new_extension:   (str) New extension if required (e.g. changing
            log-type).
        """

        if not os.access(new_dir, os.W_OK):
            raise OSError(
                f'Specified directory "{new_dir}" is not writable'
            )
        
        old_path = str(self.filepath)
        self._dir = new_dir
        
        if new_name is not None:
            self._file = new_name

        if new_extension is not None:
            self._extension = new_extension
        try:
            os.system(f'mv {old_path} {self.filepath}')
            self.logger.debug(
                f'Moved file successfully from {old_path} to {self.filepath}'
            )
        except OSError as err:
            self.__set_filepath(old_path)
            raise err
        
    def __set_filepath(self, filepath) -> None:
        """
        Private method to hard reset the filepath.

        :param filepath: (str) Reset dir and filename via a single filepath.
        """

        components = '/'.join(filepath.split("/"))
        self._dir = components[:-2]
        filename  = components[-1]

        self._file, self._extension = filename.split('.')

class ListFileHandler(FileIOMixin):
    """
    Filehandler for string-based Lists in Padocc.

    List Behaviour
    --------------

    1. Append - works the same as with normal lists.
    2. Pop - remove a specific value (works as normal).
    3. Contains - (x in y) works as normal.
    4. Length - (len(x)) works as normal.
    5. Iterable - (for x in y) works as normal.
    6. Indexable - (x[0]) works as normal

    Added behaviour
    ---------------

    1. Close - close and save the file.
    2. Get/Set - Get or set the whole value.
    """

    def __init__(
            self, 
            dir: str, 
            filename: str,
            extension: Union[str,None] = None,
            init_value: Union[list, None] = None,
            **kwargs) -> None:
        
        """
        Initialisation for list filehandlers.

        :param dir:     (str) The path to the directory in which this file can be found.

        :param filename: (str) The name of the file on the filesystem.
        
        :param extension: (str) Extension to apply to this handler, if not default txt.
        
        :param init_value:  (list) Initial value to apply to this filehandler.
        """
        
        
        super().__init__(dir, filename, **kwargs)

        self._value: list    = init_value or []
        self._extension: str = extension or 'txt'

    def append(self, newvalue: Union[str,list]) -> None:
        """
        Add a new value to the internal list.

        :param newvalue:    (str|list) New value to append to current list.
        """
        self._obtain_value()

        if isinstance(newvalue, list):
            newvalue = ','.join(newvalue)
        
        self._value.append(newvalue)

    def remove(self, oldvalue: str) -> None:
        """
        Remove a value from the internal list
        
        :param oldvalue:    (str) Remove past value from list."""
        self._obtain_value()
        
        self._value.remove(oldvalue)

    def set(self, value: list[str,list]) -> None:
        """
        Reset the value as a whole for this 
        filehandler.

        :param value:   (list) Reset the ``_value`` property for this filehandler
            to the new value.
        """
        if len(value) == 0:
            self.logger.warning(f'No value given to ListFileHandler {self.filepath}')
            return

        if isinstance(value[0],list):
            value = [','.join(v) for v in value]

        self._value = list(value)

    def __contains__(self, item: str) -> bool:
        """
        Check if the item value is contained in this list.
        
        :param item:    (str) Index item from list filehandler.
        """
        self._obtain_value()

        return item in self._value

    def __str__(self) -> str:
        """String representation"""
        self._obtain_value()

        return '\n'.join(self._value)
    
    def __repr__(self) -> str:
        """Programmatic representation"""
        return f"<PADOCC List Filehandler: {format_str(self.file,10, concat=True)}>"
    
    def __len__(self) -> int:
        """Length of value"""
        self._obtain_value()

        self.logger.debug(f'content length: {len(self._value)}')
        return len(self._value)
    
    def __iter__(self) -> Iterator[str]:
        """Iterator for the set of values"""
        self._obtain_value()

        for i in self._value:
            if i is not None:
                yield i

    def __getitem__(self, index: int) -> str:
        """
        Override FileIOMixin class for getting index

        :param index:   (int) Index item by position.
        """
        self._obtain_value()

        return self._value[index]
    
    def get(self) -> list:
        """
        Get the current value
        """
        self._obtain_value()

        return self._value
    
    def __setitem__(self, index: int, value: str) -> None:
        """
        Enables setting items in filehandlers 'fh[0] = 1'.

        :param index:   (int) Set item by position in list.

        :param value:   (str) New value to set for item at position.
        """
        self._obtain_value()

        self._value[index] = value

    def _obtain_value(self) -> None:
        """
        Obtain the value for this filehandler.
        """
        if self._value == []:
            self._obtain_value_from_file()

    def _obtain_value_from_file(self) -> None:
        """
        Obtain the value specifically from
        the represented file
        """
        if not self.file_exists():
            self.create_file()

        with open(self.filepath) as f:
            self._value = [r.strip() for r in f.readlines()]

    def _set_value_in_file(self) -> None:
        """
        On initialisation or close, set the value
        in the file.
        """
        if self._dryrun or self._value == []:
            self.logger.debug(f"Skipped setting value in {self.file}")
            return

        if not self.file_exists():
            self.create_file()

        with open(self.filepath,'w') as f:
            f.write('\n'.join(self._value))

    def close(self) -> None:
        """
        Save the content of the filehandler
        """
        self._set_value_in_file()

class JSONFileHandler(FileIOMixin):
    """
    JSON File handler for padocc config files.

    Dictionary Behaviour
    --------------------

    1. Indexable - index by key (as normal)
    2. Contains - key in dict (as normal)
    3. Length - length of the key set (as normal)

    Added Behaviour
    ---------------

    1. Iterable - iterate over the keys.
    2. Get/set - get/set the whole value.
    3. Create_file - Specific for JSON files.

    """

    def __init__(
            self, 
            dir: str, 
            filename: str, 
            conf: Union[dict,None] = None, 
            init_value: Union[dict,None] = None,
            **kwargs
        ) -> None:
        """
        :param dir:     (str) The path to the directory in which this file can be found.

        :param filename: (str) The name of the file on the filesystem.
        
        :param conf:    (dict) Dictionary to apply as default values for this filehandler.
        
        :param init_value:  (list) Initial value to apply to this filehandler.
        """

        super().__init__(dir, filename, **kwargs)
        self._conf: dict  = conf or {}
        self._value: dict = init_value or {}
        self._extension: str = 'json'

    def set(self, value: dict) -> None:
        """
        Set the value of the whole dictionary.

        :param value:   (dict) New value to set for this filehandler.
        """
        self._value = dict(value)

    def __contains__(self, key: str):
        """
        Check if the dict for this filehandler contains this key.
        
        :param key:     (str) Key to check in this filehandlers dictionary value.
        """
        self._obtain_value()

        return key in self._value.keys()

    def __str__(self) -> str:
        """String representation"""
        self._obtain_value()

        return yaml.safe_dump(self._value,indent=2)

    def __repr__(self) -> str:
        """Programmatic representation"""
        return f"<PADOCC JSON Filehandler: {format_str(self.file,10, concat=True)}>"

    def __len__(self) -> int:
        """Returns number of keys in this dict-like object."""
        self._obtain_value()

        return len(self._value.keys())
    
    def __iter__(self) -> Iterator[str]:
        """Iterate over set of keys."""
        self._obtain_value()

        for i in self._value.keys():
            yield i

    def __getitem__(self, index: str) -> Union[str,dict,None]:
        """
        Enables indexing for filehandlers. 
        Dict-based filehandlers accept string keys only.

        :param index:   (str) Key to use for getting a value from the dictionary.
        """
        self._obtain_value()

        if index in self._value:
            return self._value[index]
        
        return None
    
    def pop(self, index: str, default: Any = None) -> Any:
        """
        Wrapper for ``pop`` function of a dict.
        """
        self._obtain_value()

        return self._value.pop(index, default)
    
    def create_file(self) -> None:
        """JSON files require entry of a single dict on creation."""
        super().create_file()

        if not self._dryrun:
            with open(self.filepath,'w') as f:
                f.write(json.dumps({}))
    
    def get(
            self, 
            index: Union[str,None] = None, 
            default: Union[str,None] = None
        ) -> Union[str,dict,None]:
        """
        Safe method to get a value from this filehandler.

        :param index:   (str) Key in dictionary.

        :param default: (str) Default value for this item in the dictionary.
        """
        self._obtain_value()

        if index is None:
            return self._value

        return self._value.get(index, default)

    def __setitem__(self, index: str, value: str) -> None:
        """
        Enables setting items in filehandlers.
        Dict-based filehandlers accept string keys only.

        :param index:   (str) Key in dictionary.

        :param value:   (str) value to set for this key.
        """
        self._obtain_value()
        self._value[index] = value
    
    def _obtain_value(self, index: Union[str,None] = None) -> None:
        """
        Obtain the value for this filehandler.

        :param index:   (str) Key in dictionary.
        """
        if self._value == {}:
            self._obtain_value_from_file()

        if index is None:
            return
        
        if self._conf is not None:
            if index in self._conf:
                self._apply_conf()

    def _obtain_value_from_file(self) -> None:
        """
        Obtain the value specifically from
        the represented file
        """
        if not self.file_exists():
            self.create_file()
            return

        with open(self.filepath) as f:
            self._value = json.load(f)

    def _set_value_in_file(self) -> None:
        """
        On initialisation or close, set the value
        in the file.
        """
        if self._dryrun or self._value == {}:
            self.logger.debug(f"Skipped setting value in {self.file}")
            return
        
        self._apply_conf()

        if not self.file_exists():
            self.create_file()

        with open(self.filepath,'w') as f:
            f.write(json.dumps(self._value))

    def _apply_conf(self) -> None:
        """
        Update value with properties from conf - fill
        missing values.
        """

        if self._conf is None:
            return
        
        self._conf.update(self._value)

        self._value = dict(self._conf)
        self._conf = {}

    def close(self) -> None:
        """
        Save the content of the filehandler
        """
        self._set_value_in_file()

class KerchunkFile(JSONFileHandler):
    """
    Filehandler for Kerchunk file, enables substitution/replacement
    for local/remote links, and updating content.
    """

    def add_download_link(
            self,
            sub: str = '/',
            replace: str = 'https://dap.ceda.ac.uk'
        ) -> None:
        """
        Add the download link to this Kerchunk File.

        :param sub:     (str) Substitution value to be replaced.

        :param replace: (str) Replacement value in download links.
        """
        self._obtain_value()

        for key in self._value.keys():
            if len(self._value[key]) == 3:
                if self._value[key][0][0] == sub:
                    self._value[key][0] = replace + self._value[key][0]

    def update_history(
            self, 
            addition: str,
            new_version: str,
        ) -> None:
        """
        Update the history with a new addition.
        
        Sets the new version/revision automatically.

        :param addition:    (str) Message to add to dataset history.

        :param new_version:  (str) Specific version number for the history
            entry being applied.
        """

        from datetime import datetime

        # Get current time
        attrs = self.get_meta()

        if attrs is None or not isinstance(attrs,str):
            raise ValueError(
                'Attribute "refs" not present in Kerchunk file'
            )
        
        attrs = attrs['.zattrs']

        now   = datetime.now()

        hist = attrs.get('history',[])
        if isinstance(hist, str):
            hist = hist.split('\n')
        hist.append(addition)

        attrs['history'] = '\n'.join(hist)
        attrs['padocc_revision'] = new_version
        attrs['padocc_last_changed'] = now.strftime("%d%m%yT%H%M%S")
        
        self.set_meta(attrs)

    def open_dataset(
            self, 
            fsspec_kwargs: Union[dict,None] = None,
            retry: bool = False,
            **kwargs) -> xr.Dataset:
        """
        Open the kerchunk file as a dataset
        
        :param fsspec_kwargs:   (dict) Kwargs applied to fsspec mapper.

        :param retry:   (bool) Unused property for multiple tries when searching for kerchunk
            dataset.
        """

        default_fsspec = {'target_options':{'compression':None}}
        if fsspec_kwargs is not None:
            default_fsspec.update(fsspec_kwargs)

        default_zarr = {'consolidated':False, 'decode_times':True}
        default_zarr.update(kwargs)

        self.logger.info('Attempting to open Kerchunk JSON file')
        try:
            mapper  = fsspec.get_mapper('reference://',fo=self.filepath, **default_fsspec)
        except json.JSONDecodeError as err:
            self.logger.error(f"Kerchunk file {self.filepath} appears to be empty")
            return None
        
        # Need a safe repeat here
        ds = None
        attempts = 0
        while attempts < 3 and not ds:
            attempts += 1
            try:
                ds = xr.open_zarr(mapper, **default_zarr)
            except OverflowError:
                ds = None
            except KeyError as err:
                if re.match('.*https.*',str(err)) and not retry:
                    # RemoteProtocol is not https - retry with correct protocol
                    self.logger.warning('Found KeyError "https" on opening the Kerchunk file - retrying with local filepaths.')
                    return self.open_dataset(fsspec_kwargs=default_fsspec, retry=True)
                else:
                    raise err
            except Exception as err:
                if 'decode' in str(err):
                    raise KerchunkDecodeError
                raise err #MissingKerchunkError(message=f'Failed to open kerchunk file {kfile}')
        if not ds:
            raise ChunkDataError
        self.logger.debug('Successfully opened Kerchunk with virtual xarray ds')
        return ds

    def get_meta(self) -> Union[dict,None]:
        """
        Obtain the metadata dictionary
        """
        self._obtain_value()

        refs = self._value.get('refs',{})
        zattrs = refs.get('.zattrs',None)
        
        if isinstance(zattrs, str):
            zattrs = json.loads(zattrs)

        return zattrs
    
    def set_meta(self, values: dict):
        """
        Reset the metadata dictionary.

        :param values:  (dict) Fully replace all zattrs in kerchunk dataset.
        """
        if 'refs' not in self._value:
            raise ValueError(
                'Cannot reset metadata for a file with no existing values.'
            )
        self._value['refs']['.zattrs'] = values

    def spawn_copy(self, copy: str):
        """
        Spawn a copy of this file (not filehandler)

        :param copy:    (str) Path to new copy location and filename (minus extension).
        """
        if self._dryrun:
            self.logger.info(f'[DRYRUN]: cp {self.filepath} {copy}.{self._extension}')
        else:
            os.system(f'cp {self.filepath} {copy}.{self._extension}')

class GenericStore(LoggedOperation):
    """
    Filehandler for Generic stores in Padocc - enables Filesystem
    operations on component files.

    Behaviours (Applies to Metadata)
    --------------------------------

    1. Length - length of metadata keyset
    2. Contains - metadata contains key (as with dict)
    3. Indexable - Get/set a specific property.
    4. Get/set_meta - Get/set the whole metadata set.
    5. Clear - clears all files in the store.

    """

    def __init__(
            self,
            parent_dir: str,
            store_name: str, 
            metadata_name: str = '.zattrs',
            extension: str = 'zarr',
            logger   : Optional[Union[logging.Logger,FalseLogger]] = None, 
            label    : Union[str,None] = None,
            fh       : Optional[str] = None,
            logid    : Optional[str] = None,
            dryrun   : bool = False,
            forceful : bool = False,
            thorough : bool = False,
            verbose  : int = 0
        ) -> None:
        """
        :param parent_dir:          (str) Directory to place this store.

        :param store_name:          (str) Name of this particular store.

        :param metadata_name:       (str) Stores contain multiple files including one metadata file, which
            is identified using its own filehandler.
        
        :param extension:           (str) Extension for this store, where different stores have different
            extensions.

        :param logger:              (logging.Logger) Logger supplied to this Operation.

        :param label:               (str) The label to apply to the logger object.

        :param fh:                  (str) Path to logfile for logger object generated in this specific process.

        :param logid:               (str) ID of the process within a subset, which is then added to the name
            of the logger - prevents multiple processes with different logfiles getting
            loggers confused.

        :param verbose:         (int) Level of verbosity for log messages (see core.init_logger).

        :param forceful:        (bool) Continue with processing even if final output file 
            already exists.

        :param dryrun:          (bool) If True will prevent output files being generated
            or updated and instead will demonstrate commands that would otherwise happen.

        :param thorough:        (bool) From args.quality - if True will create all files 
            from scratch, otherwise saved refs from previous runs will be loaded."""

        self._parent_dir: str = parent_dir
        self._store_name: str = store_name
        self._extension: str = extension

        self._meta: JSONFileHandler = JSONFileHandler(
            self.store_path, metadata_name)

        # All filehandlers are logged operations
        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose,
            forceful=forceful,
            dryrun=dryrun,
            thorough=thorough)

    def update_history(
            self,
            addition: str,
            new_version: str,
        ) -> None:
        """
        Update the history with a new addition.
        
        Sets the new version/revision automatically.

        :param addition:    (str) Message to add to dataset history.

        :param new_version: (str) New version the message applies to.
        """

        attrs = self._meta['refs']['.zattrs']
        now   = datetime.now()

        now   = datetime.now()

        hist = attrs.get('history',[])
        if isinstance(hist, str):
            hist = hist.split('\n')
        hist.append(addition)

        attrs['history'] = '\n'.join(hist)
        attrs['padocc_revision'] = new_version
        attrs['padocc_last_changed'] = now.strftime("%d%m%yT%H%M%S")

        self._meta['refs']['.zattrs'] = attrs
    
    def spawn_copy(self, copy: str):
        """
        Spawn a copy of this store (not filehandler)

        :param copy:    (str) New full path + name for external copy of the store (minus extension).
        """
        if self._dryrun:
            self.logger.info(f'[DRYRUN]: cp -R {self.store_path} {copy}.{self._extension}/')
        else:
            os.system(f'cp -R {self.store_path} {copy}.{self._extension}/')

    def close(self) -> None:
        """
        Close the meta filehandler for this store
        """
        if not self.is_empty:
            self._meta.close()

    @property
    def store_path(self) -> str:
        """Assemble the store path"""
        return f'{self._parent_dir}/{self._store_name}.{self._extension}'

    def clear(self) -> None:
        """
        Remove all components of the store
        """
        if not self._dryrun:
            os.system(f'rm -rf {self.store_path}')
        else:
            self.logger.debug(
                f'Skipped clearing "{self._extension}"-type '
                f'Store "{self._store_name}" in dryrun mode.'
            )

    @property
    def is_empty(self) -> bool:
        """
        Check if the store contains any data
        """
        if not os.path.exists(self.store_path):
            return True
        return len(os.listdir(self.store_path)) == 0

    def get_meta(self):
        """
        Obtain the metadata dictionary
        """
        return self._meta.get()
    
    def set_meta(self, values: dict):
        """
        Reset the metadata dictionary

        :param values:  (dict) Complete set of metadata for this store.
        """
        if 'refs' not in self._meta:
            raise ValueError(
                'Cannot reset metadata for a file with no existing values.'
            )
        self._meta.set(values)

    def __contains__(self, key: str) -> bool:
        """
        Check if a key exists in the metadata file.

        :param key: (str) Key to be checked in the metadata for this store.
        """
        return key in self._meta
    
    def __str__(self) -> str:
        """Return the string representation of the store"""
        return self.__repr__()
    
    def __len__(self) -> int:
        """Find the number of keys in zattrs""" 
        return len(self._meta)

    def __repr__(self) -> str:
        """Programmatic representation"""
        return f'<PADOCC Store: {format_str(self._store_name,10)}>'

    def __getitem__(self, index: str) -> Union[str,dict,None]:
        """
        Get an attribute from the zarr store
        
        :param index:   (str) Key in the metadata to attempt retrieval.
        """
        # Fail if improperly indexed with nonexistant key.
        return self._meta[index]
    
    def __setitem__(self, index: str, value: str) -> None:
        """
        Set an attribute in the zarr store
        
        :param index:   (str) Key in the metadata to attempt retrieval.
        
        :param value:   (str) Value to set for the key in the metadata.
        """
        # Fail if improperly indexed with nonexistant key.
        self._meta[index] = value

class ZarrStore(GenericStore):
    """
    Filehandler for Zarr stores in PADOCC.
    Enables manipulation of Zarr store on filesystem
    and setting metadata attributes.
    
    Added Behaviours
    ----------------
    
    1. Open dataset - open the zarr store.

    2. Write to s3 - write a disk-based zarr store to s3.
    """

    def __init__(
            self,
            parent_dir: str,
            store_name: str,
            remote_s3: Union[dict,None] = None,
            **kwargs
        ) -> None:

        if remote_s3:
            parent_dir = f's3://{remote_s3["bucket_id"]}'
            store_name = remote_s3["store_name"]

        self._remote_s3 = remote_s3

        super().__init__(
            parent_dir, 
            store_name, 
            metadata_name='.zattrs',
            extension='zarr',
            **kwargs)

    def __repr__(self) -> str:
        """Programmatic representation"""
        return f'<PADOCC ZarrStore: {format_str(self._store_name,10)}>'
    
    def get_meta(self) -> dict:
        """
        Override super function in case of remote s3.
        """

        if self._remote_s3 is not None:
            raise NotImplementedError
        
        return super().get_meta()

    @property
    def store(self) -> Union[str,object]:
        """
        Returns the store path or s3 store object as required.
        """
        if self._remote_s3 is None:
            return self.store_path
        
        # Optional extra kwargs for s3 connection.
        s3_kwargs = self._remote_s3.get('s3_kwargs',None)
        target = self.store_path

        self.logger.debug(f'Target store: {target}')
        # Internal s3 store created from known config
        return self._store(
            target,
            self._remote_s3['s3_credentials'],
            s3_kwargs,
        )

    def open_dataset(self, **zarr_kwargs) -> xr.Dataset:
        """
        Open the ZarrStore as an xarray dataset
        """
        return xr.open_dataset(self.store_path, engine='zarr', **zarr_kwargs)

    def write_to_s3(
            self, 
            credentials: Union[dict, str],
            bucket_id: str,
            name_overwrite: Union[str, None] = None,
            s3_kwargs: dict = None,
            ds: Union[xr.Dataset,None] = None,
            **zarr_kwargs):
        """
        Write zarr store to an S3 Object Store
        bucket directly from padocc
        """

        self.logger.info(f'Configuring s3 connection')

        if name_overwrite is not None:
            target = f'{bucket_id}/{name_overwrite}.{self._extension}'
        else:
            target = f'{bucket_id}/{self._store_name}.{self._extension}'

        self.logger.info(f'Writing to {target}')

        # Internal s3 store function
        s3_store = self._store(
            target,
            credentials,
            s3_kwargs
        )

        ds = ds or self.open_dataset(**zarr_kwargs)
        ds.to_zarr(store=s3_store, mode='w')

        self.logger.info(f'Zarr store {target} written.')

    def _store(
            self,
            target: str,
            s3_file_or_json: Union[str,dict],
            s3_kwargs: Union[dict,None] = None,
        ) -> object:
        """
        Internal private store object retriever.
        
        Takes all configuration parameters required 
        to access a writable store for this object.
        """

        try:
            import s3fs
        except ImportError:
            raise ValueError(
                "s3fs package not installed in your environment - please "
                "install with pip or otherwise."
            )

        # Optional extra kwargs for s3 connection.
        default_s3 = {'anon':False}
        s3_kwargs = s3_kwargs or {}
        default_s3.update(s3_kwargs)
        
        # Extract credentials for accessing the store.
        if isinstance(s3_file_or_json, str):
            creds = extract_json(s3_file_or_json)
        else:
            creds = s3_file_or_json

        self.logger.info(f'Connecting to {creds["endpoint_url"]}')

        # Create remote_s3 connection.
        remote_s3 = s3fs.S3FileSystem(
            secret = creds['secret'],
            key = creds['token'],
            client_kwargs = {'endpoint_url': creds['endpoint_url']},
            **default_s3
        )

        return s3fs.S3Map(target, s3=remote_s3)

class KerchunkStore(GenericStore):
    """
    Filehandler for Kerchunk stores using parquet
    in PADOCC. Enables setting metadata attributes and
    will allow combining stores in future.

    Added behaviours
    ----------------

    1. Open dataset - opens the kerchunk store.
    """

    def __init__(
            self,
            parent_dir: str,
            store_name: str,
            **kwargs
        ) -> None:

        super().__init__(
            parent_dir, store_name, 
            metadata_name='.zmetadata',
            extension='parq',
            **kwargs)

    def __repr__(self) -> str:
        """Programmatic representation"""
        return f'<PADOCC ParquetStore: {format_str(self._store_name,10)}>'
    
    def open_dataset(
            self, 
            rfs_kwargs: Union[dict,None] = None,
            **parquet_kwargs
        ) -> xr.Dataset:
        """
        Open the Parquet Store as an xarray dataset
        """
        self.logger.debug('Opening Kerchunk Parquet store')

        default_rfs = {
            'remote_protocol':'file',
            'target_protocol':'file',
            'lazy':True
        }
        if rfs_kwargs is not None:
            default_rfs.update(rfs_kwargs)

        default_parquet = {
            'backend_kwargs':{"consolidated": False, "decode_times": False}
        }
        default_parquet.update(parquet_kwargs)

        from fsspec.implementations.reference import ReferenceFileSystem
        fs = ReferenceFileSystem(
            self.filepath, 
            **default_rfs)
        
        return xr.open_dataset(
            fs.get_mapper(), 
            engine="zarr",
            **default_parquet
        )

class LogFileHandler(ListFileHandler):
    """Log File handler for padocc phase logs."""
    description = "Log File handler for padocc phase logs."

    def __init__(
            self, 
            dir: str, 
            filename: str, 
            extra_path: str = '',
            **kwargs
        ) -> None:

        """
        Initialisation of a log file retrievable via padocc.

        :param dir:     (str) The path to the directory in which this file can be found.

        :param filename: (str) The name of the file on the filesystem.
        
        :param extra_path:  (str) Extra directory structure to apply to the directory.
        """

        self._extra_path = extra_path
        super().__init__(dir, filename, **kwargs)

        self._extension = 'log'

    @property
    def filepath(self) -> str:
        """
        Returns the full filepath attribute.
        """
        return f'{self._dir}/{self._extra_path}{self.file}'

class CSVFileHandler(ListFileHandler):
    """CSV File handler for padocc config files"""
    description = "CSV File handler for padocc config files"
    
    def __init__(
            self, 
            dir: str, 
            filename: str, 
            **kwargs
        ) -> None:
        """
        Initialisation of the CSV filehandler.

        :param dir:     (str) The path to the directory in which this file can be found.

        :param filename: (str) The name of the file on the filesystem.
        """

        super().__init__(dir, filename, **kwargs)

        self._extension = 'csv'

    def __iter__(self) -> Iterator[str]:
        """
        Iterable for this dataset
        """
        self._obtain_value()

        for i in self._value:
            if i is not None:
                yield i.replace(' ','').split(',')

    def update_status(
            self, 
            phase: str, 
            status: str,
            jobid : str = '',
        ) -> None:

        """
        Update formatted status for this 
        log with the phase and status
        
        :param phase:   (str) The phase for which this project is being
            operated.
            
        :param status:  (str) The status of the current run 
            (e.g. Success, Failed, Fatal) 
        
        :param jobid:   (str) The jobID of this run if present.
        """

        status = status.replace(',', '.').replace('\n','.')
        addition = f'{phase},{status},{datetime.now().strftime("%H:%M %D")},{jobid}'
        self.append(addition)
        self.logger.info(f'Updated new status: {phase} - {status}')

class CFADataset(LoggedOperation):
    """
    Basic handler for CFA dataset

    Added behaviours
    ----------------

    1. Open dataset - opens the CFA dataset
    """

    def __init__(
            self, 
            filepath : str, 
            identifier: str,
            **kwargs
            ):
        """
        Initialisation of the CFA Dataset Filehandler.
        
        :param dir:     (str) The path to the directory in which this file can be found.

        :param filename: (str) The name of the file on the filesystem.
        """

        if 'CFA' not in xr.backends.list_engines():
            raise ImportError(
                'CFA Engine Module not found, see the documentation '
                'at https://github.com/cedadev/CFAPyX'
            )
        
        self._extension = 'nca'
        self._ident     = identifier
        self._filepath  = filepath
        self._meta      = None

        # All filehandlers are logged operations
        super().__init__(**kwargs)

        self._correct_existing_files()

    def update_history(
            self,
            addition: str,
            new_version: str,
        ) -> None:
        """
        Update the history with a new addition.
        
        Sets the new version/revision automatically.

        :param addition:    (str) Message to add to dataset history.

        :param new_version: (str) New version the message applies to.
        """

        attrs = self.get_meta()
        now   = datetime.now()

        hist = attrs.get('history',[])
        if isinstance(hist, str):
            hist = hist.split('\n')
        hist.append(addition)

        attrs['history'] = '\n'.join(hist)
        attrs['padocc_revision'] = new_version
        attrs['padocc_last_changed'] = now.strftime("%d%m%yT%H%M%S")

        self.set_meta(attrs)

    @property
    def filepath(self):
        return f'{self._filepath}.{self._extension}'

    def __str__(self) -> str:
        """String representation of CFA Dataset"""
        return f'<PADOCC CFA Dataset: {self._ident}>'
    
    def __repr__(self) -> str:
        """Programmatic representation of CFA Dataset"""
        return self.__str__()
    
    def __getitem__(self, index: str) -> Any:
        """
        Get an attribute from the metadata.
        
        :param index:   (str) Name of an attribute in the metadata.
        """
        self._load_meta()
        return self._meta[index]
    
    def __setitem__(self, index: str, value: Any) -> None:
        """
        Set an attribute within the metadata.

        :param index:   (str) Name of an attribute in the metadata.

        :param value:   (Any) New value to apply.
        """
        self._load_meta()
        self._meta[index] = value

    def _correct_existing_files(self) -> None:
        """
        Correction applied to pre-1.3.2 nca files.
        """

        dir = self.filepath.replace(
            self.filepath.split('/')[-1],
            ''
        )
        files = sorted(glob.glob(f'{dir}/*.{self._extension}'))
        if len(files) == 0:
            self.logger.debug('No 1.3.2 related file issues.')
            return
        if len(files) > 1:
            self.logger.warning(
                'Applying corrections to CFA datasets. The following files will be deleted:'
            )
            for f in files[1:]:
                self.logger.warning(f' > {f.split("/")[-1]}')
            self.logger.warning(f' -> {files[0].split("/")[-1]} will be preserved')
            inp = input('Accept changes? (Y/N): ')
            if inp != 'Y':
                raise ValueError(
                    'Changes not permitted to CFA Dataset - please '
                    f'remove files {files[1:]} to continue.'
                )
            
            if self._dryrun:
                self.logger.info(f'DRYRUN Skipped deleting CFA files: {files}')
                return

            for f in files[1:]:
                os.system(f'rm {f}')
            os.system(f'mv {files[0]} {self.filepath}')
        else:
            if files[0] != self.filepath:
                if self._dryrun:
                    self.logger.info(f'DRYRUN Skipped moving file {files[0]}')
                else:
                    os.system(f'mv {files[0]} {self.filepath}')

    def _load_meta(self) -> None:
        """
        Load the metadata for this filehandler."""
        if self._meta is not None:
            return

        self._meta = {}
        ds = netCDF4.Dataset(self.filepath)
        for nca in ds.ncattrs():
            self._meta[nca] = ds.getncattr(nca)
        ds.close()

    def close(self) -> None:
        """
        Set the meta attribute for this dataset.
        """
        if self._meta is None:
            return
        
        ds = netCDF4.Dataset(self.filepath, mode='w')
        for k, v in self._meta.items():
            ds.setncattr(k, v)
        ds.close()

    def get_meta(self) -> dict:
        """
        Get the metadata/attributes for this dataset.
        """
        self._load_meta()
        return self._meta
    
    def set_meta(self, new_value: dict) -> None:
        """
        Set the whole meta attribute for this dataset.

        :param new_value:   (dict) New metadata contents.
        """
        self._meta = new_value

    def spawn_copy(self, copy: str):
        """
        Spawn a copy of this file (not filehandler)

        :param copy:    (str) For the CFA filehandler, copy should be the full path
            to the new location, minus the extension. This should include the version number
            at the point of release.
        """
        if self._dryrun:
            self.logger.info(f'[DRYRUN]: cp {self.filepath} {copy}.{self._extension}')
        else:
            os.system(f'cp {self.filepath} {copy}.{self._extension}')

    def open_dataset(self, **kwargs) -> xr.Dataset:
        """Open the CFA Dataset [READ-ONLY]"""
        return xr.open_dataset(self.filepath, engine='CFA',**kwargs)