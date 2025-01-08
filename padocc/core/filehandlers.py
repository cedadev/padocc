__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import json
import os
import yaml
from datetime import datetime
import logging
from typing import Iterator
from typing import Optional, Union
import xarray as xr

from padocc.core import LoggedOperation, FalseLogger
from .utils import format_str


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
            verbose=verbose)
        
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
        ):

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
        Private method to hard reset the filepath
        """

        components = '/'.join(filepath.split("/"))
        self._dir = components[:-2]
        filename  = components[-1]

        self._file, self._extension = filename.split('.')

class ListFileHandler(FileIOMixin):
    """
    Filehandler for string-based Lists in Padocc
    """

    def __init__(
            self, 
            dir: str, 
            filename: str,
            extension: Union[str,None] = None,
            init_value: Union[list, None] = None,
            **kwargs) -> None:
        
        super().__init__(dir, filename, **kwargs)

        self._value: list    = init_value or []
        self._extension: str = extension or 'txt'

    def append(self, newvalue: str) -> None:
        """Add a new value to the internal list"""
        self._obtain_value()
        
        self._value.append(newvalue)

    def set(self, value: list) -> None:
        """
        Reset the value as a whole for this 
        filehandler.
        """
        self._value = list(value)

    def __contains__(self, item: str) -> bool:
        """
        Check if the item value is contained in
        this list."""
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
        Enables setting items in filehandlers 'fh[0] = 1'
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
    """JSON File handler for padocc config files."""

    def __init__(
            self, 
            dir: str, 
            filename: str, 
            conf: Union[dict,None] = None, 
            init_value: Union[dict,None] = None,
            **kwargs
        ) -> None:

        super().__init__(dir, filename, **kwargs)
        self._conf: dict  = conf or {}
        self._value: dict = init_value or {}
        self._extension: str = 'json'

    def set(self, value: dict) -> None:
        """
        Set the value of the whole dictionary.
        """
        self._value = dict(value)

    def __contains__(self, key: str):
        """
        Check if the dict for this filehandler
        contains this key."""
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
        """
        self._obtain_value()

        if index in self._value:
            return self._value[index]
        
        return None
    
    def create_file(self) -> None:
        """JSON files require entry of a single dict on creation"""
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
        Safe method to get a value from this filehandler
        """
        self._obtain_value()

        if index is None:
            return self._value

        return self._value.get(index, default)

    def __setitem__(self, index: str, value: str) -> None:
        """
        Enables setting items in filehandlers.
        Dict-based filehandlers accept string keys only.
        """
        self._obtain_value()

        if index in self._value:
            self._value[index] = value
    
    def _obtain_value(self, index: Union[str,None] = None) -> None:
        """
        Obtain the value for this filehandler.
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
        Add the download link to this Kerchunk File
        """
        self._obtain_value()

        for key in self._value.keys():
            if len(self._value[key]) == 3:
                if self._value[key][0][0] == sub:
                    self._value[key][0] = replace + self._value[key][0]

    def add_kerchunk_history(self, version_no: str) -> None:
        """
        Add kerchunk variables to the metadata for this dataset, including 
        creation/update date and version/revision number.
        """

        from datetime import datetime

        # Get current time
        attrs = self.get('refs',None)

        if attrs is None or not isinstance(attrs,str):
            raise ValueError(
                'Attribute "refs" not present in Kerchunk file'
            )

        # Format for different uses
        now = datetime.now()
        if 'history' in attrs:
            hist = attrs.get('history','')

            if type(hist) == str:
                hist = hist.split('\n')

            if 'Kerchunk' in hist[-1]:
                hist[-1] = 'Kerchunk file updated on ' + now.strftime("%D")
            else:
                hist.append('Kerchunk file created on ' + now.strftime("%D"))
            attrs['history'] = '\n'.join(hist)
        else:
            attrs['history'] = 'Kerchunk file created on ' + now.strftime("%D") + '\n'
        
        attrs['kerchunk_revision'] = version_no
        attrs['kerchunk_creation_date'] = now.strftime("%d%m%yT%H%M%S")
        
        self['refs'] = attrs

class GenericStore(LoggedOperation):
    """
    Filehandler for Generic stores in Padocc - enables Filesystem
    operations on component files.
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
            verbose  : int = 0
        ) -> None:

        self._parent_dir: str = parent_dir
        self._store_name: str = store_name
        self._extension: str = extension

        self._meta: JSONFileHandler = JSONFileHandler(
            self.store_path, metadata_name)

        self._dryrun: bool   = dryrun
        self._forceful: bool = forceful

        # All filehandlers are logged operations
        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose)
        
    @property
    def store_path(self) -> str:
        """Assemble the store path"""
        return f'{self._parent_dir}/{self._store_name}.{self._extension}'

    def clear(self) -> None:
        """
        Remove all components of the store"""
        if not self._dryrun:
            os.system(f'rm -rf {self.store_path}')
        else:
            self.logger.debug(
                f'Skipped clearing "{self._extension}"-type '
                f'Store "{self._store_name}" in dryrun mode.'
            )

    def open(self, engine: str = 'zarr', **open_kwargs) -> xr.Dataset:
        """Open the store as a dataset (READ_ONLY)"""
        return xr.open_dataset(self.store_path, engine=engine,**open_kwargs)

    def __contains__(self, key: str) -> bool:
        """
        Check if a key exists in the zattrs file"""
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
        """Get an attribute from the zarr store"""
        return self._meta[index]
    
    def __setitem__(self, index: str, value: str) -> None:
        """Set an attribute in the zarr store"""
        self._meta[index] = value

class ZarrStore(GenericStore):
    """
    Filehandler for Zarr stores in PADOCC.
    Enables manipulation of Zarr store on filesystem
    and setting metadata attributes."""

    def __init__(
            self,
            parent_dir: str,
            store_name: str,
            **kwargs
        ) -> None:

        super().__init__(parent_dir, store_name, **kwargs)

    def __repr__(self) -> str:
        """Programmatic representation"""
        return f'<PADOCC ZarrStore: {format_str(self._store_name,10)}>'
    
    def open(self, *args, **zarr_kwargs) -> xr.Dataset:
        """
        Open the ZarrStore as an xarray dataset
        """
        return super().open(engine='zarr',**zarr_kwargs)

class KerchunkStore(GenericStore):
    """
    Filehandler for Kerchunk stores using parquet
    in PADOCC. Enables setting metadata attributes and
    will allow combining stores in future.
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
    
    def open(self, *args, **parquet_kwargs) -> xr.Dataset:
        """
        Open the Parquet Store as an xarray dataset
        """
        raise NotImplementedError

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

        self._extra_path = extra_path
        super().__init__(dir, filename, **kwargs)

        self._extension = 'log'

    @property
    def file(self) -> str:
        return f'{self._extra_path}{self._file}.{self._extension}'

class CSVFileHandler(ListFileHandler):
    """CSV File handler for padocc config files"""
    description = "CSV File handler for padocc config files"
    
    def __init__(
            self, 
            dir: str, 
            filename: str, 
            **kwargs
        ) -> None:

        super().__init__(dir, filename, **kwargs)

        self._extension = 'csv'

    def __iter__(self) -> Iterator[str]:
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