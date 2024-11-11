__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import json
import os
import yaml
from datetime import datetime
import logging
from typing import Generator

from padocc.core import LoggedOperation, FalseLogger

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
        fh.save_file()

    3. Get/set:

        contents = fh.get()
        fh.set(contents)
    
    """

    def __init__(
            self, 
            dir : str, 
            filename : str, 
            logger   : logging.Logger | FalseLogger = None, 
            label    : str = None,
            fh       : str = None,
            logid    : str = None,
            dryrun   : bool = None,
            forceful : bool = None,
            verbose  : int = 0
        ) -> None:
        """
        General filehandler for PADOCC operations involving file I/O operations.

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
        
        self.dir       = dir
        self.filename  = filename

        self._dryrun   = dryrun
        self._forceful = forceful
        self._value    = None
        self._file     = None

        self._set_file()

        # All filehandlers are logged operations
        super().__init__(
            logger,
            label=label,
            fh=fh,
            logid=logid,
            verbose=verbose)

    def __contains__(self, item) -> bool:
        """
        Enables checking 'if x in fh'.
        """
        if self._value is None:
            self._get_content()
        
        return item in self._value

    @property
    def filepath(self) -> str:
        """
        Returns the private file attribute.
        """
        return self._file

    def file_exists(self) -> bool:
        """
        Return true if the file is found.
        """
        return os.path.isfile(self._file)

    def create_file(self):
        """
        Create the file if not on dryrun.
        """
        if not self._dryrun:
            self.logger.debug(f'Creating file "{self._file}"')
            os.system(f'touch {self._file}')
        else:
            self.logger.info(f'DRYRUN: Skipped creating "{self._file}"')

    def save_file(self):
        """
        Wrapper for _set_content method
        """
        self.logger.debug(f'Saving file {self._file}')
        self._set_content()

    def set(self, value):
        """
        Reset the whole value of the private ``_value`` attribute.
        """
        self._check_value()

        self._value = value

    def get(self):
        """
        Get the value of the private ``_value`` attribute.
        """
        self._check_value()

        return self._value

    def _check_save(self) -> bool:
        """
        Returns true if content is able to be saved.
        """

        # Only set value if value has been loaded to edit.
        self._check_value()

        # Only set value if not doing a dryrun
        if self._dryrun:
            self.logger.info(f'DRYRUN: Skip writing file "{self.filename}"')
            return None

        # Create new file as required
        if not self.file_exists():
            self.create_file()

        # Continue with setting content to Filesystem object.
        return True

    def _check_value(self):
        """
        Check if the value needs to be loaded from the file.
        """
        if self._value is None:
            self._get_content()

class ListIOMixin(FileIOMixin):

    def __str__(self) -> str:
        """String representation"""
        content = self.get()
        return '\n'.join(content)
    
    def __len__(self) -> int:
        """Length of value"""
        content = self.get()
        return len(content)
    
    def __iter__(self) -> Generator[str, None, None]:
        """Iterator for the set of values"""
        for i in self._value:
            if i is not None:
                yield i

    def __getitem__(self, index):
        """
        Override FileIOMixin class for getting index
        """
        if self._value is None:
            self._get_content()

        if not isinstance(index, int):
            raise ValueError(
                'List-based Filehandler is not numerically indexable.'
            )

        return self._value[index]
    
    def __setitem__(self, index: int, value) -> None:
        """
        Enables setting items in filehandlers 'fh[0] = 1'
        """
        if self._value is None:
            self._get_content()

        if not isinstance(index, int):
            raise ValueError(
                'List-based Filehandler is not numerically indexable.'
            )

        self._value[index] = value

    def append(self, newvalue) -> None:
        """Add a new value to the internal list"""
        self._value.append(newvalue)

    def set(self, value: list):
        """
        Extends the set function of the parent, creates a copy
        of the input list so the original parameter is preserved.
        """
        super().set(list(value))

    def _get_content(self) -> None:
        """
        Open the file to get content if it exists
        """
        if self.file_exists():
            with open(self._file) as f:
                content = [r.strip() for r in f.readlines()]
            self._value = content

        else:
            self.create_file()
            self._value = []

    def _set_content(self) -> None:
        """If the content can be saved, save to the file."""
        if super()._check_save():
            with open(self._file,'w') as f:
                f.write('\n'.join(self._value))

class JSONFileHandler(FileIOMixin):
    description = "JSON File handler for padocc config files."

    def __str__(self) -> str:
        """String representation"""
        return yaml.dump(self.get())

    def __len__(self) -> int:
        """Returns number of keys in this dict-like object."""
        self._check_value()

        return len(self._value.keys())
    
    def __iter__(self) -> Generator[str, None, None]:
        """Iterate over set of keys."""
        self._check_value()

        for i in self._value.keys():
            yield i

    def __getitem__(self, index: str):
        """
        Enables indexing for filehandlers 'fh[0]'
        """
        if self._value is None:
            self._get_content()

        if index in self._value:
            return self._value[index]
        return None

    def __setitem__(self, index: str, value) -> None:
        """
        Enables setting items in filehandlers 'fh[0] = 1'
        """
        if self._value is None:
            self._get_content()

        if index in self._value:
            self._value[index] = value
        return None

    def set(self, value: dict):
        """
        Wrapper to create a detached dict copy
        """
        super().set(dict(value))

    def _set_file(self):
        if '.json' not in self.filename:
            self._file = f'{self.dir}/{self.filename}.json'
        else:
            self._file = f'{self.dir}/{self.filename}'

    # Get/set routines for the filesystem files.
    def _get_content(self):
        if self.file_exists():
            try:
                with open(self._file) as f:
                    self._value = json.load(f)
            except json.decoder.JSONDecodeError:
                self._value={}
        else:
            self.create_file()
            self._value = {}

    def _set_content(self):
        if super()._check_save():
            with open(self._file,'w') as f:
                f.write(json.dumps(self._value))

class KerchunkFile(JSONFileHandler):

    def add_download_link(self) -> dict:
        """
        Add the download link to this Kerchunk File
        """
        refs = self.get()

        for key in refs.keys():
            if len(refs[key]) == 3:
                if refs[key][0][0] == '/':
                    refs[key][0] = 'https://dap.ceda.ac.uk' + refs[key][0]

        self.set(refs)

    def add_kerchunk_history(self, version_no) -> dict:
        """
        Add kerchunk variables to the metadata for this dataset, including 
        creation/update date and version/revision number.
        """

        from datetime import datetime

        # Get current time
        attrs = self['refs']

        # Format for different uses
        now = datetime.now()
        if 'history' in attrs:
            if type(attrs['history']) == str:
                hist = attrs['history'].split('\n')
            else:
                hist = attrs['history']

            if 'Kerchunk' in hist[-1]:
                hist[-1] = 'Kerchunk file updated on ' + now.strftime("%D")
            else:
                hist.append('Kerchunk file created on ' + now.strftime("%D"))
            attrs['history'] = '\n'.join(hist)
        else:
            attrs['history'] = 'Kerchunk file created on ' + now.strftime("%D") + '\n'
        
        attrs['kerchunk_revision'] = version_no
        attrs['kerchunk_creation_date'] = now.strftime("%d%m%yT%H%M%S")
        
        self.set(attrs, index='refs')

class ZarrStore(FileIOMixin):
    def clear(self):
        if not self._dryrun:
            os.system(f'rm -rf {self._file}')
        else:
            self.logger.warning(
                f'Unable to clear ZarrStore "{self._file}" in dryrun mode.')

    def _set_file(self):
        self._file = f'{self.dir}/{self.filename}'

class TextFileHandler(ListIOMixin):
    description = "Text File handler for padocc config files."

    def _set_file(self):
        if '.txt' not in self.filename:
            self._file = f'{self.dir}/{self.filename}.txt'
        else:
            self._file = f'{self.dir}/{self.filename}'

class LogFileHandler(ListIOMixin):
    description = "Log File handler for padocc phase logs."

    def __init__(self, dir, filename, logger, extra_path, **kwargs):
        self._extra_path = extra_path
        super().__init__(dir, filename, logger, **kwargs)

    def _set_file(self):
        self._file = f'{self.dir}/{self._extra_path}{self.filename}.log'

class CSVFileHandler(ListIOMixin):
    description = "CSV File handler for padocc config files"
    
    def _set_file(self):
        self._file = f'{self.dir}/{self.filename}.csv'

    def __iter__(self):
        for i in self._value:
            if i is not None:
                yield i.replace(' ','').split(',')

    def update_status(
            self, 
            phase: str, 
            status: str,
            jobid : str = '',
            dryrun: bool = False
        ) -> None:

        self._check_value()

        status = status.replace(',', '.').replace('\n','.')
        addition = f'{phase},{status},{datetime.now().strftime("%H:%M %D")},{jobid},{dryrun}'
        self.append(addition)
        self.logger.info(f'Updated new status: {phase} - {status}')