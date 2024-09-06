__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import json
import os
import yaml
from datetime import datetime
import logging

from padocc.core import LoggedOperation, FalseLogger

class PadoccFileHandler(LoggedOperation):
    description = "Generic Filehandler for padocc file objects."
    def __init__(
            self, 
            dir : str, 
            filename : str, 
            logger   : logging.logger | FalseLogger = None, 
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

        :param logger:      (logging.logger | FalseLogger) An existing logger object.

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

    def __contains__(self, item):
        content = self.get()
        return item in content

    def __getitem__(self, index):
        return self.get(index=index)

    def __setitem__(self, index, value):
        if not self._value:
            self._get_content()

        self._value[index] = value

    def file_exists(self):
        return os.path.isfile(self._file)

    def create_file(self):
        if not self._dryrun:
            os.system(f'touch {self._file}')

    def save_file(self):
        """Wrapper for _set_content method"""
        self._set_content()

    def _get_content(self):
        raise NotImplementedError
    
    def _set_content(self):
        # Only set value if value has been loaded to edit.
        if not self._value:
            return None

        # Only set value if not doing a dryrun
        if self._dryrun:
            self.logger.debug(f'DRYRUN: Skip writing file "{self.filename}"')
            return None

        # Create new file as required
        if not self.file_exists():
            self.create_file()

        # Continue with setting content to Filesystem object.
        return True

    def set(self, value, index=None):
        if not self._value:
            self._get_content()

        if index:
            self._value[index] = value
        else:
            self._value = value

    def get(self, index=None):
        if not self._value:
            self._get_content()

        if index:
            v = self._value[index]
        else:
            v = self._value

        if v == {}:
            return None
    
    def _set_file(self):
        raise NotImplementedError

class JSONFileHandler(PadoccFileHandler):
    description = "JSON File handler for padocc config files."

    def __str__(self):
        return yaml.dump(self.get())

    def _set_file(self):
        if '.json' not in self.filename:
            self._file = f'{self.dir}/{self.filename}.json'
        else:
            self._file = f'{self.dir}/{self.filename}'
    
    # Get/set routines for the filesystem files.
    def _get_content(self):
        if self.file_exists():
            with open(self._file) as f:
                self._value = json.load(f)
        else:
            self.create_file()
            self._value = {}

    def _set_content(self):
        if super()._set_content():
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
        attrs = self.get(index='refs')

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

class ZarrStore(PadoccFileHandler):
    def clear(self):
        if not self._dryrun:
            os.system(f'rm -rf {self._file}')
        else:
            self.logger.warning(
                f'Unable to clear ZarrStore "{self._file}" in dryrun mode.')

    def _set_file(self):
        self._file = f'{self.dir}/{self.filename}'

class ListBased(PadoccFileHandler):

    def __str__(self):
        content = self.get()
        return '\n'.join(content)
    
    def __len__(self):
        content = self.get()
        return len(content)

    def _get_content(self):
        if self.file_exists():
            with open(self._file) as f:
                content = [r.strip() for r in f.readlines()]
            self._value = content

        else:
            self.create_file()
            self._value = []

    def _set_content(self):
        if not self.file_exists():
            self.create_file()

        with open(self._file,'w') as f:
            f.write('\n'.join(self._value))

    def update(self, newvalue):
        self._value = newvalue

class TextFileHandler(ListBased):
    description = "Text File handler for padocc config files."

    def _set_file(self):
        self._file = f'{self.dir}/{self.filename}.txt'

class LogFileHandler(ListBased):
    description = "Log File handler for padocc logs."

    def __init__(self, dir, filename, logger, extra_path, **kwargs):
        self._extra_path = extra_path
        super().__init__(dir, filename, logger, **kwargs)

    def _set_file(self):
        self._file = f'{self.dir}/{self._extra_path}{self.filename}.log'

class CSVFileHandler(ListBased):
    description = "CSV File handler for padocc config files"
    
    def _set_file(self):
        self._file = f'{self.dir}/{self.filename}.csv'

    def append(self, newvalue):
        self._value.append(newvalue)

    def __iter__(self):
        for i in self._value:
            if i is not None:
                yield i.replace(' ','').split(',')

    def update_status(self, phase, status, jobid='', dryrun=''):
        if not self._value:
            self._get_content()

        status = status.replace(',', '.').replace('\n','.')
        addition = f'{phase},{status},{datetime.now().strftime("%H:%M %D")},{jobid},{dryrun}'
        self.append(addition)
        self.logger.info(f'Updated new status: {phase} - {status}')