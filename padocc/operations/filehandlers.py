import json
import os

from datetime import datetime

class PadoccFileHandler:
    description = "Generic Filehandler for padocc file objects."
    def __init__(
            self, 
            dir, 
            filename, 
            logger, 
            dryrun=None,
            forceful=None):
        
        self.dir       = dir
        self.filename  = filename
        self.logger    = logger

        self._dryrun   = dryrun
        self._forceful = forceful
        self._value    = None
        self._file     = None

        self._set_file()

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
            return self._value[index]
        else:
            return self._value
    
    def _set_file(self):
        raise NotImplementedError

class JSONFileHandler(PadoccFileHandler):
    description = "JSON File handler for padocc config files."

    def _set_file(self):
        self._file = f'{self.dir}/{self.filename}.json'
    
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

class ListBased(PadoccFileHandler):

    def __str__(self):
        content = self.get()
        return '\n'.join(content)

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