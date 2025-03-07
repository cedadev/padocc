__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from typing import Callable

from padocc.core.filehandlers import JSONFileHandler

class StatusMixin:
    """
    Methods relating to the ProjectOperation class, in terms
    of determining the status of previous runs.
    
    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: ProjectOperation [ONLY]
    """

    @classmethod
    def help(cls, func: Callable = print):
        """
        Helper function displays basic functions for use.

        :param func:        (Callable) provide an alternative to 'print' function
            for displaying help information.
        """
        func('Status Options:')
        func(' > project.get_last_run() - Get the last performed phase and time it occurred')
        func(' > project.get_last_status() - Get the status of the previous core operation.')
        func(' > project.get_log_contents() - Get the log contents of a previous core operation')

    def update_status(
            self, 
            phase : str, 
            status: str, 
            jobid : str = ''
        ) -> None: 
        """
        Update the status of a project

        Status updates performed via the status log filehandler,
        during phased operation of the pipeline.

        :param phase:   (str) Phased operation being performed.

        :param status:  (str) Status of phased operation outcome

        :param jobid:   (str) ID of SLURM job in which this operation has taken place.
        """
        self.status_log.update_status(phase, status, jobid=jobid)
        self.status_log.close()

    def set_last_run(self, phase: str, time : str) -> None:
        """
        Set the phase and time of the last run for this project.

        :param phase:   (str) Phased operation of last run.

        :param time:    (str) Timestamp for operation.
        """
        lr = (phase, time)
        self.base_cfg['last_run'] = lr

    def get_last_run(self) -> tuple:
        """
        Get the tuple-value for this projects last run."""
        return self.base_cfg['last_run']

    def get_last_status(self) -> str:
        """
        Gets the last line of the correct log file
        """
        try:
            return self.status_log[-1]
        except IndexError:
            return None

    def get_log_contents(self, phase: str) -> str:
        """
        Get the contents of the log file as a string

        :param phase:   (str) Phased operation from which to pull logs.
        """

        if phase in self.phase_logs:
            return str(self.phase_logs[phase])
        self.logger.warning(f'Phase "{phase}" not recognised - no log file retrieved.')
        return ''

    def show_log_contents(self, phase: str, halt : bool = False, func: Callable = print):
        """
        Format the contents of the log file to print.

        :param phase:   (str) Phased operation to pull log data from.

        :param halt:    (bool) Stop and display log data, wait for input before
            continuing.

        :param func:        (Callable) provide an alternative to 'print' function
            for displaying help information.
        """

        logfh = self.get_log_contents(phase=phase)
        status = self.status_log[-1].split(',')
        func(logfh)

        func(f'Project Code: {self.proj_code}')
        func(f'Status: {status}')

        func(self._rerun_command())

        if halt:
            paused = input('Type "E" to exit assessment:')
            if paused == 'E':
                raise KeyboardInterrupt

    def _rerun_command(self):
        """
        Setup for running this specific component interactively.
        """
        return f'padocc <operation> -G {self.groupID} -p {self.proj_code} -vv'
    
    def get_report(self) -> dict:
        """
        Get the validation report if present for this project.
        """

        full_report = {'data':None, 'metadata':None}

        meta_fh = JSONFileHandler(self.dir, 'metadata_report',logger=self.logger, **self.fh_kwargs)
        data_fh = JSONFileHandler(self.dir, 'data_report',logger=self.logger, **self.fh_kwargs)

        if meta_fh.file_exists():
            full_report['metadata'] = meta_fh.get()
        if data_fh.file_exists():
            full_report['data'] = data_fh.get()

        return full_report
