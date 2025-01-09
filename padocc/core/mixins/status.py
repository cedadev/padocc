__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

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

    def set_last_run(self, phase: str, time : str) -> None:
        """
        Set the phase and time of the last run for this project.
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
        return self.status_log[-1]

    def get_log_contents(self, phase: str) -> str:
        """
        Get the contents of the log file as a string
        """

        if phase in self.phase_logs:
            return str(self.phase_logs[phase])
        self.logger.warning(f'Phase "{phase}" not recognised - no log file retrieved.')
        return ''

    def show_log_contents(self, phase: str, halt : bool = False):
        """
        Format the contents of the log file to print.
        """

        logfh = self.get_log_contents(phase=phase)
        status = self.status_log[-1].split(',')
        self.logger.info(logfh)

        self.logger.info(f'Project Code: {self.proj_code}')
        self.logger.info(f'Status: {status}')

        self.logger.info(self._rerun_command())

        if halt:
            paused = input('Type "E" to exit assessment:')
            if paused == 'E':
                raise KeyboardInterrupt

    def _rerun_command(self):
        """
        Setup for running this specific component interactively.
        """
        return ''
