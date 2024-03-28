__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

class TestArgs:
    def __init__(self):
        self.blacklist = None
        self.reason    = None

        self.option    = None
        self.cleanup   = None
        self.upgrade   = None
        self.long      = None

        self.jobID     = None
        self.phase     = None
        self.repeat_id = 'main'
        self.new_id    = None

        self.error     = ''
        self.examine   = None

        self.write     = None
        self.overwrite = 0

        self.workdir   = None
        self.groupdir  = None
        self.verbose   = None
        self.mode      = None