__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os

from typing import Union, Optional

from padocc.core.utils import BypassSwitch, format_str

class PreprocessMixin:
    """
    Mixin for enabling custom data preprocessing via PADOCC

    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: GroupOperation [ONLY]
    """

    # WCRP Example:
    # - Preprocess all data into healpix format.
    # - Each project preprocesses data individually.

    def preprocess_all(
            self,
            preprocess_script: str,
            preprocess_opts: dict,
            repeat_id: str = 'main',
            proj_code: Optional[str] = None,
            subset: Optional[str] = None,
            bypass: Union[BypassSwitch, None] = None,
            forceful: Optional[bool] = None,
            thorough: Optional[bool] = None,
            dryrun: Optional[bool] = None,
            run_kwargs: Union[dict,None] = None,
            **kwargs,
        ) -> dict[str]:
        """
        Perform preprocessing for all projects.
        """

        bypass = bypass or self._bypass
        run_kwargs = run_kwargs or {}

        self._set_fh_kwargs(forceful=forceful, dryrun=dryrun, thorough=thorough)

        jobid = None
        if os.getenv('SLURM_ARRAY_JOB_ID'):
            jobid = f"{os.getenv('SLURM_ARRAY_JOB_ID')}-{os.getenv('SLURM_ARRAY_TASK_ID')}"
   
        codeset = self.proj_codes[repeat_id].get()
        if subset is not None:
            codeset = self._configure_subset(codeset, subset, proj_code)

        if proj_code is not None:
            if proj_code in codeset:
                self.logger.info(f'Project code: {proj_code}')
                codeset = [proj_code]
            elif proj_code.isnumeric():
                if abs(int(proj_code)) > len(codeset):
                    raise ValueError(
                        'Invalid project code specfied. If indexing, '
                        f'must be less than {len(codeset)-1}'
                    )
                # Perform by index
                codeset = [codeset[int(proj_code)]]

        for id, proj_code in enumerate(codeset):
            self.logger.info(f'Starting operation: {id+1}/{len(codeset)} ({format_str(proj_code, 15, concat=True, shorten=True)})')

            proj_op = self.get_project(proj_code)
            proj_op.preprocess(
                preprocess_script,
                preprocess_opts
            )

            # A preprocess task must be of the form:
            # input files -> preprocess script -> output files
            # plus option to reconfigure allfiles to output files.
        