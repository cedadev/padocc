__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import datetime
from collections.abc import Callable
from typing import Optional, Union

from padocc import ProjectOperation
from padocc.core.utils import deformat_float, format_float, format_str


class EvaluationsMixin:
    """
    Group Mixin for methods to evaluate the status of a group.

    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: GroupOperation [ONLY]
    """

    @classmethod
    def help(cls, func: Callable = print):
        func('Evaluations:')
        func(' > group.get_project() - Get a project operator, indexed by project code')
        func(' > group.repeat_by_status() - Create a new subset group to (re)run an operation, based on the current status')
        func(' > group.remove_by_status() - Delete projects based on a given status')
        func(' > group.merge_subsets() - Merge created subsets')
        func(' > group.summarise_data() - Get a printout summary of data representations in this group')
        func(' > group.summarise_status() - Summarise the status of all group member projects.')

    def get_project(self, proj_code: str):
        """
        Get a project operation from this group
        """

        return ProjectOperation(
            proj_code,
            self.workdir,
            groupID=self.groupID,
            logger=self.logger,
            **self.fh_kwargs
        )

    def check_attribute(
            self, 
            attribute: str,
            repeat_id: str = 'main',
            func: Callable = print
        ):
        """
        Check an attribute across all projects.
        """

        func(f'Checking {attribute} for {self.groupID}')

        for proj_code in self.proj_codes[repeat_id]:
            proj_op = self.get_project(proj_code)

            val = proj_op.detail_cfg.get(attribute, None)
            if val is None:
                val = proj_op.base_cfg.get(attribute, None)
            func(f' > {proj_code}: {val}')

    def repeat_by_status(
            self, 
            status: str, 
            new_repeat_id: str, 
            phase: Optional[str] = None,
            old_repeat_id: str = 'main'
        ) -> None:
        """
        Group projects by their status, to then
        create a new repeat ID.
        """
        faultdict = self._get_fault_dict()
        status_dict, _ = self._get_status_dict(
            old_repeat_id,
            faultdict,
            specific_phase=phase,
            specific_error=status
        )

        new_code_ids = []
        if phase is not None:
            # New codes are in the status_dict
            if status in status_dict[phase]:
                new_code_ids = status_dict[phase][status]
        else:
            for phase in status_dict.keys():
                if status in status_dict[phase]:
                    new_code_ids = new_code_ids + status_dict[phase][status]

        new_codes = []
        for id in new_code_ids:
            new_codes.append(self.proj_codes['main'][id])

        self._add_proj_codeset(
                new_repeat_id,
                new_codes
            )
        self._save_proj_codes()

    def remove_by_status(
            self, 
            status: str, 
            phase: Optional[str] = None,
            old_repeat_id: str = 'main'
        ) -> None:
        """
        Group projects by their status for
        removal from the group
        """
        faultdict = self._get_fault_dict()
        status_dict = self._get_status_dict(
            old_repeat_id,
            faultdict,
            specific_phase=phase,
            specific_error=status
        )

        for code in status_dict[phase][status]:
            self.remove_project(code)

        self.save_files()
        
    def merge_subsets(
            self,
            subset_list: list[str],
            combined_id: str,
            remove_after: bool = False,
        ) -> None:
        """
        Merge one or more of the subsets previously created
        """
        newset = []

        for subset in subset_list:
            if subset not in self.proj_codes:
                raise ValueError(
                    f'Repeat subset "{subset}" not found in existing subsets.'
                )
            
            newset = newset + self.proj_codes[subset].get()

        if remove_after:
            for subset in subset_list:
                self._delete_proj_codeset(subset)

        self._add_proj_codeset(combined_id, newset)

        self._save_proj_codes()

    def remove_subset(
            self,
            repeat_id: str
        ) -> None:
        """
        Remove a subset from the group.
        
        :param repeat_id:       (str) The repeat_id classifying the subset in this group
            to which this operation will apply.
        
        """
        
        if self._dryrun:
            self.logger.warning('Unable to remove a subset in dryrun mode')

        fh = self.proj_codes.pop(repeat_id, None)

        if fh is None:
            raise ValueError(
                f'Subset "{repeat_id}" not found - '
                f'Group contains {list(self.proj_codes.keys())}'
            )
        
        fh.remove_file()

    def summarise_data(self, repeat_id: str = 'main', func: Callable = print):
        """
        Summarise data stored across all projects, mostly
        concatenating results from the detail-cfg files from
        all projects.
        """
        import numpy as np

        # Cloud Formats and File Types
        # Source Data [Avg,Total]
        # Cloud Data [Avg,Total]
        # File Count [Avg,Total]

        cloud_formats: dict = {}
        source_formats: dict = {}
        file_types: dict = {}

        source_data: list = []
        cloud_data:  list = []
        file_count:  list = []
        
        # Chunk Info
        ## Chunks per file [Avg,Total]
        ## Total Chunks [Avg, Total]

        chunks_per_file: list = []
        total_chunks: list = []

        for proj_code in self.proj_codes[repeat_id]:
            op = self.get_project(proj_code)

            if op.cloud_format in cloud_formats:
                cloud_formats[op.cloud_format] += 1
            else:
                cloud_formats[op.cloud_format] = 1

            if op.source_format in source_formats:
                source_formats[op.source_format] += 1
            else:
                source_formats[op.source_format] = 1

            if op.file_type in file_types:
                file_types[op.file_type] += 1
            else:
                file_types[op.file_type] = 1

            if not op.detail_cfg.file_exists():
                continue

            details = op.detail_cfg.get()
            if details == {} or 'skipped' in details:
                continue

            if 'source_data' in details:
                source_data.append(
                    deformat_float(details['source_data'])
                )
            if 'cloud_data' in details:
                cloud_data.append(
                    deformat_float(details['cloud_data'])
                )

            file_count.append(int(details['num_files']))

            chunk_data = details['chunk_info']
            chunks_per_file.append(
                float(chunk_data['chunks_per_file'])
            )
            total_chunks.append(
                int(chunk_data['total_chunks'])
            )

        # Render Outputs
        ot = []
        
        ot.append(f'Summary Report: {self.groupID}')
        ot.append(f'Project Codes: {len(self.proj_codes[repeat_id])}')
        ot.append('')
        if len(file_count) > 0:
            ot.append(f'Source Files: {sum(file_count)} [Avg. {np.mean(file_count):.2f} per project]')
        else:
            ot.append('Source Files: Unknown')
        if len(source_data) > 0:
            ot.append(f'Source Data: {format_float(sum(source_data))} [Avg. {np.mean(source_data):.2f} per project]')
        else:
            ot.append('Source Data: Unknown')
        if len(cloud_data) > 0:
            ot.append(f'Cloud Data: {format_float(sum(cloud_data))} [Avg. {np.mean(cloud_data):.2f} per project]')
        else:
            ot.append('Cloud Data: Unknown')
        ot.append('')
        if len(cloud_formats) > 0:
            ot.append(f'Cloud Formats: {list(set(cloud_formats))}')
        if len(source_formats) > 0:
            ot.append(f'Source Formats: {list(set(source_formats))}')
        if len(file_types) > 0:
            ot.append(f'File Types: {list(set(file_types))}')
        ot.append('')
        if len(chunks_per_file) > 0:
            ot.append(
                f'Chunks per File: {format_float(sum(chunks_per_file))} [Avg. {np.mean(chunks_per_file):.2f} per project]')
        if len(total_chunks) > 0:
            ot.append(
                f'Total Chunks: {format_float(sum(total_chunks))} [Avg. {np.mean(total_chunks):.2f} per project]')
        
        func('\n'.join(ot))

    def summarise_status(
            self, 
            repeat_id: str = 'main', 
            specific_phase: Union[str,None] = None,
            specific_error: Union[str,None] = None,
            long_display: Union[bool,None] = None,
            display_upto: int = 5,
            halt: bool = False,
            write: bool = False,
            fn: Callable = print,
        ) -> None:
        """
        Gives a general overview of progress within the pipeline
        - How many datasets currently at each stage of the pipeline
        - Errors within each pipeline phase
        - Allows for examination of error logs
        - Allows saving codes matching an error type into a new repeat group
        """

        faultdict = self._get_fault_dict()

        status_dict, longest_err = self._get_status_dict(
            repeat_id, 
            faultdict=faultdict,
            specific_phase=specific_phase,
            specific_error=specific_error,
            halt=halt,
            write=write,
        )

        num_codes  = len(self.proj_codes[repeat_id])
        ot = []
        ot.append('')
        ot.append(f'Group: {self.groupID}')
        ot.append(f'  Total Codes: {num_codes}')
        ot.append('')
        ot.append('Pipeline Current:')

        if longest_err > 30 and long_display is None:
            longest_err = 30

        for phase, records in status_dict.items():

            if isinstance(records, dict):
                ot = ot + self._summarise_dict(phase, records, num_codes, status_len=longest_err, numbers=display_upto)
            else:
                ot.append('')

        ot.append('')
        ot.append('Pipeline Complete:')
        ot.append('')

        complete = len(status_dict['complete'])

        complete_percent = format_str(f'{complete*100/num_codes:.1f}',4)
        ot.append(f'   complete  : {format_str(complete,5)} [{complete_percent}%]')

        for option, records in faultdict['faultlist'].items():
            ot = ot + self._summarise_dict(option, records, num_codes, status_len=longest_err, numbers=0)

        ot.append('')
        fn('\n'.join(ot))

    def _get_fault_dict(self) -> dict:
        """
        Assemble the fault list into a dictionary
        with all reasons.
        """
        extras   = {'faultlist': {}}
        for code, reason in self.faultlist:
            if reason in extras['faultlist']:
                extras['faultlist'][reason].append(0)
            else:
                extras['faultlist'][reason] = [0]
            extras['ignore'][code] = True
        return extras

    def _get_status_dict(
            self,
            repeat_id, 
            faultdict: dict = None,
            specific_phase: Union[str,None] = None,
            specific_error: Union[str,None] = None,
            halt: bool = False,
            write: bool = False,
        ) -> dict:

        """
        Assemble the status dict, can be used for stopping and 
        directly assessing specific errors if needed.
        """

        faultdict = faultdict or {}

        if 'ignore' not in faultdict:
            faultdict['ignore'] = []
        
        proj_codes = self.proj_codes[repeat_id]

        if write:
            self.logger.info(
                'Write permission granted:'
                ' - Will seek status of unknown project codes'
                ' - Will update status with "JobCancelled" for >24hr pending jobs'
            )

        status_dict = {'init':{},'scan': {}, 'compute': {}, 'validate': {},'complete':[]}

        longest_err = 0
        for idx, p in enumerate(proj_codes):
            if p in faultdict['ignore']:
                continue

            status_dict, longest_err = self._assess_status_of_project(
                p, idx,
                status_dict,
                write=write,
                specific_phase=specific_phase,
                specific_error=specific_error,
                halt=halt,
                longest_err=longest_err
            )
        return status_dict, longest_err

    def _assess_status_of_project(
            self, 
            proj_code: str, 
            pid: int,
            status_dict: dict,
            write: bool = False,
            specific_phase: Union[str,None] = None,
            specific_error: Union[str,None] = None,
            halt: bool = False,
            longest_err: int = 0,
            ) -> dict:
        """
        Assess the status of a single project
        """

        # Open the specific project
        proj_op = self.get_project(proj_code)

        current = proj_op.get_last_status()
        if current is None:
            return {}, 0

        entry   = current.split(',')

        phase  = entry[0]
        status = entry[1]
        time   = entry[2]

        if len(status) > longest_err:
            longest_err = len(status)

        if status == 'pending' and write:
            timediff = (datetime.now() - datetime(time)).total_seconds()
            if timediff > 86400: # 1 Day - fixed for now
                status = 'JobCancelled'
                proj_op.update_status(phase, 'JobCancelled')
        
        total_match = True
        if specific_phase or specific_error:
            match_phase = (specific_phase == phase)
            match_error = (specific_error == status)

            if bool(specific_phase) != (match_phase) or bool(specific_error) != (match_error):
                total_match = False
            else:
                total_match = match_phase or match_error

            if total_match and halt:
                proj_op.show_log_contents(specific_phase, halt=halt)

        if status == 'complete':
            status_dict['complete'] += 1
        else:
            if status in status_dict[phase]:
                status_dict[phase][status].append(pid)
            else:
                status_dict[phase][status] = [pid]

        return status_dict, longest_err

    def _summarise_dict(
            self,
            phase: str, 
            records: dict, 
            num_codes: int, 
            status_len: int = 5, 
            numbers: int = 0
        ) -> list:
        """
        Summarise information for a dictionary structure
        that contains a set of errors for a phase within the pipeline
        """
        ot = []

        pcount = len(list(records.keys()))
        num_types = sum([len(records[pop]) for pop in records.keys()])
        if pcount > 0:

            ot.append('')
            fmentry     = format_str(phase,10, concat=False)
            fmnum_types = format_str(num_types,5, concat=False)
            fmcalc      = format_str(f'{num_types*100/num_codes:.1f}',4, concat=False)

            ot.append(f'   {fmentry}: {fmnum_types} [{fmcalc}%] (Variety: {int(pcount)})')

            # Convert from key : len to key : [list]
            errkeys = reversed(sorted(records, key=lambda x:len(records[x])))
            for err in errkeys:
                num_errs = len(records[err])
                if num_errs < numbers:
                    ot.append(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs} (IDs = {list(records[err])})')
                else:
                    ot.append(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs}')
        return ot
