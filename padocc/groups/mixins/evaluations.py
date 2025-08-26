__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from datetime import datetime
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
    
    def combine_reports(
            self,
            repeat_id: str = 'main'
        ) -> dict:
    
            combined = {}
            for proj_code in self.proj_codes['main']:
                project = self[proj_code]

                report = project.get_report()
                combined.update(report)
            return combined

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
        new_code_ids = self.determine_status_sets(status, phase=phase)

        new_codes = []
        for id in new_code_ids:
            new_codes.append(self.proj_codes[old_repeat_id][id])

        if len(new_codes) < 1:
            self.logger.warning('No project codes correspond to the request - no repeat ID created')
            return

        self._add_proj_codeset(
                new_repeat_id,
                new_codes
            )
        self._save_proj_codes()

    def determine_status_sets(
            self, 
            status: str, 
            phase: Union[str,None] = None,
            repeat_id: str = 'main',
            write: bool = False):
        """
        Analyse status dict for given status request.
        """

        status_dict = self.get_codes_by_status(repeat_id, write=write)

        if phase is None:
            phase_set = list(status_dict.keys())
        else:
            phase_set = phase.split('&')

        def add_sets(collator, new_sample):
            """
            Add sets by intersection.
            """
            if collator == []:
                return new_sample
            
            return set(collator).intersection(new_sample)

        new_code_ids = []
        for phase in phase_set:
            status_per_phase = []
            if phase == 'complete':
                continue

            # Bug - multiple statuses can have same prefix. 
            # Need to have options for multiple mappings here.
            status_opts = {}
            for stat in status_dict.get(phase,{}).keys():
                stat_short = stat.split('-')[0].strip()
                if stat_short in status_opts:
                    if isinstance(status_opts[stat_short], str):
                        status_opts[stat_short] = [status_opts[stat_short], stat]
                    else:
                        status_opts[stat_short].append(stat)
                else:
                    status_opts[stat_short] = stat

            status_set = status.split('&')
            for status_part in status_set:
                matching_status = []

                self.logger.debug(f'Phase: {phase}, Status: {status_part}')
                # Pull any statuses
                if status_part == 'Any':
                    # Get all statuses
                    for all_status in status_dict[phase].keys():
                        matching_status += status_dict[phase][all_status]

                    status_per_phase = add_sets(status_per_phase, matching_status)

                elif status_part.startswith('!'):
                    # Get all except the status
                    status_part = status_part[1:]
                    for all_status in status_dict[phase].keys():
                        # Get all statuses except where they are in the opts dict above.
                        optionals = status_opts.get(status_part,None)
                        if optionals == all_status:
                            continue
                        elif isinstance(optionals, list) and all_status in optionals:
                            continue
                        matching_status += status_dict[phase][all_status]
                    status_per_phase = add_sets(status_per_phase, matching_status)

                else:
                    # Specific status from the dict for this phase

                    # Is this status one of the known ones for this phase?

                    if status_part in status_dict[phase].keys():
                        status_per_phase = add_sets(status_per_phase, status_dict[phase][status_part])

                    # Is this status representing one from a set of multiple statuses?
                    elif status_part in status_opts.keys():
                        if isinstance(status_opts[status_part], str):
                            status_per_phase = add_sets(status_per_phase, status_dict[phase][status_opts[status_part]])
                        else:
                            spp = []
                            for sp in status_opts[status_part]:
                                spp += status_dict[phase][sp]
                            status_per_phase = add_sets(status_per_phase, spp)

            new_code_ids += status_per_phase

        return list(set(new_code_ids))

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

    def summarise_data(self, repeat_id: str = 'main', func: Callable = print) -> Union[str,None]:
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

        proj_count = 0
        for proj_code in self.proj_codes[repeat_id]:
            op = self.get_project(proj_code)

            if not op.detail_cfg.file_exists():
                continue

            details = op.detail_cfg.get()
            if details == {} or 'skipped' in details:
                continue

            if details['source_data'] is None:
                continue

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
                int(chunk_data['total_chunks'].split('.')[0])
            )
            proj_count += 1

        # Render Outputs
        ot = []
        
        ot.append(f'Summary Report: {self.groupID}')
        ot.append(f'Project Codes Assessed: {proj_count}')
        ot.append('')
        if len(file_count) > 0:
            ot.append(f'Source Files: {sum(file_count)} [Avg. {np.mean(file_count):.2f} per project]')
        else:
            ot.append('Source Files: Unknown')
        if len(source_data) > 0:
            ot.append(f'Source Data: {format_float(sum(source_data))} [Avg. {format_float(np.mean(source_data))} per project]')
        else:
            ot.append('Source Data: Unknown')
        if len(cloud_data) > 0:
            ot.append(f'Cloud Data: {format_float(sum(cloud_data))} [Avg. {format_float(np.mean(cloud_data))} per project]')
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
                f'Chunks per File: {sum(chunks_per_file):.2f} [Avg. {np.mean(chunks_per_file):.2f} per project]')
        if len(total_chunks) > 0:
            ot.append(
                f'Total Chunks: {sum(total_chunks):.2f} [Avg. {np.mean(total_chunks):.2f} per project]')
        
        if func is not None:
            func('\n'.join(ot))
        else:
            return '\n'.join(ot)

    def match_data_reports(
            self,
            sample_report
        ) -> dict:

        matches = {}
        def check_keys(test, control):
            matched = True
            for k, v in test.items():
                if isinstance(v, dict):
                    try:
                        matched = check_keys(test[k], control[k])
                    except KeyError:
                        matched = False
                if k not in control:
                    matched = False
            return matched
        
        for pc in self.proj_codes['main']:
            proj = self[pc]
            matches[pc] = check_keys(proj.get_report()['data'], sample_report['data'])

        return matches
            
    def get_codes_by_status(
            self,
            repeat_id: str = 'main',
            write: bool = False,
        ) -> dict:
            """
            Public Method for just getting the status dict
            for a group.
            """

            status_dict, _ = self._get_status_dict(repeat_id, write=write)
            return status_dict

    def summarise_aggregations(
            self, 
            repeat_id: str = 'main',
            fn: Callable = print,
        ) -> None:

        if repeat_id not in self.proj_codes:
            raise ValueError(f'Unrecognised repeat ID: {repeat_id} for {self.groupID}')

        aggregations = {
            'Incomplete':[],
            'PADOCC':[],
            'VirtualiZarr':[],
            'Kerchunk':[]
        }

        for proj_code in self.proj_codes[repeat_id]:
            project = self[proj_code]

            status_msg = project.get_last_status().split(',')

            if status_msg[0] == 'validate' or (status_msg[0] == 'compute' and status_msg[1] == 'Success'):

                if project.padocc_aggregation:
                    aggregations['PADOCC'].append(proj_code)
                elif project.virtualizarr:
                    aggregations['VirtualiZarr'].append(proj_code)
                else:
                    aggregations['Kerchunk'].append(proj_code)

            else:
                aggregations['Incomplete'].append(proj_code)

        fn(f'Aggregations for {self.groupID}')
        for k, v in aggregations.items():
            fn(f' > {k}: {len(v)}')

    def summarise_status(
            self, 
            repeat_id: str = 'main', 
            specific_phase: Union[str,None] = None,
            specific_error: Union[str,None] = None,
            long_display: Union[bool,None] = False,
            display_upto: int = 5,
            separate_errors: bool = False,
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

        if longest_err > 30 and not long_display:
            longest_err = 30

        for phase, records in status_dict.items():

            if isinstance(records, dict):
                ot = ot + self._summarise_dict(phase, records, num_codes, 
                                               status_len=longest_err, numbers=display_upto, separate_errors=separate_errors)
            else:
                ot.append('')

        ot.append('')
        ot.append('Pipeline Complete:')
        ot.append('')

        complete = len(status_dict.get('complete',[]))

        complete_percent = format_str(f'{complete*100/num_codes:.1f}',4)
        ot.append(f'   complete  : {format_str(complete,5)} [{complete_percent}%]')

        for option, records in faultdict['faultlist'].items():
            ot = ot + self._summarise_dict(option, records, num_codes, 
                                           status_len=longest_err, numbers=0, separate_errors=separate_errors)

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
        
        try:
            proj_codes = self.proj_codes[repeat_id]
        except KeyError:
            raise ValueError(f'Repeat ID {repeat_id} not known to group {self.groupID} - no status possible')

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

            try:
                status_dict, longest_err = self._assess_status_of_project(
                    p, idx,
                    status_dict,
                    write=write,
                    specific_phase=specific_phase,
                    specific_error=specific_error,
                    halt=halt,
                    longest_err=longest_err
                )
            except Exception as err:
                self.logger.error(f'Project {p}: {err}')
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
            return status_dict, longest_err

        entry   = current.split(',')

        phase  = entry[0]
        status = entry[1]
        time   = entry[2]

        if len(status) > longest_err:
            longest_err = len(status)

        if status == 'Pending' and write:
            timediff = (datetime.now() - datetime.strptime(time, '%H:%M %d/%m/%y')).total_seconds()
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

        if phase == 'complete':
            try:
                status_dict['complete'].append(pid)
            except:
                status_dict['complete'] = [pid]
        else:
            if phase not in status_dict:
                status_dict[phase] = {}

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
            numbers: int = 0,
            separate_errors: bool = False,
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

            # Break records into groups if in validate
            if phase != 'validate' or not separate_errors:
                record_groups = {'All':records}
            else:
                record_groups  = {'Success':{},'Error':{}}
                success, error = 0, 0
                for r, v in records.items():
                    if 'Warn' in r or 'Success' in r:
                        record_groups['Success'][r] = v
                        success += len(v)
                    else:
                        record_groups['Error'][r] = v   
                        error += len(v)

                percentages = {
                    'Success': f'{success*100/(success+error):.1f}',
                    'Error': f'{error*100/(success+error):.1f}'
                }

            for g, r in record_groups.items():

                if g != 'All':
                    ot.append('')
                    ot.append(f'  > {g} ({percentages[g]} %)')
                # Convert from key : len to key : [list]
                errkeys = reversed(sorted(r, key=lambda x:len(r[x])))
                for err in errkeys:
                    num_errs = len(r[err])
                    if num_errs < numbers:
                        ot.append(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs} (IDs = {",".join([str(i) for i in list(r[err])])})')
                    else:
                        ot.append(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs}')
        return ot
