__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

from padocc.operations import GroupOperation, ProjectOperation

"""
Replacement for assessor tool. Requires the following (public) methods:
 - progress (progress_check)
 - blacklist
 - upgrade (upgrade_version)
 - summarise (summary_data)
 - display (show_options)
 - cleanup (cleanup) - May not need since this is built into the group.
 - match ?
 - status (status_log)
 - allocations (assess_allocation)

 
Private methods suspected:
 - _get_rerun_command : To get a specific rerun for a dataset.
 - _merge_old_new     : Combine sets of project codes.
 - _save_project_codes : Depends how the group stuff works if we need this
 _ _analyse_data      : Connect to project codes and get a summary of each.
 - _force_datetime_decode : Decode datetimes.
"""

class EvaluationOperation(GroupOperation):
    def progress(self, repeat_id, write=True):
        """Give a general overview of progress within the pipeline
        - How many datasets currently at each stage of the pipeline
        - Errors within each pipeline phase
        - Allows for examination of error logs
        - Allows saving codes matching an error type into a new repeat group
        """
        blacklist  = self.blacklist_codes.get() 
        proj_codes = self.get_codes(repeat_id)

        if write:
            self.logger.info(
                'Write permission granted:'
                ' - Will seek status of unknown project codes'
                ' - Will update status with "JobCancelled" for >24hr pending jobs'
            )

        done_set = {}
        extras = {'blacklist': {}}
        complete = 0

        # Summarising the blacklist reasons
        for code, reason in blacklist:
            if reason in extras['blacklist']:
                extras['blacklist'][reason].append(0)
            else:
                extras['blacklist'][reason] = [0]
            done_set[code] = True

        phases = {'init':{}, 'scan': {}, 'compute': {}, 'validate': {}}
        savecodes = []
        longest_err = 0
        for idx, p in enumerate(proj_codes):

            proj_op = ProjectOperation(
                self.workdir,
                p,
                groupID=self.groupID,
                logger=self.logger
            )


            try:
                if p not in done_set:
                    proj_dir = f'{args.workdir}/in_progress/{args.groupID}/{p}'
                    current = get_log_status(proj_dir)
                    if not current:
                        seek_unknown(proj_dir)
                        if 'unknown' in extras:
                            extras['unknown']['no data'].append(idx)
                        else:
                            extras['unknown'] = {'no data':[idx]}
                        continue
                    entry = current.split(',')
                    if len(entry[1]) > longest_err:
                        longest_err = len(entry[1])

                    if entry[1] == 'pending' and args.write:
                        timediff = (datetime.now() - force_datetime_decode(entry[2])).total_seconds()
                        if timediff > 86400: # 1 Day - fixed for now
                            entry[1] = 'JobCancelled'
                            log_status(entry[0], proj_dir, entry[1], FalseLogger())
                    
                    match_phase = (bool(args.phase) and args.phase == entry[0])
                    match_error = (bool(args.error) and any([err == entry[1].split(' ')[0] for err in args.error]))

                    if bool(args.phase) != (args.phase == entry[0]):
                        total_match = False
                    elif bool(args.error) != (any([err == entry[1].split(' ')[0] for err in args.error])):
                        total_match = False
                    else:
                        total_match = match_phase or match_error

                    if total_match:
                        if args.examine:
                            examine_log(args.workdir, p, entry[0], groupID=args.groupID, repeat_id=args.repeat_id, error=entry[1])
                        if args.new_id or args.blacklist:
                            savecodes.append(p)

                    merge_errs = True # Debug - add as argument later?
                    if merge_errs:
                        err_type = entry[1].split(' ')[0]
                    else:
                        err_type = entry[1]

                    if entry[0] == 'complete':
                        complete += 1
                    else:
                        if err_type in phases[entry[0]]:
                            phases[entry[0]][err_type].append(idx)
                        else:
                            phases[entry[0]][err_type] = [idx]
            except KeyboardInterrupt as err:
                raise err
            except Exception as err:
                examine_log(args.workdir, p, entry[0], groupID=args.groupID, repeat_id=args.repeat_id, error=entry[1])
                print(f'Issue with analysis of error log: {p}')
        num_codes  = len(proj_codes)
        print()
        print(f'Group: {args.groupID}')
        print(f'  Total Codes: {num_codes}')

        def summary_dict(pdict, num_codes, status_len=5, numbers=0):
            """Display summary information for a dictionary structure of the expected format."""
            for entry in pdict.keys():
                pcount = len(list(pdict[entry].keys()))
                num_types = sum([len(pdict[entry][pop]) for pop in pdict[entry].keys()])
                if pcount > 0:
                    print()
                    fmentry = format_str(entry,10, concat=False)
                    fmnum_types = format_str(num_types,5, concat=False)
                    fmcalc = format_str(f'{num_types*100/num_codes:.1f}',4, concat=False)
                    print(f'   {fmentry}: {fmnum_types} [{fmcalc}%] (Variety: {int(pcount)})')

                    # Convert from key : len to key : [list]
                    errkeys = reversed(sorted(pdict[entry], key=lambda x:len(pdict[entry][x])))
                    for err in errkeys:
                        num_errs = len(pdict[entry][err])
                        if num_errs < numbers:
                            print(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs} (IDs = {list(pdict[entry][err])})')
                        else:
                            print(f'    - {format_str(err, status_len+1, concat=True)}: {num_errs}')
        if not args.new_id:
            print()
            print('Pipeline Current:')
            if not args.long and longest_err > 30:
                longest_err = 30
            summary_dict(phases, num_codes, status_len=longest_err, numbers=int(args.numbers))
            print()
            print('Pipeline Complete:')
            print()
            complete_percent = format_str(f'{complete*100/num_codes:.1f}',4)
            print(f'   complete  : {format_str(complete,5)} [{complete_percent}%]')
            summary_dict(extras, num_codes, status_len=longest_err, numbers=0)
            print()

        if args.new_id:
            logger.debug(f'Preparing to write {len(savecodes)} codes to proj_codes/{args.new_id}.txt')
            if args.write:
                save_selection(savecodes, groupdir, args.new_id, logger, overwrite=args.overwrite)
            else:
                print('Skipped writing new codes - Write flag not present')

        if args.blacklist:
            logger.debug(f'Preparing to add {len(savecodes)} codes to the blacklist')
            if args.write:
                add_to_blacklist(savecodes, args.groupdir, args.reason, logger)
            else:
                print('Skipped blacklisting codes - Write flag not present')

    def get_operation(self, opt):
        if hasattr(self, opt):
            try:
                getattr(self, opt)()
            except TypeError as err:
                self.logger.error(
                    f'Attribute "{opt}" is not callable'
                )
                raise err
            except KeyboardInterrupt as err:
                raise err
            except Exception as err:
                examine_log(args.workdir, p, entry[0], groupID=args.groupID, repeat_id=args.repeat_id, error=entry[1])
                print(f'Issue with analysis of error log: {p}')
        else:
            self.logger.error(
                'Unrecognised operation type for EvaluationOperation.')
