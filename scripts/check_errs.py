import os
import sys
import argparse

def check_errs(path, showtype=None):
    errs = {'None':0}
    for efile in os.listdir(path):
        if os.path.isfile(os.path.join(path, efile)):
            with open(os.path.join(path, efile)) as f:
                q = [r.strip() for r in f.readlines()]
            if len(q) > 0:
                if type(q[-1]) == str:
                    key = q[-1].split(':')[0]
                else:
                    key = q[-1][0]
                if key in errs:
                    errs[key] += 1
                else:
                    errs[key] = 1
                if key == showtype:
                    print(efile)
                    print()
                    for line in q:
                        print(line)
                    x=input()
            else:
                errs['None'] += 1

    for key in errs.keys():
        print(f'{key}: {errs[key]}')

def check_outs(path):
    outs = {'None':0}
    for ofile in os.listdir(path):
        if os.path.isfile(os.path.join(path, ofile)):
            with open(os.path.join(path, ofile)) as f:
                q = [r.strip() for r in f.readlines()]
            if len(q) > 0:
                key = q[-1]
                if type(q[-1]) == str:
                    key = q[-1].split(':')[0]
                else:
                    key = q[-1][0]
                if 'detail-cfg' in key:
                    key = 'detail-cfg'
                if 'base-cfg' in key:
                    key = 'base-cfg'
                if key in outs:
                    outs[key] += 1
                    if 'metadata' in key:
                        print(os.path.join(path, ofile))
                        x=input()
                else:
                    outs[key] = 1
            else:
                outs['None'] += 1

    for key in outs.keys():
        print(f'{key}: {outs[key]}')

# 13569810
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a pipeline step for a single dataset')
    parser.add_argument('groupID',type=str, help='Group identifier code')
    parser.add_argument('jobID',type=str, help='Identifier of job to inspect')

    parser.add_argument('-e','--error',    dest='showtype',      help='Display a specific error in more detail')
    args = parser.parse_args()

    path = f'/gws/nopw/j04/cmip6_prep_vol1/kerchunk-pipeline/groups/{args.groupID}/errs/{args.jobID}/'
    print(f'Checking errs/outs for {args.groupID} ID: {args.jobID}')

    print('Errors: ')
    check_errs(path, showtype=args.showtype)
    print('')
    print('Outputs:')
    check_outs(path)
        