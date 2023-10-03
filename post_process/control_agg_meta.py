import os

workdir = '/gws/nopw/j04/esacci_portal/kerchunk'

with open(f'{workdir}/filesets.csv') as f:
    contents = [r.replace('\n','') for r in f.readlines()]

cfg = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/temp/lccs_cfg.txt'

for line in contents:
    proj_code = line.split(',')[1]

    old = 'kr1.1'
    new = 'kr1.1'
    print(proj_code)
    os.system(f'python correct_meta.py {proj_code}-{old} {old} {new} {workdir} {cfg}')
    #x=input()