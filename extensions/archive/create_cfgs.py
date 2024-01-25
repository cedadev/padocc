import os

workdir = '/gws/nopw/j04/esacci_portal/kerchunk'

revision = input('Enter revision: ')

with open(f'{workdir}/filesets.csv') as f:
    contents = [r.replace('\n','') for r in f.readlines()]
for x, line in enumerate(contents):
    pattern   = line.split(',')[0]
    proj_code = line.split(',')[1]

    with open(f'{workdir}/temp/configs/{x}.txt','w') as f:
        f.write(f'{pattern},{proj_code},{revision}')
