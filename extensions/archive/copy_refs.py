import os


PATH = '/home/users/dwest77/Documents/kerchunk_dev/parquet/dev/batch'

PARTS = f'{PATH}/gargant_parts'
FULL = f'{PATH}/gargant'


# Combine metadatae into a single zmeta directory
#if not os.path.isdir(f'batch/{FULL}'):
    #os.makedirs(f'batch/{FULL}')
varnames = []
for dirname in os.listdir(f'{PARTS}/batch0'):
    if dirname != '.zmetadata':
        try:
            os.makedirs(f'{FULL}/{dirname}')
        except:
            pass
        varnames.append(dirname)
        
specials = {'lat':1, 'lon':1}
repeat = 2
for varname in varnames:
#if True:
    #varname = 'time'
    print(varname)
    refid = 0
    if varname in specials:
        repeat = specials[varname]

    for index in range(repeat):
        directory = f'{PARTS}/batch{index}/{varname}'
        for ref in os.listdir(directory):
            #if not os.path.isfile(f'batch/{FULL}/{varname}/refs.{refid}.parq'):
            os.system(f'cp {directory}/{ref} {FULL}/{varname}/refs.{refid}.parq')
            refid += 1
