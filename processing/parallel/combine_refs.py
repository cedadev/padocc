import os


PARTS = 'esacci7_parts'
FULL = 'esacci7_full'


# Combine metadatae into a single zmeta directory
if not os.path.isdir(f'batch/{FULL}'):
    os.makedirs(f'batch/{FULL}')
varnames = []
for dirname in os.listdir(f'batch/{PARTS}/batch0'):
    if dirname != '.zmetadata':
        try:
            os.makedirs(f'batch/{FULL}/{dirname}')
        except:
            pass
        varnames.append(dirname)
        
specials = {'lat':1, 'lon':1}
repeat = 76
#for varname in varnames:
if True:
    varname = 'time'
    print(varname)
    refid = 0
    if varname in specials:
        repeat = specials[varname]

    for index in range(repeat):
        directory = f'batch/{PARTS}/batch{index}/{varname}'
        for ref in os.listdir(directory):
            #if not os.path.isfile(f'batch/{FULL}/{varname}/refs.{refid}.parq'):
            os.system(f'cp {directory}/{ref} batch/{FULL}/{varname}/refs.{refid}.parq')
            refid += 1
