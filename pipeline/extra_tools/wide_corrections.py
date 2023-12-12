
import json
import glob
import os

with open('corrections.json') as f:
    refs = json.load(f)

workdir = '/gws/nopw/j04/esacci_portal/kerchunk/DELIVERY_09_10_23'

for proj in refs.keys():
    kfile = glob.glob(f'{workdir}/{proj}*.json')[0]
    newfile = kfile.replace('DELIVERY_09_10_23','DELIVERY_10_10_23')
    if not os.path.isfile(newfile):
    
        with open(kfile) as f:
            kdata = json.load(f)
        zattrs = json.loads(kdata['refs']['.zattrs'])
        new_zattrs = {}
        for attr in zattrs:
            if attr in refs[proj].keys():
                if refs[proj][attr] == 'remove':
                    pass 
                else:
                    new_zattrs[attr] = refs[proj][attr]
            else:
                new_zattrs[attr] = zattrs[attr]]
                

        kdata['refs']['.zattrs'] = json.dumps(new_zattrs)
        with open(newfile,'w') as f:
            f.write(json.dumps(kdata))
        print('Written corrections to ', newfile)
    else:
        print('File exists - ',newfile)

print('End')



"""
with open('corrections.csv') as f:
    content = f.readlines()

def get_portion(raw, proj_id):
    raw = raw.replace('\n','').replace('\t','')
    if raw.replace(' ','') == 'id':
        return {'id':proj_id}
    else:
        if '=' in raw:
            attr = raw.split('=')[0].replace(' ','')
            value = raw.split('=')[1]
        else:
            attr = 'id'
            value = proj_id
        if '"' in value:
            # Loop and cut everything not inside ""
            x = 0
            inside = False
            reached = False
            fval = ''
            while not reached:
                if value[x] == '"' and inside:
                    inside = False
                    reached = True
                elif value[x] == '"':
                    inside = True
                else:
                    pass
                if inside and value[x] != '"':
                    fval += value[x]
                x += 1
            value = fval
        else:
            value = value.replace(' ','')
        return {attr: value}

projects = {}
for x, line in enumerate(content):
    print(x, len(content))
    if line.startswith('ESACCI'):
        proj_id = ''
        x=0
        while line[x:x+3] != '-kr':
            proj_id += line[x]
            x += 1
        projects[proj_id] = {**get_portion(line[x:], proj_id)}
    else:
        projects[proj_id] = {**projects[proj_id], **get_portion(line, proj_id)}
        
with open('corrections.json','w') as f:
    f.write(json.dumps(projects))


"""