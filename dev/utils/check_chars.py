import json

zm1 = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/test_parqs/parqs/esacci24_start/.zmetadata'
zm2 = '/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/test_parqs/parqs/esacci24/.zmetadata'

with open(zm1) as f:
    refs1 = json.load(f)

with open(zm2) as f:
    refs2 = json.load(f)

check_keys = ['shape','chunks','compressor']

def recursive_dict_check(d1, d2, superkey, depth):
    if depth > 4:
        return ''
    returns = ''
    req = False
    #print(superkey)
    for key in d1.keys():
        if key not in d2:
            returns += f'{superkey} ! {key}\n'
        else:
            if d1[key] != d2[key]:
                if type(d1[key]) == dict:
                    returns += recursive_dict_check(d1[key],d2[key],f'{superkey}.{key}', depth+1)
                else:
                    returns += f'{superkey} : {key} <{d1[key]} != {d2[key]}>\n'
                    if key not in check_keys:
                        returns += f'REQUIRED {key}'
                        req = True
    return returns, req

response, req = recursive_dict_check(refs1, refs2, '',0)