# Script for viewing and editing current metadata
# Global attributes (.zattrs)
# .zgroup
# List all variables that contain a .zattrs/.zarray (inspectable)

import sys
import json

def format(text, size):
    newtext = str(text)
    for x in range(size - len(text)):
        newtext += ' '
    return newtext

def rundecode(cfgs):
    """
    cfgs - list of command inputs depending on user input to this program
    """
    flags = {
        '-f':'filename'
    }
    kwargs = {}
    for x in range(0,int(len(cfgs)),2):
        flag = flags[cfgs[x]]
        kwargs[flag] = cfgs[x+1]

    return kwargs

class Editor:

    def __init__(self, filename=None):
        self.filename=filename
        if not self.filename:
            self.filename = input('Kerchunk file: ')

        with open(self.filename) as f:
            self.refs = json.load(f)

        self.glob_attrs = json.loads(self.refs['refs']['.zattrs'])
        self.zgroup     = json.loads(self.refs['refs']['.zgroup'])

        self.variables = {}
        for key in self.refs['refs']:
            if '/.z' in key:
                var, type = key.split('/')
                if var not in self.variables:
                    self.variables[var] = {}
                self.variables[var][type] = self.refs['refs'][key]

    def display(self):
        print()
        print(self.filename)
        buffer = len(self.filename) - 35
        print('_'.join(['' for x in range(len(self.filename)+1)]))

        print('Global Attributes:')
        for k in self.zgroup.keys():
            print(f'    {format(k,30)}: {self.zgroup[k]}')
        for key in self.glob_attrs.keys():
            print(f'   {format(key,30)}: ',end='')
            lvalue = len(self.glob_attrs[key])
            cs = lvalue // 4
            if cs < 1:
                print(self.glob_attrs[key])
            else:
                raw = self.glob_attrs[key].replace('\n','. ')
                value = [raw[i:i+buffer] for i in range(0, lvalue, buffer)]
                print(value[0])
                for v in value[1:]:
                    print(format('',35) + v)
            

        print('Variables:')
        for key in self.variables.keys():
            print(f'    {key}')

if __name__ == '__main__':
    cfgs   = sys.argv[1:]
    Editor(**rundecode(cfgs)).display()