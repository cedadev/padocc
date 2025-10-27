from padocc import ProjectOperation, GroupOperation
import os
import pdb
import bdb

def main():
    workdir=os.environ.get('WORKDIR',None)

    try:
        pdb.set_trace()
    except bdb.BdbQuit as err:
        pass


if __name__ == '__main__':
    main()