from padocc.operations import GroupOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

infile  = 'padocc/tests/data/myfile.csv' 
# Input CSV has Identifier, Path/To/Datasets, {updates}, {removals}

groupID = 'padocc-test-suite'
workdir = '/home/username/padocc-workdir'

mygroup = GroupOperation(
    groupID,
    workdir=workdir,
    label='test_group'
)

mygroup.init_from_file(infile)


class TestInit:

    def test_init_basic(self, wd=WORKDIR):
        infile  = 'padocc/tests/data/myfile.csv'
        groupID = 'padocc-test-suite'

        workdir = wd

        kwargs = {}

        substitutions = {
            'init_file': {
                '/home/users/dwest77/cedadev/padocc/':''
            },
            'dataset_file': {
                '/home/users/dwest77/cedadev/padocc/':''
            },
            'datasets': {
                '/home/users/dwest77/cedadev/padocc/':''
            },
        }

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_init')

        process.init_from_file(infile, substitutions=substitutions)

if __name__ == '__main__':
    TestInit().test_init_basic()