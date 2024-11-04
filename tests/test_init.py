from padocc.operations import GroupOperation

WORKDIR = 'auto_testdata_dir'

class TestInit:

    def test_init_basic(self, wd=WORKDIR):
        infile  = 'tests/data/myfile.csv'
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
        }

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_init')

        process.init_from_file(infile, substitutions=substitutions)

if __name__ == '__main__':
    TestInit().test_init_basic()