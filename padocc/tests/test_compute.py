from padocc.operations import GroupOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestCompute:
    def test_compute_basic(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_compute',
            verbose=1)

        results = process.run('compute', forceful=True)

        assert results['Success'] == 3

if __name__ == '__main__':
    #workdir = '/home/users/dwest77/cedadev/padocc/padocc/tests/auto_testdata_dir'
    TestCompute().test_compute_basic()#workdir=workdir)