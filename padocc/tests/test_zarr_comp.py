from padocc import GroupOperation
from padocc.core.utils import BypassSwitch

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestZarrCompute:
    def test_compute_basic(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_compute',
            verbose=1)

        results = process.run('compute', mode='zarr', forceful=True, bypass=BypassSwitch('D'), proj_code='1DAgg')

        assert results['Success'] == 1

if __name__ == '__main__':
    #workdir = '/home/users/dwest77/cedadev/padocc/padocc/tests/auto_testdata_dir'
    TestZarrCompute().test_compute_basic()#workdir=workdir)