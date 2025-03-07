from padocc import GroupOperation
from padocc.core.utils import BypassSwitch

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestZarrValidate:
    def test_validate(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_validate',
            verbose=1)

        results = process.run('validate', mode='zarr', forceful=True, bypass=BypassSwitch('DS'),proj_code='1DAgg')

        print(results)

if __name__ == '__main__':
    #workdir = '/home/users/dwest77/cedadev/padocc/padocc/tests/auto_testdata_dir'
    TestZarrValidate().test_validate()#workdir=workdir)