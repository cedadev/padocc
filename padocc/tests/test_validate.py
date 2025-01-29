from padocc import GroupOperation

from padocc.core.utils import BypassSwitch
import os

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestValidate:
    def test_validate(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_validate',
            verbose=1)
        
        assert os.path.exists(process.groupdir)
        assert len(process) >= 2

        results = process.run('validate', forceful=True, bypass=BypassSwitch('DS'), verbose=2)

        print(results)
        assert 'Fatal' in results

if __name__ == '__main__':
    #workdir = '/home/users/dwest77/cedadev/padocc/padocc/tests/auto_testdata_dir'
    TestValidate().test_validate()#workdir=workdir)