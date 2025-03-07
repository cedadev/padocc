import os

from padocc import GroupOperation
from padocc.core.utils import BypassSwitch

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestValidate:
    def test_validate(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir)

        results = process.run('validate', forceful=True, bypass=BypassSwitch('DS'), verbose=2)

        assert results['Warning'] >= 1, "Expected warnings not present"

if __name__ == '__main__':
    #workdir = '/home/users/dwest77/cedadev/padocc/padocc/tests/auto_testdata_dir'
    TestValidate().test_validate() #workdir=workdir)