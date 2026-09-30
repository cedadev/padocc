import os

from padocc.phases import ValidateOperation
from padocc.core.utils import BypassSwitch

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestValidate:
    def test_validate(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = ValidateOperation(
            '1DAgg',
            groupID=groupID,
            workdir=workdir,
            forceful=True, bypass=BypassSwitch('DS'), verbose=2)

        results = process.run()
        assert results == 'Warning' or results == 'Success'

        #assert results['Warning'] >= 1, "Expected warnings not present"

if __name__ == '__main__':
    #workdir = '/home/users/dwest77/cedadev/padocc/padocc/tests/auto_testdata_dir'
    TestValidate().test_validate() #workdir=workdir)