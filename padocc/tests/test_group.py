import os

from padocc import GroupOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestGroup:
    def test_project_transfer(self, wd=WORKDIR):

        tempA = GroupOperation('padocc-test-suite',workdir=wd, verbose=2, logid='A')
        tempB = GroupOperation('tempB',workdir=wd, verbose=2, logid='B')

        tempA.transfer_project('0DAgg',tempB)

        assert len(tempA) == 2
        assert len(tempB) == 1

        assert len(tempA.datasets) == 2
        assert len(tempB.datasets) == 1

        assert os.path.exists(f'{WORKDIR}/in_progress/tempB/0DAgg')