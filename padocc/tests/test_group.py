import os

from padocc import GroupOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

def get_groupA(workdir=WORKDIR):
    return GroupOperation('groupA',workdir=workdir)

class TestGroup:
    # General
    def test_stac_representation(self, wd=WORKDIR):
        assert False

    def test_info(self, wd=WORKDIR):
        groupA = get_groupA(workdir=wd)
        info = groupA.info()
        assert info['groupA']['projects'] == 2

    # Allocations
    def test_allocations(self, wd=WORKDIR):
        assert False

    def test_sbatch(self, wd=WORKDIR):
        assert False

    # Evaluations
    def test_get_product(self, wd=WORKDIR):
        assert False

    def test_repeat_by_status(self, wd=WORKDIR):
        assert False

    def test_remove_by_status(self, wd=WORKDIR):
        assert False

    def test_merge_subsets(self, wd=WORKDIR):
        assert False

    def test_summarise_data(self, wd=WORKDIR):
        assert False

    def test_summarise_status(self, wd=WORKDIR):
        assert False
    
    # Modifiers
    def test_add_project(self, wd=WORKDIR):
        assert False

    def test_remove_project(self, wd=WORKDIR):
        assert False

    def test_transfer_project(self, wd=WORKDIR):

        tempA = GroupOperation('padocc-test-suite',workdir=wd, verbose=2, logid='A')
        tempB = GroupOperation('tempB',workdir=wd, verbose=2, logid='B')

        tempA.transfer_project('0DAgg',tempB)

        assert len(tempA) == 2
        assert len(tempB) == 1

        assert len(tempA.datasets) == 2
        assert len(tempB.datasets) == 1

        assert os.path.exists(f'{WORKDIR}/in_progress/tempB/0DAgg')

    def test_merge(self, wd=WORKDIR):
        assert False

    def test_unmerge(self, wd=WORKDIR):
        assert False