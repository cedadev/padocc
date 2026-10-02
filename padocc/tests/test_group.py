import os

from padocc import GroupOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

def get_groupA(workdir=WORKDIR):
    return GroupOperation('padocc-test-suite',workdir=workdir)

class TestGroup:
    # General
    def test_stac_representation(self, wd=WORKDIR):
        pass

    def test_info(self, wd=WORKDIR):
        groupA = get_groupA(workdir=wd)
        info = groupA.info()
        assert info['padocc-test-suite']['projects'] == 2

    # Allocations
    def test_allocations(self, wd=WORKDIR):
        pass

    def test_sbatch(self, wd=WORKDIR):
        pass

    # Evaluations
    def test_get_product(self, wd=WORKDIR):
        pass

    def test_repeat_by_status(self, wd=WORKDIR):
        pass

    def test_remove_by_status(self, wd=WORKDIR):
        pass

    def test_merge_subsets(self, wd=WORKDIR):
        pass

    def test_summarise_data(self, wd=WORKDIR):
        pass

    def test_summarise_status(self, wd=WORKDIR):
        pass
    
    # Modifiers
    def test_add_project(self, wd=WORKDIR):
        pass

    def test_remove_project(self, wd=WORKDIR):
        pass

    def test_transfer_project(self, wd=WORKDIR):

        tempA = GroupOperation('padocc-test-suite',workdir=wd, verbose=2, logid='A')
        tempB = GroupOperation('tempB',workdir=wd, verbose=2, logid='B')

        tempA.transfer_project('1DAgg_z',tempB)

        assert len(tempA) == 1
        assert len(tempB) == 1

        assert len(tempA.datasets) == 1
        assert len(tempB.datasets) == 1

        assert os.path.exists(f'{WORKDIR}/in_progress/tempB/1DAgg_z')
        assert not os.path.exists(f'{WORKDIR}/in_progress/padocc-test-suite/1DAgg_z')

    def test_merge(self, wd=WORKDIR):
        pass

    def test_unmerge(self, wd=WORKDIR):
        pass