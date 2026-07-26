import pytest

# icechunk requires Python 3.12, while PADOCC supports 3.11, so it is an
# optional dependency and the icechunk format is unavailable on 3.11.
pytest.importorskip('icechunk')

from padocc import GroupOperation
from padocc.core.utils import BypassSwitch

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestIcechunkCompute:
    def test_compute_basic(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_compute',
            verbose=1)

        results = process.run('compute', mode='icechunk', forceful=True, bypass=BypassSwitch('D'), proj_code='1DAgg')

        assert results['Success'] == 1

    @pytest.mark.xfail(
        reason='Multi-dimensional aggregation is unsupported by the VirtualiZarr '
               'route: combine_nested needs a nested structure matching the concat '
               'dims, but virtualise passes a flat list. The PADOCC aggregator '
               'raises NotImplementedError for the same case. Affects the kerchunk '
               'VirtualiZarr aggregator equally - it just falls back to MZZ.',
        raises=ValueError, strict=True)
    def test_compute_3DAgg(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_compute',
            verbose=1)

        results = process.run('compute', mode='icechunk', forceful=True, bypass=BypassSwitch('D'), proj_code='3DAgg')

        assert results['Success'] == 1

if __name__ == '__main__':
    TestIcechunkCompute().test_compute_basic()
