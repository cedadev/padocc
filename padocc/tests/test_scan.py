from padocc.operations import GroupOperation
from padocc.phases import ScanOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestScan:
    def test_scan_basic(self, workdir=WORKDIR, verbose=2):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_scan_basic',
            verbose=verbose)

        results = process.run('scan', forceful=True)

        assert results['Success'] == 3

    def test_scan_0DAgg(self, workdir=WORKDIR, verbose=1):
        groupID = 'padocc-test-suite'

        process = ScanOperation(
            '0DAgg',
            workdir=workdir,
            groupID=groupID,
            label='test_scan_0DAgg',
            verbose=verbose)

        status = process.run(forceful=True, thorough=True)

        print(f'Successful scan - results {process.proj_code}:')
        print(f' > Chunks: {process.detail_cfg["chunk_info"]}')
        print(f' > Format: {process.detail_cfg["type"]}')
        print(f' > Driver: {process.detail_cfg["driver"]}')
        print(f' > Data Properties: {process.base_cfg["data_properties"]}')

        assert status == 'Success'

    def test_scan_1DAgg(self, workdir=WORKDIR, verbose=1):
        groupID = 'padocc-test-suite'

        process = ScanOperation(
            '1DAgg',
            workdir=workdir,
            groupID=groupID,
            label='test_scan_1DAgg',
            verbose=verbose)

        status = process.run(forceful=True,thorough=True)

        print(f'Successful scan - results {process.proj_code}:')
        print(f' > Chunks: {process.detail_cfg["chunk_info"]}')
        print(f' > Format: {process.detail_cfg["type"]}')
        print(f' > Driver: {process.detail_cfg["driver"]}')
        print(f' > Data Properties: {process.base_cfg["data_properties"]}')

        assert status == 'Success'

    def test_scan_3DAgg(self, workdir=WORKDIR, verbose=1):
        groupID = 'padocc-test-suite'

        process = ScanOperation(
            '3DAgg',
            workdir=workdir,
            groupID=groupID,
            label='test_scan_3DAgg',
            verbose=verbose)

        status = process.run(forceful=True, thorough=True)

        print(f'Successful scan - results {process.proj_code}:')
        print(f' > Chunks: {process.detail_cfg["chunk_info"]}')
        print(f' > Format: {process.detail_cfg["type"]}')
        print(f' > Driver: {process.detail_cfg["driver"]}')
        print(f' > Data Properties: {process.base_cfg["data_properties"]}')

        assert status == 'Success'

if __name__ == '__main__':
    TestScan().test_scan_basic(verbose=1)
    TestScan().test_scan_0DAgg(verbose=1)
    TestScan().test_scan_1DAgg(verbose=1)
    TestScan().test_scan_3DAgg(verbose=0)