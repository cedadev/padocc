from padocc.operations import GroupOperation
from padocc.phases import ScanOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestScan:
    def test_scan_basic(self, workdir=WORKDIR, verbose=1):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_scan_basic',
            verbose=verbose)

        process.run('scan')

    def test_scan_0DAgg(self, workdir=WORKDIR, verbose=1):
        groupID = 'padocc-test-suite'

        process = ScanOperation(
            '0DAgg',
            workdir=workdir,
            groupID=groupID,
            label='test_scan_0DAgg',
            verbose=verbose)

        process.run()

        print(f'Successful scan - results {process.proj_code}:')
        print(f' > Chunks: {process.detail_cfg["total_chunks"]}')
        print(f' > Format: {process.detail_cfg["type"]}')
        print(f' > Driver: {process.detail_cfg["driver"]}')
        print(f' > Variables: {process.detail_cfg["variable_count"]}')

    def test_scan_1DAgg(self, workdir=WORKDIR, verbose=1):
        groupID = 'padocc-test-suite'

        process = ScanOperation(
            '1DAgg',
            workdir=workdir,
            groupID=groupID,
            label='test_scan_1DAgg',
            verbose=verbose)

        process.run()

        print(f'Successful scan - results {process.proj_code}:')
        print(f' > Chunks: {process.detail_cfg["total_chunks"]}')
        print(f' > Format: {process.detail_cfg["type"]}')
        print(f' > Driver: {process.detail_cfg["driver"]}')
        print(f' > Variables: {process.detail_cfg["variable_count"]}')

    def test_scan_3DAgg(self, workdir=WORKDIR, verbose=1):
        groupID = 'padocc-test-suite'

        process = ScanOperation(
            '3DAgg',
            workdir=workdir,
            groupID=groupID,
            label='test_scan_3DAgg',
            verbose=verbose)

        process.run()

        print(f'Successful scan - results {process.proj_code}:')
        print(f' > Chunks: {process.detail_cfg["total_chunks"]}')
        print(f' > Format: {process.detail_cfg["type"]}')
        print(f' > Driver: {process.detail_cfg["driver"]}')
        print(f' > Variables: {process.detail_cfg["variable_count"]}')

if __name__ == '__main__':
    TestScan().test_scan_basic(verbose=0)
    TestScan().test_scan_0DAgg(verbose=0)
    TestScan().test_scan_1DAgg(verbose=0)
    TestScan().test_scan_3DAgg(verbose=0)