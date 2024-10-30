from padocc.operations import GroupOperation

WORKDIR = 'auto_testdata_dir'

class TestScan:
    def test_scan_basic(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_scan',
            verbose=1)

        process.run('scan')

if __name__ == '__main__':
    TestScan().test_scan_basic()