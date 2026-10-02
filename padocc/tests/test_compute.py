from padocc.phases.compute import KerchunkDS

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestCompute:
    def test_compute_kerchunk(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = KerchunkDS(
            '1DAgg',
            workdir=workdir,
            groupID=groupID,
            label='test_compute',
            verbose=2,
            thorough=True,
            forceful=True)

        results = process.run()

        assert results == 'Success'

if __name__ == '__main__':
    #workdir = '/home/users/dwest77/cedadev/padocc/padocc/tests/auto_testdata_dir'
    TestCompute().test_compute_kerchunk()#workdir=workdir)