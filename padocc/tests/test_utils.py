import os

class TestSetup:
    def test_setup(self):
        os.system('mkdir padocc/tests/auto_testdata_dir')
        assert os.path.isdir('padocc/tests/auto_testdata_dir')

class TestCleanup:

    def test_cleanup(self):
        os.system('rm -rf padocc/tests/auto_testdata_dir')
        assert not os.path.isdir('padocc/tests/auto_testdata_dir')