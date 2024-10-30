import os

class TestSetup:
    def test_setup(self):
        os.system('mkdir auto_testdata_dir')
        assert os.path.isdir('auto_testdata_dir')

class TestCleanup:

    def test_cleanup(self):
        os.system('rm -rf auto_testdata_dir')
        assert not os.path.isdir('auto_testdata_dir')