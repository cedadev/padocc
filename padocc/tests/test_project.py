import os

from padocc import ProjectOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestProject:

    # General
    def test_info(self, wd=WORKDIR):
        assert False

    def test_version(self, wd=WORKDIR):
        assert False

    # Dataset
    def test_dataset(self, wd=WORKDIR):
        assert False

    def test_ds_attributes(self, wd=WORKDIR):
        assert False

    def test_kfile(self, wd=WORKDIR):
        assert False

    def test_kstore(self, wd=WORKDIR):
        assert False

    def test_cfa_dataset(self, wd=WORKDIR):
        assert False

    def test_zstore(self, wd=WORKDIR):
        assert False

    def test_update_attribute(self, wd=WORKDIR):
        assert False

    # Status

    def test_last_run(self, wd=WORKDIR):
        assert False

    def test_last_status(self, wd=WORKDIR):
        assert False

    def test_log_contents(self, wd=WORKDIR):
        assert False

    # Properties

    def test_properties(self, wd=WORKDIR):
        assert False