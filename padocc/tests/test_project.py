import os

import xarray

import padocc.core.filehandlers as fhs
from padocc import ProjectOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestProject:

    zarrds = ProjectOperation(
        '1DAgg', 
        workdir=WORKDIR, 
        groupID='padocc-test-suite'
    )

    kds = ProjectOperation(
        '3DAgg', 
        workdir=WORKDIR, 
        groupID='padocc-test-suite'
    )

    # General
    def test_info(self, wd=WORKDIR):
        resp = self.zarrds.info()

        assert isinstance(resp, dict)
        assert resp['1DAgg'].get('File count') == 8

    def test_version(self, wd=WORKDIR):

        version = self.zarrds.version_no
        revision = self.zarrds.revision

        assert version == '1.1', 'Version not found'
        assert version in revision, 'Version does not match revision'

    # Dataset
    def test_dataset(self, wd=WORKDIR):

        ds = self.zarrds.dataset
        dstype = isinstance(ds, fhs.FileIOMixin) or isinstance(ds, fhs.GenericStore)
        assert dstype, 'Dataset property not found'

    def test_ds_attributes(self, wd=WORKDIR):
        
        attrs = self.zarrds.dataset_attributes
        assert isinstance(attrs, dict), "Attributes retrieval was unsuccessful"

    def test_kfile(self, wd=WORKDIR):

        kfile = self.kds.kfile
        kfile_m = kfile.get_meta()

        assert isinstance(kfile_m, dict), "Kfile metadata retrieval was unsuccessful."

        ds = kfile.open_dataset()
        assert isinstance(ds, xarray.Dataset), "Kerchunk Dataset could not be opened"

        #assert False, "Add download link test"
        #assert False, "Add kerchunk history test"
        #assert False, "Spawn Copy test"
        #assert False, "Open dataset test"
        #assert False, "Set metadata test"


    def test_kstore(self, wd=WORKDIR):

        assert True
        #assert False, "Kstore testing is not implemented"

        #kstore = self.project.kstore
        #kstore_m = kstore.get_meta()
        #assert isinstance(kstore_m, dict), "Kstore metadata retrieval was unsuccessful."

    def test_cfa_dataset(self, wd=WORKDIR):
        
        cfa_ds = self.zarrds.cfa_dataset
        assert isinstance(cfa_ds, fhs.CFADataset), "CFA Dataset is not accessible"

        ds = cfa_ds.open_dataset()
        assert isinstance(ds, xarray.Dataset), "CFA Dataset could not be opened"

    def test_zstore(self, wd=WORKDIR):
        
        zstore = self.zarrds.zstore
        assert isinstance(zstore, fhs.ZarrStore), "Zarr Dataset is not accessible"

        ds = zstore.open_dataset()
        assert isinstance(ds, xarray.Dataset), "Zarr Dataset could not be opened"

    def test_update_attribute(self, wd=WORKDIR):
        assert True

    def test_last_status(self, wd=WORKDIR):
        ls = self.zarrds.get_last_status().split(',')
        assert ls[1] == 'Success'

    def test_log_contents(self, wd=WORKDIR):
        lc = self.zarrds.get_log_contents('scan')

        assert len(lc) > 0, "Log file empty"

if __name__ == '__main__':
    testp = TestProject()

    testp.test_info()
    testp.test_version()
    testp.test_dataset()
    testp.test_ds_attributes()
    testp.test_kfile()
    testp.test_kstore()
    testp.test_cfa_dataset()
    testp.test_zstore()
    testp.test_update_attribute()
    testp.test_last_status()
    testp.test_log_contents()