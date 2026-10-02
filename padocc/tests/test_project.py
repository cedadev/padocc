import os

import xarray

import padocc.core.filehandlers as fhs
from padocc import ProjectOperation

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestProject:

    def test_zarr(self, wd=WORKDIR):

        zarrds = ProjectOperation(
            '1DAgg_z', 
            workdir=WORKDIR, 
            groupID='padocc-test-suite'
        )

        resp = zarrds.info()
        
        assert isinstance(resp, dict)
        assert resp['1DAgg_z'].get('File count') == 8

        version = zarrds.version_no
        revision = zarrds.revision

        assert version == '1.0', f'Version {version}'
        assert version in revision, 'Version does not match revision'

        ds = zarrds.dataset
        dstype = isinstance(ds, fhs.FileIOMixin) or isinstance(ds, fhs.GenericStore)
        assert dstype, 'Dataset property not found'

        attrs = zarrds.dataset_attributes
        assert isinstance(attrs, dict), "Attributes retrieval was unsuccessful"

        cfa_ds = zarrds.cfa_dataset
        assert isinstance(cfa_ds, fhs.CFADataset), "CFA Dataset is not accessible"

        ds = cfa_ds.open_dataset()
        assert isinstance(ds, xarray.Dataset), "CFA Dataset could not be opened"

        zstore = zarrds.zstore
        assert isinstance(zstore, fhs.ZarrStore), "Zarr Dataset is not accessible"

        ds = zstore.open_dataset()
        assert isinstance(ds, xarray.Dataset), "Zarr Dataset could not be opened"

        ls = zarrds.get_last_status().split(',')
        assert 'Warn' in ls[1] or 'Success' in ls[1], "Failed Validation in testing"

        lc = zarrds.get_log_contents('scan')
        
        assert len(lc) > 0, "Log file empty"

    def test_kerchunk(self, wd=WORKDIR):

        kds = ProjectOperation(
            '1DAgg', 
            workdir=WORKDIR, 
            groupID='padocc-test-suite'
        )

        kfile = kds.kfile
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

        pass
        #assert False, "Kstore testing is not implemented"

        #kstore = self.project.kstore
        #kstore_m = kstore.get_meta()
        #assert isinstance(kstore_m, dict), "Kstore metadata retrieval was unsuccessful."


if __name__ == '__main__':
    testp = TestProject()