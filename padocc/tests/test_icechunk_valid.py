from glob import glob

import pytest
import xarray as xr

# icechunk requires Python 3.12, while PADOCC supports 3.11, so it is an
# optional dependency and the icechunk format is unavailable on 3.11.
pytest.importorskip('icechunk')

from padocc import GroupOperation, ProjectOperation
from padocc.core.filehandlers import IcechunkStore
from padocc.core.logs import init_logger
from padocc.core.utils import BypassSwitch
from padocc.phases.aggregate import virtualise_icechunk

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestIcechunkValidate:
    def test_validate(self, workdir=WORKDIR):
        groupID = 'padocc-test-suite'

        process = GroupOperation(
            groupID,
            workdir=workdir,
            label='test_validate',
            verbose=1)

        results = process.run('validate', mode='icechunk', forceful=True, bypass=BypassSwitch('DS'), proj_code='1DAgg')

        # The 1DAgg fixtures carry deliberately mismatched attributes on some
        # files, so validation reports Warning rather than Success once the
        # metadata report exists - as it does for kerchunk in TestValidate.
        # What matters is that the store validated without failing.
        assert results.get('Success', 0) + results.get('Warning', 0) == 1, results
        assert not [k for k in results if 'Fatal' in k or 'Failed' in k], results

    def test_virtual_chunks_resolve(self, workdir=WORKDIR):
        """
        Read the store back and compare against the source files.

        An Icechunk store holding virtual references can commit successfully and
        still be unreadable, because chunk resolution depends on container
        configuration that nothing up to this point exercises. Calling .load()
        is the assertion: it forces Icechunk to fetch the bytes from the source
        NetCDF files rather than from the store.
        """
        project = ProjectOperation(
            '1DAgg', workdir, groupID='padocc-test-suite')
        project.cloud_format = 'icechunk'

        test = project.dataset.open_dataset()

        control = xr.open_mfdataset(
            sorted(glob('padocc/tests/data_creator/1DAgg/*.nc')),
            combine='nested', concat_dim='time', data_vars='minimal')

        assert test['rain'].shape == control['rain'].shape

        xr.testing.assert_allclose(test['rain'].load(), control['rain'].load())
        xr.testing.assert_allclose(test['lat_projection'].load(),
                                   control['lat_projection'].load())

    def test_corrected_metadata_is_serialised(self, tmp_path, workdir=WORKDIR):
        """
        The reconciled global attributes must reach the store.

        combine_nested(combine_attrs='override') takes the first file's
        attributes verbatim, discarding what _correct_metadata worked out
        across the whole fileset, so the aggregator has to reapply them.

        Uses a marker absent from every cached reference, so the assertion
        fails if the corrected attributes are dropped rather than passing on
        attributes that happened to be identical anyway.
        """
        project = ProjectOperation(
            '1DAgg', workdir, groupID='padocc-test-suite')
        allfiles = project.allfiles.get()

        corrected = {'Conventions': 'DW-0.1', 'aggregation_marker': 'corrected'}
        store = IcechunkStore(str(tmp_path), 'attrs_probe', allfiles=allfiles)

        virtualise_icechunk(
            f'{project.dir}/cache/',
            store_path=store.store_path,
            agg_dims=['time'],
            data_vars=['rain'],
            nfiles=len(allfiles),
            logger=init_logger(0, 'test_attrs'),
            allfiles=allfiles,
            zattrs=corrected,
        )

        attrs = store.open_dataset().attrs

        assert attrs.get('aggregation_marker') == 'corrected', dict(attrs)

    def test_dropped_variables_are_absent(self, workdir=WORKDIR, tmp_path=None):
        """
        keep_vars must carry through to the store.

        _drop_vars edits the per-file references before they are cached, so
        the dropped variables should never reach the chunk manifest.
        """
        project = ProjectOperation(
            '1DAgg', workdir, groupID='padocc-test-suite')
        project.base_cfg['keep_vars'] = ['rain']
        project.base_cfg.save()

        try:
            process = GroupOperation(
                'padocc-test-suite', workdir=workdir, label='test_keep_vars', verbose=0)
            results = process.run(
                'compute', mode='icechunk', forceful=True, thorough=True,
                bypass=BypassSwitch('D'), proj_code='1DAgg')

            assert results.get('Success') == 1, results

            reread = ProjectOperation('1DAgg', workdir, groupID='padocc-test-suite')
            reread.cloud_format = 'icechunk'
            variables = set(reread.dataset.open_dataset().variables)

            assert 'rain' in variables
            assert 'lat_projection' not in variables, variables
            assert 'height' not in variables, variables
        finally:
            project = ProjectOperation(
                '1DAgg', workdir, groupID='padocc-test-suite')
            project.base_cfg['keep_vars'] = 'all'
            project.base_cfg.save()

if __name__ == '__main__':
    TestIcechunkValidate().test_validate()
    TestIcechunkValidate().test_virtual_chunks_resolve()
