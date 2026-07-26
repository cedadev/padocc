import os

import pytest

# icechunk requires Python 3.12, while PADOCC supports 3.11, so it is an
# optional dependency and the icechunk format is unavailable on 3.11. This
# must precede every import below that reaches icechunk.
pytest.importorskip('icechunk')

import icechunk as ic

from padocc import ProjectOperation
from padocc.core.utils import BypassSwitch
from padocc.phases import IcechunkDS
from padocc.phases.icechunk_store import (container_prefixes, open_virtual_repo,
                                          store_config)

WORKDIR = 'padocc/tests/auto_testdata_dir'

class TestIcechunkPrefixes:
    """
    Virtual chunk containers are keyed on a URL prefix. Getting the prefix
    wrong produces a store that commits cleanly and then cannot resolve a
    single chunk, so these are checked directly.
    """

    def test_one_prefix_per_directory(self):
        prefixes = container_prefixes(
            ['/data/agg/file0.nc', '/data/agg/file1.nc', '/data/agg/file2.nc'])

        assert prefixes == ['file:///data/agg/']

    def test_separate_prefix_per_directory(self):
        prefixes = container_prefixes(['/data/a/file0.nc', '/data/b/file1.nc'])

        assert prefixes == ['file:///data/a/', 'file:///data/b/']

    def test_relative_paths_are_absolute(self):
        """
        VirtualiZarr normalises manifest paths against the working directory,
        so the container prefix has to be absolute to match them.
        """
        prefixes = container_prefixes(['padocc/tests/data_creator/1DAgg/file0.nc'])

        assert prefixes == [f'file://{os.getcwd()}/padocc/tests/data_creator/1DAgg/']

    def test_file_scheme_is_preserved(self):
        assert container_prefixes(['file:///data/agg/file0.nc']) == ['file:///data/agg/']

    def test_http_prefix_is_per_host(self):
        prefixes = container_prefixes([
            'https://dap.ceda.ac.uk/badc/one.nc',
            'https://dap.ceda.ac.uk/badc/other/two.nc',
        ])

        assert prefixes == ['https://dap.ceda.ac.uk/']

    def test_unsupported_scheme_is_explicit(self):
        with pytest.raises(NotImplementedError):
            store_config(container_prefixes(['s3://bucket/key.nc']))

    def test_every_container_is_authorised(self):
        prefixes = container_prefixes(['/data/a/file0.nc', '/data/b/file1.nc'])
        _, credentials = store_config(prefixes)

        assert sorted(credentials.keys()) == prefixes

    def test_authorisation_matches_the_scheme(self):
        """
        Each container must be authorised with the sentinel for its scheme.

        Icechunk rejects a credential that does not match the container's
        store, and neither sentinel carries any secret, so the wrong one is
        easy to write and only fails at read time - well after the store has
        committed successfully. Mixed schemes in one call, so a hardcoded
        single sentinel cannot pass.
        """
        prefixes = container_prefixes([
            '/data/local/file0.nc',
            'https://dap.ceda.ac.uk/badc/file1.nc',
            'http://example.org/file2.nc',
        ])
        _, credentials = store_config(prefixes)

        assert sorted(credentials.keys()) == prefixes

        assert isinstance(
            credentials['file:///data/local/'],
            ic.Credentials.LocalFileSystemAccess)
        assert isinstance(
            credentials['https://dap.ceda.ac.uk/'],
            ic.Credentials.HttpAccess)
        assert isinstance(
            credentials['http://example.org/'],
            ic.Credentials.HttpAccess)

    def test_authorisation_is_never_none(self):
        """
        Passing None in place of a sentinel is deprecated in icechunk 2.x and
        is slated to be rejected outright, so no container may carry it.
        """
        prefixes = container_prefixes(
            ['/data/local/file0.nc', 'https://dap.ceda.ac.uk/badc/file1.nc'])
        _, credentials = store_config(prefixes)

        assert all(v is not None for v in credentials.values()), credentials


class TestIcechunkStoreLayout:
    """
    Checks against the store written by TestIcechunkCompute.
    """

    def _store(self, workdir=WORKDIR):
        project = ProjectOperation(
            '1DAgg', workdir, groupID='padocc-test-suite')
        project.cloud_format = 'icechunk'
        return project.dataset

    def test_store_is_a_repository(self):
        """The output is a repository directory, not a single file."""
        store_path = self._store().store_path

        assert os.path.isdir(store_path)
        for member in ('snapshots', 'manifests'):
            assert os.path.isdir(f'{store_path}/{member}'), f'missing {member}'

    def test_rerun_reuses_the_repository(self, workdir=WORKDIR):
        """
        Recomputing an existing project must succeed rather than fail on the
        existing repository, as Icechunk's Repository.create would.
        """
        compute = IcechunkDS(
            '1DAgg', workdir, groupID='padocc-test-suite',
            thorough=True, verbose=0)

        assert compute.run(mode='icechunk', bypass=BypassSwitch('D')) == 'Success'

    def test_config_survives_a_fresh_open(self):
        """
        Virtual chunk containers are only resolvable if save_config persisted
        them; a fresh Repository.open is the only way to catch that.
        """
        store = self._store()
        repo = open_virtual_repo(store.store_path, store.allfiles)

        containers = repo.config.virtual_chunk_containers

        assert containers, 'no virtual chunk containers persisted in the store'
        assert len(list(repo.ancestry(branch='main'))) >= 1

if __name__ == '__main__':
    TestIcechunkPrefixes().test_one_prefix_per_directory()
    TestIcechunkStoreLayout().test_store_is_a_repository()
