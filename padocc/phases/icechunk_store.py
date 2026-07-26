"""
Icechunk helpers for PADOCC.

An Icechunk store holding *virtual* references does not contain the chunk bytes;
it records, for every chunk, the URL of the source file and a byte range. For
those URLs to be resolvable, the repository needs a
``VirtualChunkContainer`` whose ``url_prefix`` matches them, and credentials
authorising access to that prefix. Both are needed at write time *and* again,
separately, at read time.

The functions here derive that configuration from the source file list, so a
caller only has to pass the files it already has.

Note the two similarly-named Icechunk factories, which are easy to confuse:

* ``icechunk.local_filesystem_storage`` -- where the *repository* lives.
* ``icechunk.local_filesystem_store``   -- where the *source data* lives, i.e.
  what goes inside a ``VirtualChunkContainer``.
"""

import os
from typing import Union
from urllib.parse import urlparse

import icechunk as ic


def container_prefixes(allfiles: list[str]) -> list[str]:
    """
    Determine the set of Icechunk virtual chunk container prefixes for a fileset.

    Containers are keyed on a URL *prefix*, so one container covers every file
    beneath a given root; there is deliberately no container per file. Icechunk
    resolves each chunk against the longest matching prefix.

    Local paths are returned as ``file://`` URLs because that is what VirtualiZarr
    writes into the chunk manifest - a bare posix path such as ``/badc/file.nc``
    is normalised to ``file:///badc/file.nc``, and a container prefix that does
    not match that form will never be used.

    :param allfiles:    (list) Source file paths or URLs.

    :returns:           (list) Sorted, deduplicated container prefixes.
    """

    prefixes = set()
    for f in allfiles:
        parsed = urlparse(str(f))

        if parsed.scheme in ('', 'file'):
            # Match VirtualiZarr's manifest normalisation of local paths.
            path = parsed.path if parsed.scheme == 'file' else str(f)
            prefixes.add(f'file://{os.path.dirname(os.path.abspath(path))}/')
        else:
            # Remote sources are grouped per host.
            prefixes.add(f'{parsed.scheme}://{parsed.netloc}/')

    return sorted(prefixes)


def _store_for(prefix: str):
    """
    Build the object store for a container prefix.

    :param prefix:  (str) A container url_prefix, as built by ``container_prefixes``.

    :returns:       An Icechunk store instance appropriate to the prefix scheme.
    """

    scheme = urlparse(prefix).scheme

    if scheme == 'file':
        return ic.local_filesystem_store(urlparse(prefix).path)
    if scheme in ('http', 'https'):
        return ic.http_store()

    raise NotImplementedError(
        f'No Icechunk virtual chunk store configured for "{scheme}://" sources. '
        'PADOCC currently supports local (file://) and http(s):// source data.'
    )


def _credential_for(prefix: str):
    """
    Build the access sentinel authorising a container prefix.

    Neither local nor anonymous http sources need real credentials, but Icechunk
    still requires each container to be explicitly authorised, and passing
    ``None`` in place of the sentinel is deprecated.

    :param prefix:  (str) A container url_prefix.

    :returns:       The matching Icechunk credentials sentinel.
    """

    scheme = urlparse(prefix).scheme

    if scheme == 'file':
        return ic.Credentials.LocalFileSystemAccess()

    return ic.Credentials.HttpAccess()


def store_config(prefixes: list[str]) -> tuple[ic.RepositoryConfig, dict]:
    """
    Build the repository config and credentials for a set of container prefixes.

    :param prefixes:    (list) Container prefixes from ``container_prefixes``.

    :returns:           (tuple) The RepositoryConfig with all containers registered,
                        and the credentials mapping to authorise them.
    """

    config = ic.RepositoryConfig.default()

    for prefix in prefixes:
        # One container per call - this method does not accept a collection.
        config.set_virtual_chunk_container(
            ic.VirtualChunkContainer(
                url_prefix=prefix,
                store=_store_for(prefix),
            )
        )

    credentials = ic.containers_credentials(
        {p: _credential_for(p) for p in prefixes})

    return config, credentials


def write_virtual_dataset(
        vds,
        store_path: str,
        allfiles: list[str],
        message: str = 'PADOCC virtual aggregation',
        logger = None,
    ) -> str:
    """
    Write a virtual dataset to a local Icechunk repository.

    :param vds:         (obj) The combined virtual xarray Dataset.

    :param store_path:  (str) Directory for the Icechunk repository.

    :param allfiles:    (list) Source files, used to derive the containers.

    :param message:     (str) Commit message for the snapshot.

    :param logger:      (obj) Logger for progress reporting.

    :returns:           (str) The id of the committed snapshot.
    """

    prefixes = container_prefixes(allfiles)
    config, credentials = store_config(prefixes)

    if logger is not None:
        logger.debug(f'Icechunk: virtual chunk containers: {prefixes}')

    # open_or_create rather than create, so a forceful rerun appends a snapshot
    # rather than failing on an existing repository.
    repo = ic.Repository.open_or_create(
        storage=ic.local_filesystem_storage(store_path),
        config=config,
        authorize_virtual_chunk_access=credentials,
    )

    session = repo.writable_session('main')
    vds.vz.to_icechunk(session.store)
    snapshot = session.commit(message)

    # Without this the container configuration is not persisted, and a fresh
    # Repository.open elsewhere cannot resolve any virtual chunk.
    repo.save_config()

    if logger is not None:
        logger.info(f'Icechunk: committed snapshot {snapshot}')

    return snapshot


def open_virtual_repo(store_path: str, allfiles: list[str]) -> ic.Repository:
    """
    Open an existing Icechunk repository for reading virtual references.

    Read access to virtual chunks is authorised separately from write access;
    a repository opened without ``authorize_virtual_chunk_access`` opens
    cleanly but errors as soon as chunk data is actually requested.

    :param store_path:  (str) Directory holding the Icechunk repository.

    :param allfiles:    (list) Source files, used to derive the containers.

    :returns:           (obj) The opened Icechunk Repository.
    """

    _, credentials = store_config(container_prefixes(allfiles))

    return ic.Repository.open(
        storage=ic.local_filesystem_storage(store_path),
        authorize_virtual_chunk_access=credentials,
    )
