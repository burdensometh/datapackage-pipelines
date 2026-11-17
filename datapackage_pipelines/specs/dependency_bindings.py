"""Selecting, pinning and replaying dependency data versions.

A pipeline that depends on another pipeline depends not only on that
pipeline's *code* (which is what the pipeline cache hash used to cover) - it
also depends on the actual *data* that pipeline produced.

This module implements "dependency bindings":

* A binding records which successful execution of a dependency was selected
  for a dependent run, the content version (``hash``) of the datapackage it
  published, its original location and an immutable snapshot of the
  datapackage.
* ``dependency://`` reads are resolved through the snapshot, so a dependency
  that republishes while the dependent is running cannot alter the data seen
  by that run.
* Bindings are persisted with the execution record, which makes the run
  replayable.  A replay resolves the recorded snapshot; it never falls back
  to whatever currently happens to live in the dependency's latest output
  path.  If the bound version is gone a DependencyVersionMissing is raised.
"""

import hashlib
import json
import logging
import os
import re
import shutil
import tempfile

from ..utilities.stat_utils import STATS_DPP_KEY, STATS_OUT_DP_URL_KEY


SNAPSHOTS_DIRNAME = '.dpp-dependencies'
DATAPACKAGE_FILENAME = 'datapackage.json'
SNAPSHOT_METADATA_FILENAME = 'dependency-snapshot.json'


class DependencyVersionMissing(Exception):
    """The exact dependency data version selected for a run is unavailable."""

    def __init__(self, pipeline_id, version, location=None):
        self.pipeline_id = pipeline_id
        self.version = version
        self.location = location
        message = 'Dependency "{}" data version {} is no longer available' \
            .format(pipeline_id, version)
        if location is not None:
            message += ' (expected at {})'.format(location)
        message += '. Refusing to use newer content from the same path.'
        super(DependencyVersionMissing, self).__init__(message)


def is_url(path):
    return isinstance(path, str) and \
        (path.startswith('http://') or path.startswith('https://'))


def _safe_component(value):
    value = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(value)).strip('._')
    return value or '_'


def snapshots_root():
    """Base directory of the dependency snapshot store.

    Like the status DB, it lives in the directory ``dpp`` runs from - not in
    the dependency's output directory (which is published and may be wiped).
    """
    root = os.environ.get('DPP_DEPENDENCIES_DIR')
    if root:
        return os.path.abspath(root)
    return os.path.abspath(SNAPSHOTS_DIRNAME)


def snapshot_dir_for(pipeline_id, version):
    """Where the snapshot of ``version`` of ``pipeline_id`` is stored."""
    name = _safe_component(os.path.basename(pipeline_id))
    return os.path.join(snapshots_root(), name, str(version))


# ## Content identity

def _canonical_resource_path(path):
    if os.path.isabs(path) or is_url(path):
        return os.path.basename(path)
    return os.path.normpath(path)


def _resource_paths(descriptor):
    """Yield (resource_descriptor, path) pairs."""
    for resource in descriptor.get('resources', ()):
        path = resource.get('path')
        if isinstance(path, list):
            path = path[0] if len(path) > 0 else None
        if isinstance(path, str):
            yield resource, path


def _md5_file(filename, blocksize=1024 * 1024):
    hasher = hashlib.md5()
    with open(filename, 'rb') as f:
        while True:
            block = f.read(blocksize)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


def _local_path(base_dir, path):
    if path is None or is_url(path):
        return None
    if os.path.isabs(path):
        return path
    if base_dir is None:
        return None
    return os.path.join(base_dir, path)


def version_from_descriptor(descriptor, base_dir=None):
    """Calculate the content identity of a datapackage.

    Dumpers store a content-derived hash in the descriptor (covering the
    resource hashes) - prefer it.  When it is missing (custom dumpers, plain
    hand-written datapackages) fall back to hashing the descriptor together
    with the actual bytes of every local resource.

    Resource paths are canonicalized (relative paths stay as they are,
    absolute paths and urls are reduced to their basename) so that the
    identity does not depend on where the package is mounted - a snapshot of
    the same content has the same version.
    """
    version = descriptor.get('hash')
    if version:
        return str(version)

    canonical = json.loads(json.dumps(descriptor))
    for resource in canonical.get('resources', ()):
        path = resource.get('path')
        if isinstance(path, list):
            if len(path) > 0:
                resource['path'] = [_canonical_resource_path(path[0])] + path[1:]
        elif path is not None:
            resource['path'] = _canonical_resource_path(path)

    hasher = hashlib.md5()
    hasher.update(json.dumps(canonical, sort_keys=True,
                             ensure_ascii=True).encode('ascii'))
    for _, path in _resource_paths(descriptor):
        full_path = _local_path(base_dir, path)
        if full_path is not None and os.path.isfile(full_path):
            hasher.update(_md5_file(full_path).encode('ascii'))
    return hasher.hexdigest()


def read_descriptor(url, base_dir=None):
    """Return (descriptor, absolute base dir) for a local or remote url.

    For remote datapackages the base dir is None.
    """
    if is_url(url):
        import requests
        response = requests.get(url)
        response.raise_for_status()
        return response.json(), None

    full_path = _local_path(base_dir or '.', url)
    with open(full_path, encoding='utf8') as f:
        descriptor = json.load(f)
    return descriptor, os.path.dirname(os.path.abspath(full_path))


def content_matches(descriptor, base_dir, version):
    """Verify that content at base_dir really is the requested version."""
    if version_from_descriptor(descriptor, base_dir) != str(version):
        return False
    # Verify declared resource hashes as well, so overwritten resource files
    # are detected even if the top level descriptor hash was not refreshed.
    for resource, path in _resource_paths(descriptor):
        expected = resource.get('hash')
        if not expected:
            continue
        full_path = _local_path(base_dir, path)
        if full_path is None or not os.path.isfile(full_path):
            continue
        if _md5_file(full_path) != str(expected):
            return False
    return True


# ## Snapshots

def _snapshot_names(descriptor):
    """Map every resource path to the relative path it gets in a snapshot."""
    ret = {}
    for _, path in _resource_paths(descriptor):
        if os.path.isabs(path) or is_url(path):
            ret[path] = os.path.basename(path)
        else:
            ret[path] = os.path.normpath(path)
    return ret


def _write_snapshot(descriptor, source_base_dir, temp_dir, version):
    names = _snapshot_names(descriptor)
    copied = json.loads(json.dumps(descriptor))
    resource_hashes = {}

    for resource in copied.get('resources', ()):
        path = resource.get('path')
        if isinstance(path, list):
            path = path[0] if len(path) > 0 else None
        if path not in names:
            continue
        relative_path = names[path]
        full_path = _local_path(source_base_dir, path)
        if full_path is None or not os.path.isfile(full_path):
            # Nothing to copy (remote or already missing) - leave the
            # descriptor untouched rather than point at an absent file.
            continue
        target = os.path.join(temp_dir, relative_path)
        os.makedirs(os.path.dirname(target) or temp_dir, exist_ok=True)
        shutil.copyfile(full_path, target)
        resource_hashes[relative_path] = _md5_file(target)
        # Point the descriptor into the snapshot, not back at the (mutable)
        # original location.
        if isinstance(resource.get('path'), list):
            resource['path'] = [relative_path] + resource['path'][1:]
        else:
            resource['path'] = relative_path

    with open(os.path.join(temp_dir, DATAPACKAGE_FILENAME), 'w',
              encoding='utf8') as f:
        json.dump(copied, f, sort_keys=True, ensure_ascii=False)
    with open(os.path.join(temp_dir, SNAPSHOT_METADATA_FILENAME), 'w',
              encoding='utf8') as f:
        json.dump({
            'version': str(version),
            'datapackage-hash': descriptor.get('hash'),
            'resources': resource_hashes,
        }, f, sort_keys=True, ensure_ascii=False)


def verify_snapshot(directory, version):
    """Return the snapshot's datapackage path if it holds ``version``."""
    metadata_file = os.path.join(directory, SNAPSHOT_METADATA_FILENAME)
    datapackage_file = os.path.join(directory, DATAPACKAGE_FILENAME)
    if not os.path.isfile(metadata_file) or \
            not os.path.isfile(datapackage_file):
        return None
    try:
        with open(metadata_file, encoding='utf8') as f:
            metadata = json.load(f)
        if str(metadata.get('version')) != str(version):
            return None
        for relative_path, expected in metadata.get('resources', {}).items():
            full_path = os.path.join(directory, relative_path)
            if not os.path.isfile(full_path):
                return None
            if _md5_file(full_path) != expected:
                return None
    except (OSError, ValueError):
        return None
    return datapackage_file


def snapshot_datapackage(url, source_base_dir, version, pipeline_id):
    """Materialize an immutable snapshot of a dependency output datapackage.

    Returns the snapshot's ``datapackage.json`` path, or None when the
    datapackage is remote (it is verified instead).  Raises
    DependencyVersionMissing when the content at ``url`` is not exactly
    ``version`` - callers must never silently read newer data instead.
    """
    try:
        descriptor, descriptor_base = read_descriptor(url, source_base_dir)
    except (OSError, ValueError) as e:
        raise DependencyVersionMissing(pipeline_id, version, url) from e

    if descriptor_base is None:
        if version_from_descriptor(descriptor, None) != str(version):
            raise DependencyVersionMissing(pipeline_id, version, url)
        logging.warning('Cannot snapshot remote dependency datapackage %s',
                        url)
        return None

    destination = snapshot_dir_for(pipeline_id, version)
    existing = verify_snapshot(destination, version)
    if existing is not None:
        return existing

    if not content_matches(descriptor, descriptor_base, version):
        raise DependencyVersionMissing(pipeline_id, version, url)

    parent = os.path.dirname(destination)
    try:
        os.makedirs(parent, exist_ok=True)
        temp_dir = tempfile.mkdtemp(prefix=str(version) + '.tmp-', dir=parent)
    except OSError as e:
        logging.warning('Failed to create dependency snapshot for %s: %s',
                        url, e)
        return None

    try:
        _write_snapshot(descriptor, descriptor_base, temp_dir, version)
        if verify_snapshot(temp_dir, version) is None:
            raise DependencyVersionMissing(pipeline_id, version, url)
        if os.path.isdir(destination):
            # A concurrent writer won the race - keep the verified copy.
            shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            os.rename(temp_dir, destination)
    except DependencyVersionMissing:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    except OSError as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logging.warning('Failed to write dependency snapshot for %s: %s',
                        url, e)
        return None
    return verify_snapshot(destination, version)


# ## Bindings

def output_from_stats(stats, pipeline_cwd, pipeline_id=None,
                      snapshot=False):
    """Build an output record from a pipeline execution's final stats.

    ``pipeline_cwd`` is the directory the pipeline ran in; relative
    out-datapackage-url values are resolved against it.  When ``snapshot``
    is set the exact published data is immediately frozen into the
    dependency snapshot store, so that later dependents (and replays) bind
    to it instead of to a path that may be overwritten by the next run.
    """
    stats = stats or {}
    url = (stats.get(STATS_DPP_KEY) or {}).get(STATS_OUT_DP_URL_KEY)
    if url is None:
        return None

    base_dir = os.path.abspath(pipeline_cwd)
    version = stats.get('hash')
    if not version:
        try:
            descriptor, descriptor_base = read_descriptor(url, base_dir)
            version = version_from_descriptor(descriptor, descriptor_base)
            if descriptor_base is not None:
                base_dir = descriptor_base
        except Exception:  # noqa - best effort, the output may be remote
            logging.warning('Failed to calculate output version for %s', url,
                            exc_info=True)
            return None

    output = {
        'datapackage-url': url,
        'version': str(version),
        'base-dir': base_dir,
        'snapshot': None,
    }
    if snapshot:
        try:
            output['snapshot'] = snapshot_datapackage(
                url, base_dir, output['version'], pipeline_id)
        except DependencyVersionMissing:
            logging.warning('Output at %s does not match its reported version '
                            '%s; not snapshotting it', url, output['version'])
    return output


def binding_from_execution(pipeline_id, execution, pipeline_cwd=None):
    """Create a binding record for a selected successful execution.

    Does not touch the filesystem when the execution recorded its output at
    finish time; older execution records are upgraded from their stats.
    """
    if execution is None:
        return None

    output = getattr(execution, 'output', None)
    if not output:
        output = output_from_stats(
            getattr(execution, 'stats', None) or {},
            pipeline_cwd or os.path.dirname(pipeline_id) or '.')

    if not output or not output.get('version') \
            or not output.get('datapackage-url'):
        return None

    return {
        'pipeline': pipeline_id,
        'execution': execution.execution_id,
        'version': str(output['version']),
        'datapackage-url': output['datapackage-url'],
        'base-dir': output.get('base-dir'),
        'snapshot': output.get('snapshot'),
    }


def live_bindings(declared_dependencies, status_mgr, fallback_base_dir=None):
    """Bind declared pipeline dependencies to their latest successful output."""
    bindings = {}
    for dependency in declared_dependencies or ():
        pipeline_id = dependency.get('pipeline')
        if pipeline_id is None:
            continue
        ps = status_mgr.get(pipeline_id)
        execution = ps.get_last_successful_execution()
        # Relative output paths of a dependency are relative to that
        # dependency's own directory, not the dependent's.
        dependency_base_dir = os.path.dirname(pipeline_id) or fallback_base_dir
        binding = binding_from_execution(pipeline_id, execution,
                                         dependency_base_dir)
        if binding is not None:
            bindings[pipeline_id] = binding
    return bindings


def pin_binding(binding):
    """Snapshot a binding's datapackage; returns the updated binding.

    Raises DependencyVersionMissing if the bound version is no longer
    available at its original location (and no valid snapshot exists).
    """
    pipeline_id = binding.get('pipeline')
    version = binding.get('version')
    if not version:
        raise DependencyVersionMissing(pipeline_id, version)

    if binding.get('snapshot'):
        verified = verify_snapshot(os.path.dirname(binding['snapshot']),
                                   version)
        if verified is not None:
            binding = dict(binding, snapshot=verified)
            return binding
        logging.warning('Recorded dependency snapshot %s is missing for %s; '
                        'attempting to recreate it',
                        binding['snapshot'], pipeline_id)

    url = binding.get('datapackage-url')
    if url is None:
        raise DependencyVersionMissing(pipeline_id, version)
    base_dir = binding.get('base-dir')
    if base_dir is None and not os.path.isabs(url) and not is_url(url):
        base_dir = os.path.dirname(pipeline_id) or '.'

    try:
        snapshot = snapshot_datapackage(url, base_dir, version, pipeline_id)
    except DependencyVersionMissing:
        raise DependencyVersionMissing(pipeline_id, version, url)

    return dict(binding, snapshot=snapshot)


def materialize_bindings(bindings):
    """Pin every binding and resolve it to a stable datapackage url.

    Returns ``(pinned_bindings, urls)`` where ``urls`` maps pipeline ids to
    snapshot datapackage paths, in the shape the wrapper expects for
    ``dependency://`` resolution.
    """
    pinned = {}
    urls = {}
    for pipeline_id, binding in (bindings or {}).items():
        pinned_binding = pin_binding(binding)
        pinned[pipeline_id] = pinned_binding
        urls[pipeline_id] = pinned_binding.get('snapshot') \
            or pinned_binding['datapackage-url']
    return pinned, urls


def prune_snapshots(pipeline_id, keep_versions, status_mgr=None):
    """Delete snapshot versions of ``pipeline_id`` that are no longer needed.

    ``keep_versions`` are the versions published by the retained executions
    of the pipeline itself.  When a status manager is given, versions that
    are still referenced by any retained dependent execution (through its
    dependency bindings) are kept as well - a queued or replayable dependent
    must never lose the data it was bound to.
    """
    keep_versions = set(str(v) for v in keep_versions if v)
    if status_mgr is not None:
        for other_id in status_mgr.all_pipeline_ids():
            try:
                executions = status_mgr.get(other_id).executions
            except (KeyError, TypeError, ValueError):
                continue
            for execution in executions:
                binding = (execution.dependency_bindings or {}) \
                    .get(pipeline_id)
                if binding and binding.get('version'):
                    keep_versions.add(str(binding['version']))

    directory = os.path.dirname(snapshot_dir_for(pipeline_id, 'x'))
    if not os.path.isdir(directory):
        return
    for name in os.listdir(directory):
        if name in keep_versions:
            continue
        path = os.path.join(directory, name)
        if not os.path.isdir(path):
            continue
        # Never remove a snapshot that is currently being written.
        if '.tmp-' in name:
            continue
        try:
            shutil.rmtree(path)
        except OSError:
            logging.warning('Failed to prune dependency snapshot %s', path,
                            exc_info=True)
