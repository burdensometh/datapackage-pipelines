import json
import os

import pytest

from datapackage_pipelines.specs.dependency_bindings import (
    DependencyVersionMissing,
    binding_from_execution,
    live_bindings,
    materialize_bindings,
    pin_binding,
    read_descriptor,
    snapshot_datapackage,
    version_from_descriptor,
)


def _write_datapackage(path, name, rows, extra=None):
    """Helper: write a tiny datapackage with a CSV resource."""
    os.makedirs(path, exist_ok=True)
    resource_path = os.path.join(path, 'data.csv')
    with open(resource_path, 'w', encoding='utf8') as f:
        f.write('a,b\n')
        for a, b in rows:
            f.write('%s,%s\n' % (a, b))
    descriptor = {
        'name': name,
        'resources': [{
            'name': 'data',
            'path': 'data.csv',
            'schema': {'fields': [
                {'name': 'a', 'type': 'string'},
                {'name': 'b', 'type': 'string'},
            ]},
        }],
    }
    descriptor.update(extra or {})
    datapackage_path = os.path.join(path, 'datapackage.json')
    with open(datapackage_path, 'w', encoding='utf8') as f:
        json.dump(descriptor, f)
    return datapackage_path


class FakeExecution(object):
    def __init__(self, execution_id, output):
        self.execution_id = execution_id
        self.output = output


class FakeStatus(object):
    def __init__(self, execution):
        self.execution = execution

    def get_last_successful_execution(self):
        return self.execution


class FakeStatusManager(object):
    def __init__(self, executions):
        self.executions = executions

    def get(self, pipeline_id):
        return FakeStatus(self.executions.get(pipeline_id))


def test_binding_pins_exact_version_and_survives_republish(tmpdir):
    tmpdir = str(tmpdir)
    dep_dir = os.path.join(tmpdir, 'pipelines', 'dep')
    dp_path = _write_datapackage(dep_dir, 'dep', [('1', 'old')])
    version = version_from_descriptor(
        json.load(open(dp_path)), os.path.dirname(dp_path))

    output = {'datapackage-url': 'datapackage.json',
              'version': version, 'base-dir': dep_dir, 'snapshot': None}
    binding = binding_from_execution(
        './pipelines/dep', FakeExecution('exec-1', output))
    assert binding['version'] == version

    pinned, urls = materialize_bindings({'./pipelines/dep': binding})
    snapshot = urls['./pipelines/dep']
    assert os.path.isfile(snapshot)
    assert pinned['./pipelines/dep']['snapshot'] == snapshot

    snap_descriptor = json.load(open(snapshot))
    snap_base = os.path.dirname(snapshot)
    assert version_from_descriptor(snap_descriptor, snap_base) == version

    # The dependency republishes *in place* - the binding must keep reading
    # the old data, and the old snapshot must remain valid.
    _write_datapackage(dep_dir, 'dep', [('2', 'new')])

    pinned_again, urls_again = materialize_bindings(
        {'./pipelines/dep': pinned['./pipelines/dep']})
    assert urls_again['./pipelines/dep'] == snapshot
    with open(os.path.join(snap_base, 'data.csv')) as f:
        assert 'old' in f.read()
        assert 'new' not in f.read()


def test_replay_refuses_newer_content_after_snapshot_loss(tmpdir):
    tmpdir = str(tmpdir)
    dep_dir = os.path.join(tmpdir, 'pipelines', 'dep')
    dp_path = _write_datapackage(dep_dir, 'dep', [('1', 'old')])
    version = version_from_descriptor(
        json.load(open(dp_path)), os.path.dirname(dp_path))

    snapshot = snapshot_datapackage(dp_path, dep_dir, version, './dep')
    assert snapshot is not None

    binding = {
        'pipeline': './dep',
        'execution': 'exec-1',
        'version': version,
        'datapackage-url': 'datapackage.json',
        'base-dir': dep_dir,
        'snapshot': snapshot,
    }

    # Lose the snapshot...
    import shutil
    shutil.rmtree(os.path.dirname(snapshot))
    # ...and republish different data at the original path.
    _write_datapackage(dep_dir, 'dep', [('2', 'new')])

    with pytest.raises(DependencyVersionMissing):
        materialize_bindings({'./dep': binding})


def test_replay_recreates_snapshot_when_content_is_identical(tmpdir):
    tmpdir = str(tmpdir)
    dep_dir = os.path.join(tmpdir, 'pipelines', 'dep')
    dp_path = _write_datapackage(dep_dir, 'dep', [('1', 'stable')])
    version = version_from_descriptor(
        json.load(open(dp_path)), os.path.dirname(dp_path))

    binding = {
        'pipeline': './dep',
        'execution': 'exec-1',
        'version': version,
        'datapackage-url': 'datapackage.json',
        'base-dir': dep_dir,
        'snapshot': os.path.join(dep_dir, '.gone', version, 'datapackage.json'),
    }

    _, urls = materialize_bindings({'./dep': binding})
    snapshot = urls['./dep']
    assert os.path.isfile(snapshot)
    assert version_from_descriptor(
        json.load(open(snapshot)), os.path.dirname(snapshot)) == version


def test_pinning_errors_when_version_never_existed(tmpdir):
    tmpdir = str(tmpdir)
    dep_dir = os.path.join(tmpdir, 'pipelines', 'dep')
    _write_datapackage(dep_dir, 'dep', [('1', 'actual')])

    binding = {
        'pipeline': './dep',
        'execution': 'exec-1',
        'version': 'deadbeef' * 4,
        'datapackage-url': 'datapackage.json',
        'base-dir': dep_dir,
        'snapshot': None,
    }
    with pytest.raises(DependencyVersionMissing):
        pin_binding(binding)


def test_live_bindings_select_last_successful_execution(tmpdir):
    tmpdir = str(tmpdir)
    dep_dir = os.path.join(tmpdir, 'pipelines', 'dep')
    dp_path = _write_datapackage(dep_dir, 'dep', [('1', 'x')])
    version = version_from_descriptor(
        json.load(open(dp_path)), os.path.dirname(dp_path))

    execution = FakeExecution('exec-9', {
        'datapackage-url': 'datapackage.json',
        'version': version,
        'base-dir': dep_dir,
        'snapshot': None,
    })
    sm = FakeStatusManager({'./dep': execution})
    bindings = live_bindings([{'pipeline': './dep'}], sm, dep_dir)
    assert bindings['./dep']['execution'] == 'exec-9'
    assert bindings['./dep']['version'] == version


def test_binding_from_stats_without_recorded_output(tmpdir):
    tmpdir = str(tmpdir)
    dep_dir = os.path.join(tmpdir, 'pipelines', 'dep')
    _write_datapackage(dep_dir, 'dep', [('1', 'legacy')], extra={
        'hash': 'top-level-hash'})

    execution = FakeExecution('exec-old', None)
    execution.stats = {
        '.dpp': {'out-datapackage-url': 'datapackage.json'},
        'hash': 'top-level-hash',
    }
    binding = binding_from_execution('./dep', execution, dep_dir)
    assert binding['version'] == 'top-level-hash'
    _, urls = materialize_bindings({'./dep': binding})
    assert version_from_descriptor(
        json.load(open(urls['./dep'])),
        os.path.dirname(urls['./dep'])) == 'top-level-hash'


def test_snapshot_handles_absolute_resource_paths(tmpdir):
    tmpdir = str(tmpdir)
    dep_dir = os.path.join(tmpdir, 'pipelines', 'dep')
    dp_path = _write_datapackage(dep_dir, 'dep', [('1', 'abs')])

    descriptor = json.load(open(dp_path))
    descriptor['resources'][0]['path'] = \
        os.path.join(dep_dir, descriptor['resources'][0]['path'])
    with open(dp_path, 'w') as f:
        json.dump(descriptor, f)

    version = version_from_descriptor(descriptor, dep_dir)
    snapshot = snapshot_datapackage(dp_path, dep_dir, version, './dep')
    snap_descriptor, snap_base = read_descriptor(snapshot)
    assert not os.path.isabs(snap_descriptor['resources'][0]['path'])
    assert version_from_descriptor(snap_descriptor, snap_base) == version
