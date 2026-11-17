import json
import os
import shutil

import pytest

from datapackage_pipelines.manager import execute_pipeline, replay_execution
from datapackage_pipelines.specs.specs import pipelines
from datapackage_pipelines.status import status_mgr
from datapackage_pipelines.utilities.execution_id import gen_execution_id


SPEC = """
producer:
  pipeline:
    - run: update_package
      parameters:
        name: producer-data
    - run: load
      parameters:
        from: source.csv
        name: data
        format: csv
    - run: dump_to_path
      parameters:
        out-path: out-producer

consumer:
  dependencies:
    - pipeline: ./producer
  pipeline:
    - run: update_package
      parameters:
        name: consumer-data
    - run: load_resource
      parameters:
        url: dependency://./producer
        resource: data
    - run: dump_to_path
      parameters:
        out-path: out-consumer
"""

CONSUMER_WITH_INFLIGHT_REPUBLISH = """
producer:
  pipeline:
    - run: update_package
      parameters:
        name: producer-data
    - run: load
      parameters:
        from: source.csv
        name: data
        format: csv
    - run: dump_to_path
      parameters:
        out-path: out-producer

consumer:
  dependencies:
    - pipeline: ./producer
  pipeline:
    - run: republish-dependency
    - run: update_package
      parameters:
        name: consumer-data
    - run: load_resource
      parameters:
        url: dependency://./producer
        resource: data
    - run: dump_to_path
      parameters:
        out-path: out-consumer
"""

REPUBLISH_PROCESSOR = """
from datapackage_pipelines.wrapper import ingest, spew
import os, json

params, dp, res = ingest()

# Simulate the dependency publishing again *while this run is in flight*:
# overwrite the dependency's live output path in place.
producer_json = os.path.join('out-producer', 'datapackage.json')
if os.path.exists(producer_json):
    with open(producer_json) as f:
        producer = json.load(f)
    for resource in producer['resources']:
        path = os.path.join('out-producer', resource['path'])
        if os.path.exists(path):
            with open(path, 'w') as f:
                f.write('a,b\\n1,second\\n')

spew(dp, res)
"""

SOURCE = """a,b
1,{value}
"""


def _write_source(root, value):
    with open(os.path.join(root, 'source.csv'), 'w', encoding='utf8') as f:
        f.write(SOURCE.format(value=value))


@pytest.fixture
def inflight_env(tmpdir, monkeypatch):
    root = str(tmpdir)
    monkeypatch.chdir(root)
    with open(os.path.join(root, 'pipeline-spec.yaml'), 'w',
              encoding='utf8') as f:
        f.write(CONSUMER_WITH_INFLIGHT_REPUBLISH)
    with open(os.path.join(root, 'republish-dependency.py'), 'w',
              encoding='utf8') as f:
        f.write(REPUBLISH_PROCESSOR)
    _write_source(root, 'first')
    status = _reset_status(root)
    yield root, status
    _reset_status(root)


def _reset_status(root):
    import datapackage_pipelines.status.status_manager as sm
    sm._status = None
    sm._root_dir = None
    return status_mgr(root)


@pytest.fixture
def env(tmpdir, monkeypatch):
    root = str(tmpdir)
    monkeypatch.chdir(root)
    with open(os.path.join(root, 'pipeline-spec.yaml'), 'w',
              encoding='utf8') as f:
        f.write(SPEC)
    _write_source(root, 'first')
    status = _reset_status(root)
    yield root, status
    # Do not leak our status manager into other tests
    _reset_status(root)


def _spec(root, status, pipeline_id):
    for spec in pipelines(root_dir=root, status_manager=status):
        if spec.pipeline_id == pipeline_id:
            return spec
    raise KeyError(pipeline_id)


def _run(root, status, pipeline_id):
    spec = _spec(root, status, pipeline_id)
    assert not spec.validation_errors, spec.validation_errors
    ps = status.get(spec.pipeline_id)
    ps.init(spec.pipeline_details, spec.source_details,
            spec.validation_errors, spec.cache_hash)
    eid = gen_execution_id()
    assert ps.queue_execution(eid, 'manual', spec.dependency_bindings,
                              pipeline_path=spec.path)
    success, stats, errors = execute_pipeline(
        spec, eid, 'manual',
        dependency_bindings=spec.dependency_bindings)
    execution = status_mgr(root).get(pipeline_id).get_last_execution()
    return success, stats, errors, execution


def _consumer_rows(root):
    path = os.path.join(root, 'out-consumer', 'datapackage.json')
    with open(path, encoding='utf8') as f:
        descriptor = json.load(f)
    with open(os.path.join(os.path.dirname(path),
                           descriptor['resources'][0]['path']),
              encoding='utf8') as f:
        return f.read()


def test_dependency_data_version_participates_in_cache_hash(env):
    root, status = env
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors

    consumer_before = _spec(root, status, './consumer')
    assert consumer_before.dependency_bindings['./producer']['version']

    # Same code, different output
    _write_source(root, 'second')
    success, _, errors, producer_execution = _run(root, status, './producer')
    assert success, errors

    consumer_after = _spec(root, status, './consumer')
    assert consumer_after.cache_hash != consumer_before.cache_hash
    assert consumer_after.dependency_bindings['./producer']['version'] != \
        consumer_before.dependency_bindings['./producer']['version']


def test_consumer_reads_bound_version_not_latest_path(env):
    root, status = env
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors
    old_binding = _spec(root, status, './consumer').dependency_bindings

    # Dependency republishes new data...
    _write_source(root, 'second')
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors

    # ...but an execution bound to the old version keeps reading old data.
    spec = _spec(root, status, './consumer')
    spec.dependency_bindings = old_binding
    ps = status.get(spec.pipeline_id)
    ps.init(spec.pipeline_details, spec.source_details,
            spec.validation_errors, spec.cache_hash)
    eid = gen_execution_id()
    ps.queue_execution(eid, 'manual', old_binding, pipeline_path=spec.path)
    success, _, errors = execute_pipeline(spec, eid, 'manual',
                                          dependency_bindings=old_binding)
    assert success, errors
    assert 'first' in _consumer_rows(root)
    assert 'second' not in _consumer_rows(root)


def test_replay_uses_recorded_bindings(env):
    root, status = env
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors
    success, _, errors, consumer_execution = _run(root, status, './consumer')
    assert success, errors
    assert 'first' in _consumer_rows(root)
    assert consumer_execution.dependency_bindings

    _write_source(root, 'second')
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors
    success, _, errors, _ = _run(root, status, './consumer')
    assert success, errors
    assert 'second' in _consumer_rows(root)

    # Replaying the first consumer execution must reproduce the old input.
    result = replay_execution(consumer_execution.execution_id, root_dir=root)
    assert result.success, result.errors
    rows = _consumer_rows(root)
    assert 'first' in rows
    assert 'second' not in rows


def test_inflight_republish_does_not_change_run_inputs(inflight_env):
    root, status = inflight_env
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors

    success, _, errors, consumer_execution = _run(root, status, './consumer')
    assert success, errors

    # The consumer step overwrote the producer's live output mid-run; the
    # bound version snapshot must have shielded the run from it.
    assert 'first' in _consumer_rows(root)
    assert 'second' not in _consumer_rows(root)

    with open(os.path.join(root, 'out-producer', 'data.csv')) as f:
        assert 'second' in f.read()

    # And the record still points at data that can be replayed.
    result = replay_execution(consumer_execution.execution_id, root_dir=root)
    assert result.success, result.errors
    assert 'first' in _consumer_rows(root)


CACHED_CONSUMER = """
producer:
  pipeline:
    - run: update_package
      parameters:
        name: producer-data
    - run: load
      parameters:
        from: source.csv
        name: data
        format: csv
    - run: dump_to_path
      parameters:
        out-path: out-producer

consumer:
  dependencies:
    - pipeline: ./producer
  pipeline:
    - run: mark-run
    - run: update_package
      parameters:
        name: consumer-data
    - run: load_resource
      cache: true
      parameters:
        url: dependency://./producer
        resource: data
    - run: dump_to_path
      parameters:
        out-path: out-consumer
"""

MARK_RUN_PROCESSOR = """
from datapackage_pipelines.wrapper import ingest, spew

params, dp, res = ingest()
with open('runs.txt', 'a') as f:
    f.write('run\\n')
spew(dp, res)
"""


@pytest.fixture
def cached_env(tmpdir, monkeypatch):
    root = str(tmpdir)
    monkeypatch.chdir(root)
    with open(os.path.join(root, 'pipeline-spec.yaml'), 'w',
              encoding='utf8') as f:
        f.write(CACHED_CONSUMER)
    with open(os.path.join(root, 'mark-run.py'), 'w',
              encoding='utf8') as f:
        f.write(MARK_RUN_PROCESSOR)
    _write_source(root, 'first')
    status = _reset_status(root)
    yield root, status
    _reset_status(root)


def _run_count(root):
    path = os.path.join(root, 'runs.txt')
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return len(f.readlines())


def test_cache_reuse_follows_dependency_data_version(cached_env):
    root, status = cached_env
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors

    # First consumer run - cache is cold, the precursor step executes.
    success, _, errors, _ = _run(root, status, './consumer')
    assert success, errors
    assert _run_count(root) == 1

    # Same dependency data version - the cached step is reused, so steps
    # before it are not recalculated.
    success, _, errors, _ = _run(root, status, './consumer')
    assert success, errors
    assert _run_count(root) == 1

    # Dependency code unchanged, but it republished new data: every data
    # step affected by the dependency must be recalculated.
    _write_source(root, 'second')
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors
    success, _, errors, _ = _run(root, status, './consumer')
    assert success, errors
    assert _run_count(root) == 2
    assert 'second' in _consumer_rows(root)


def test_replay_errors_when_bound_version_is_gone(env):
    root, status = env
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors
    success, _, errors, consumer_execution = _run(root, status, './consumer')
    assert success, errors

    binding = consumer_execution.dependency_bindings['./producer']
    snapshot = binding['snapshot']
    assert snapshot and os.path.isfile(snapshot)
    shutil.rmtree(os.path.dirname(snapshot))

    _write_source(root, 'second')
    success, _, errors, _ = _run(root, status, './producer')
    assert success, errors

    result = replay_execution(consumer_execution.execution_id, root_dir=root)
    assert not result.success
    assert any('no longer available' in error for error in result.errors)

    # Nothing was silently recomputed from the newer content.
    rows = _consumer_rows(root)
    assert 'first' in rows and 'second' not in rows
