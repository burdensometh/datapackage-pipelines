"""Integration tests for the incremental partition flow.

These exercise the generated pipelines end-to-end with `dpp run`, and assert
the incremental behaviour: unchanged partitions are reused, a changed digest is
really recomputed, removed partitions don't come back, and a failing partition
prevents the merge from publishing a partial package.
"""
import json
import os

import pytest
import yaml

from datapackage_pipelines.manager import run_pipelines

DATA = {
    'p1.csv': 'id,name,extra\n1,alice,x\n2,bob,y\n',
    'p2.csv': 'id,name,extra\n3,carol,z\n',
}

# Records every partition which actually reaches the cleaning phase.  Its
# output goes to a side file, so we can prove which partitions were recomputed
# and which ones were served from the cached qualified output.
MARKER_STEP = {
    'run': 'mark-clean-run',
    'code': (
        'import os\n'
        'from datapackage_pipelines.wrapper import ingest, spew\n'
        'params, dp, res_iter = ingest()\n'
        'source = dp["resources"][0].get("dpp:streamedFrom")\n'
        'with open("clean-runs.log", "a") as f:\n'
        '    f.write(os.path.basename(source) + "\\n")\n'
        'spew(dp, res_iter)\n'
    ),
}

BASE_SPEC = {
    'name': 'demo',
    'schema': {
        'fields': [
            {'name': 'id', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
        ]
    },
    'clean': [
        MARKER_STEP,
        {'run': 'set_types',
         'parameters': {'types': {'id': {'type': 'integer'}}}},
    ],
    'partitions': [
        {'key': 'p1', 'path': 'data/p1.csv', 'digest': 'sha256:aaa'},
        {'key': 'p2', 'path': 'data/p2.csv', 'digest': 'sha256:bbb'},
    ],
    'out-path': 'published',
}


def write_spec(root, spec):
    with open(os.path.join(root, 'partitions.source-spec.yaml'), 'w') as f:
        yaml.safe_dump(spec, f)


def setup_workspace(root, spec=None, data=None):
    os.makedirs(os.path.join(root, 'data'), exist_ok=True)
    for name, contents in (data or DATA).items():
        with open(os.path.join(root, 'data', name), 'w') as f:
            f.write(contents)
    write_spec(root, spec or BASE_SPEC)


def run_all(root):
    results = run_pipelines('all', root, use_cache=True, dirty=False,
                            force=False, concurrency=1, verbose_logs=True)
    return {r.pipeline_id: r for r in results}


def clean_runs(root):
    path = os.path.join(root, 'clean-runs.log')
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def reset_clean_runs(root):
    path = os.path.join(root, 'clean-runs.log')
    if os.path.exists(path):
        os.unlink(path)


def published_rows(root):
    with open(os.path.join(root, 'published', 'datapackage.json')) as f:
        descriptor = json.load(f)
    with open(os.path.join(root, 'published', 'data', 'demo.csv')) as f:
        lines = f.read().strip().split('\n')
    return descriptor, lines


@pytest.fixture
def workspace(tmpdir, monkeypatch):
    root = str(tmpdir)
    setup_workspace(root)
    # `dpp` resolves specs relative to the working directory
    monkeypatch.chdir(root)
    return root


def test_full_run_publishes_all_partitions(workspace):
    results = run_all(workspace)
    assert all(r.success for r in results.values()), results

    descriptor, lines = published_rows(workspace)
    assert lines == ['id,name', '1,alice', '2,bob', '3,carol']
    assert [p['key'] for p in
            descriptor['partition-manifest']['partitions']] == ['p1', 'p2']
    # The unified schema - extra columns from the sources are dropped
    assert [f['name'] for f in
            descriptor['resources'][0]['schema']['fields']] == ['id', 'name']
    assert sorted(clean_runs(workspace)) == ['p1.csv', 'p2.csv']


def test_unchanged_partitions_are_reused(workspace):
    run_all(workspace)
    reset_clean_runs(workspace)

    results = run_all(workspace)
    assert all(r.success for r in results.values()), results

    # No partition was cleaned again - their qualified outputs were reused
    assert clean_runs(workspace) == []
    _, lines = published_rows(workspace)
    assert lines == ['id,name', '1,alice', '2,bob', '3,carol']


def test_changed_digest_recomputes_only_that_partition(workspace):
    run_all(workspace)
    reset_clean_runs(workspace)

    data = dict(DATA)
    data['p1.csv'] = 'id,name,extra\n1,alice,x\n2,bob-updated,z\n'
    setup_workspace(workspace, data=data)
    spec = json.loads(json.dumps(BASE_SPEC))
    spec['partitions'][0]['digest'] = 'sha256:aaa2'
    write_spec(workspace, spec)

    results = run_all(workspace)
    assert all(r.success for r in results.values()), results

    assert clean_runs(workspace) == ['p1.csv']
    _, lines = published_rows(workspace)
    assert lines == ['id,name', '1,alice', '2,bob-updated', '3,carol']

    # Both versions of p1 exist, digest-addressed; p2 was never recomputed
    versions = sorted(os.listdir(
        os.path.join(workspace, 'published.partitions', 'p1')))
    assert versions == ['sha256-aaa', 'sha256-aaa2']
    assert os.listdir(
        os.path.join(workspace, 'published.partitions', 'p2')) == ['sha256-bbb']


def test_removed_partition_is_dropped(workspace):
    run_all(workspace)

    spec = json.loads(json.dumps(BASE_SPEC))
    del spec['partitions'][1]
    write_spec(workspace, spec)

    results = run_all(workspace)
    assert all(r.success for r in results.values()), results

    descriptor, lines = published_rows(workspace)
    assert lines == ['id,name', '1,alice', '2,bob']
    assert [p['key'] for p in
            descriptor['partition-manifest']['partitions']] == ['p1']


def test_failing_partition_does_not_publish_partial_package(workspace):
    run_all(workspace)
    with open(os.path.join(workspace, 'published', 'datapackage.json')) as f:
        before = f.read()

    data = dict(DATA)
    data['p3.csv'] = 'id,name\n9,dave\n'
    setup_workspace(workspace, data=data)
    spec = json.loads(json.dumps(BASE_SPEC))
    spec['partitions'].append({
        'key': 'p3', 'path': 'data/p3.csv', 'digest': 'sha256:ccc',
        'steps': [{
            'run': 'partition_schema',
            'parameters': {
                'schema': {'fields': [
                    {'name': 'id', 'type': 'integer'},
                    {'name': 'name', 'type': 'integer'},
                ]},
                'resources': ['demo'],
            }
        }],
    })
    write_spec(workspace, spec)

    results = run_all(workspace)
    assert not results['./demo/partition/p3'].success
    assert not results['./demo/merge'].success

    # The previously published package is untouched - no half-new package
    with open(os.path.join(workspace, 'published', 'datapackage.json')) as f:
        assert f.read() == before
    _, lines = published_rows(workspace)
    assert lines == ['id,name', '1,alice', '2,bob', '3,carol']


def test_empty_manifest_publishes_empty_result(tmpdir, monkeypatch):
    root = str(tmpdir)
    monkeypatch.chdir(root)
    spec = {'name': 'demo', 'schema': BASE_SPEC['schema'],
            'partitions': [], 'out-path': 'published'}
    write_spec(root, spec)

    results = run_all(root)
    assert all(r.success for r in results.values()), results

    descriptor, lines = published_rows(root)
    assert descriptor['partition-manifest'] == {'partitions': []}
    assert [f['name'] for f in
            descriptor['resources'][0]['schema']['fields']] == ['id', 'name']
    assert lines == ['id,name']
