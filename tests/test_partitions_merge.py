"""Unit tests for the merge and publish processors."""
import json
import os

import pytest

from datapackage_pipelines.lib.merge_partitions import flow as merge_flow
from datapackage_pipelines.lib.publish_datapackage import _swap_into_place

SCHEMA = {
    'fields': [
        {'name': 'id', 'type': 'integer'},
        {'name': 'name', 'type': 'string'},
    ],
    'missingValues': [''],
}


def make_partition_output(root, key, digest, rows=('1,alice',)):
    """Write a per-partition data package as the partition pipeline would."""
    directory = os.path.join(root, key, digest.replace(':', '-'))
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, 'part.csv'), 'w') as f:
        f.write('id,name\n' + '\n'.join(rows) + '\n')
    descriptor = {
        'name': '_',
        'profile': 'data-package',
        'count_of_rows': len(rows),
        'resources': [{
            'name': 'demo',
            'path': 'part.csv',
            'profile': 'tabular-data-resource',
            'schema': SCHEMA,
            'partition': {'key': key, 'digest': digest},
        }],
    }
    with open(os.path.join(directory, 'datapackage.json'), 'w') as f:
        json.dump(descriptor, f)
    return os.path.join(directory, 'datapackage.json')


def merge_parameters(partitions):
    return {
        'partitions': partitions,
        'schema': SCHEMA,
        'resource-name': 'demo',
        'metadata': {'name': 'demo'},
    }


def consume(parameters):
    descriptor, resource_iterator = merge_flow(parameters)
    rows = [row for resource in resource_iterator for row in resource]
    return descriptor, rows


def test_merges_only_listed_partitions(tmpdir):
    root = str(tmpdir)
    p1 = make_partition_output(root, 'p1', 'sha256:aaa', ('1,alice',))
    p2 = make_partition_output(root, 'p2', 'sha256:bbb', ('2,bob',))
    # A stale partition output still exists on disk - it must be ignored
    make_partition_output(root, 'old', 'sha256:zzz', ('9,ghost',))

    descriptor, rows = consume(merge_parameters([
        {'key': 'p1', 'digest': 'sha256:aaa', 'path': 'p1.csv',
         'datapackage': p1},
        {'key': 'p2', 'digest': 'sha256:bbb', 'path': 'p2.csv',
         'datapackage': p2},
    ]))

    assert rows == [{'id': 1, 'name': 'alice'},
                    {'id': 2, 'name': 'bob'}]
    assert [p['key'] for p in
            descriptor['partition-manifest']['partitions']] == ['p1', 'p2']
    assert [f['name'] for f in
            descriptor['resources'][0]['schema']['fields']] == ['id', 'name']


def test_partition_with_a_different_digest_is_rejected(tmpdir):
    root = str(tmpdir)
    # The output was computed for the *old* digest
    stale = make_partition_output(root, 'p1', 'sha256:old')

    with pytest.raises(ValueError) as excinfo:
        consume(merge_parameters([
            {'key': 'p1', 'digest': 'sha256:new', 'path': 'p1.csv',
             'datapackage': stale},
        ]))
    assert 'refusing to use a stale partition' in str(excinfo.value)


def test_missing_partition_output_is_rejected(tmpdir):
    with pytest.raises(ValueError) as excinfo:
        consume(merge_parameters([
            {'key': 'p1', 'digest': 'sha256:aaa', 'path': 'p1.csv',
             'datapackage': os.path.join(str(tmpdir), 'nope',
                                         'datapackage.json')},
        ]))
    assert 'Missing output for partition' in str(excinfo.value)


def test_empty_manifest_is_a_defined_empty_result():
    descriptor, rows = consume(merge_parameters([]))

    assert descriptor['partition-manifest'] == {'partitions': []}
    assert rows == []
    resource = descriptor['resources'][0]
    assert [f['name'] for f in resource['schema']['fields']] == ['id', 'name']
    assert resource['name'] == 'demo'


def test_empty_manifest_without_schema_has_no_resources():
    parameters = merge_parameters([])
    parameters['schema'] = None
    descriptor, rows = consume(parameters)

    assert descriptor['partition-manifest'] == {'partitions': []}
    assert descriptor['resources'] == []
    assert rows == []


def test_unconforming_rows_fail_the_merge(tmpdir):
    root = str(tmpdir)
    output = make_partition_output(root, 'p1', 'sha256:aaa', ('x,alice',))

    with pytest.raises(ValueError) as excinfo:
        consume(merge_parameters([
            {'key': 'p1', 'digest': 'sha256:aaa', 'path': 'p1.csv',
             'datapackage': output},
        ]))
    assert 'does not conform to the unified schema' in str(excinfo.value)


def test_swap_into_place_replaces_old_package(tmpdir):
    root = str(tmpdir)
    out = os.path.join(root, 'published')
    new = os.path.join(root, 'published.new')

    os.makedirs(out)
    with open(os.path.join(out, 'marker'), 'w') as f:
        f.write('old')
    os.makedirs(new)
    with open(os.path.join(new, 'marker'), 'w') as f:
        f.write('new')

    _swap_into_place(new, out)

    with open(os.path.join(out, 'marker')) as f:
        assert f.read() == 'new'
    assert not os.path.exists(new)
    # No leftover temporary directories
    assert [n for n in os.listdir(root) if n.startswith('.')] == []
