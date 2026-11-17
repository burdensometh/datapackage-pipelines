"""Merge the confirmed partition outputs into one data package.

The step receives the partitions which were confirmed *in this round* (as
generated from the source spec) - never a listing of what happens to exist on
disk.  Each partition's per-partition data package is verified against the
digest declared for it in the manifest, and only then are its rows read.
Consequently:

- a removed partition can never come back (it simply isn't in the list),
- a partition whose digest changed is read from its newly computed output,
- a missing, stale or partially written partition output aborts the step, which
  aborts the pipeline, so no partial "new" data package is ever published.

An empty manifest produces an explicit empty result: a data package carrying an
empty partition manifest, and (when a unified schema is declared) a single
resource with that schema and no rows.
"""
import copy
import os

import datapackage
from tableschema import Schema

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew


def _unify_schema(schema):
    if schema is None:
        return None
    schema = copy.deepcopy(schema)
    schema.setdefault('fields', [])
    schema.setdefault('missingValues', [''])
    return schema


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _verify_partition(partition, resource_name):
    """Read and validate a partition's output; return (resource, count)."""
    key = partition['key']
    digest = partition['digest']
    datapackage_path = partition['datapackage']

    _check(os.path.exists(datapackage_path),
           'Missing output for partition %r (%s) - was it computed at all?'
           % (key, datapackage_path))

    package = datapackage.Package(datapackage_path, strict=False)
    _check(package.valid,
           'Invalid output data package for partition %r (%s): %s'
           % (key, datapackage_path, package.errors))

    resource = None
    for candidate in package.resources:
        if resource_name is None or candidate.name == resource_name:
            resource = candidate
            break
    _check(resource is not None,
           'Failed to find resource %r in the output of partition %r'
           % (resource_name, key))

    declared = resource.descriptor.get('partition') or {}
    _check(declared.get('key') == key,
           'The output of partition %r is marked as %r - refusing to mix '
           'partition versions' % (key, declared.get('key')))
    _check(declared.get('digest') == digest,
           'The output of partition %r was computed for digest %r but the '
           'manifest declares %r - refusing to use a stale partition'
           % (key, declared.get('digest'), digest))

    _check(os.path.exists(resource.source),
           'Missing data file for partition %r: %s' % (key, resource.source))

    count = package.descriptor.get('count_of_rows')
    if count is None:
        count = resource.descriptor.get('count_of_rows')
    return resource, count


def _rows(resource, schema, key):
    """Rows of a verified partition resource, cast to the unified schema."""
    if schema is None:
        yield from resource.iter(keyed=True)
        return

    field_names = [f['name'] for f in schema['fields']]
    table_schema = Schema(schema)
    for i, row in enumerate(resource.iter(keyed=True, cast=False)):
        values = [row.get(field_name) for field_name in field_names]
        try:
            values = table_schema.cast_row(values)
        except Exception as e:
            raise ValueError(
                'Row %d of partition %r does not conform to the unified '
                'schema: %s' % (i, key, e)) from e
        yield dict(zip(field_names, values))


def flow(parameters):
    partitions = parameters.get('partitions') or []
    schema = _unify_schema(parameters.get('schema'))
    resource_name = parameters['resource-name']
    metadata = copy.deepcopy(parameters.get('metadata') or {})

    # Verify every partition *before* emitting anything.  A single failure
    # aborts the whole step (and with it the publish step).
    loaded = []
    for partition in partitions:
        resource, count = _verify_partition(partition, resource_name)
        loaded.append((partition, resource, count))

    if schema is None and len(loaded) > 0:
        # No unified schema was declared - fall back to the schema of the
        # partition outputs (they are all required to be identical).
        schema = _unify_schema(loaded[0][1].descriptor.get('schema'))
    if schema is not None and len(schema['fields']) == 0:
        _check(len(loaded) == 0,
               'The unified schema has no fields - cannot represent %d '
               'partition(s)' % len(loaded))
        schema = None

    descriptor = {
        'profile': 'data-package',
        'resources': [],
    }
    descriptor.update(metadata)
    # The manifest records exactly the partition versions this package was
    # built from - removed partitions are gone, changed ones are new.
    descriptor['partition-manifest'] = {
        'partitions': [
            dict(
                key=partition['key'],
                digest=partition['digest'],
                path=partition['path'],
                **({'count_of_rows': count}
                   if count is not None else {})
            )
            for partition, _, count in loaded
        ]
    }

    emitted = schema is not None
    if emitted:
        descriptor['resources'].append({
            'name': resource_name,
            'path': 'data/%s.csv' % resource_name,
            'profile': 'tabular-data-resource',
            'schema': schema,
            PROP_STREAMING: True,
        })

    def resource_iterator():
        if not emitted:
            return
        yield _all_rows(schema, loaded)

    def _all_rows(_schema, _loaded):
        for partition, resource, _ in _loaded:
            yield from _rows(resource, _schema, partition['key'])

    return descriptor, resource_iterator()


if __name__ == '__main__':
    with ingest() as ctx:
        datapackage_, resource_iterator = flow(ctx.parameters)
        spew(datapackage_, resource_iterator, ctx.stats)
