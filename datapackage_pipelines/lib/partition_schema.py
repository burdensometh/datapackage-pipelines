"""Enforce the unified schema on a partition's resource.

The generator adds this step after the cleaning steps of every partition
pipeline.  It makes the qualified output conform to the unified schema - the
fields are exactly the unified schema's fields, in its order, cast to its
types.  Rows which can't be cast make the partition fail, which in turn keeps
the merge pipeline from publishing anything.
"""
import copy
import logging

from tableschema import Schema
from tableschema.exceptions import CastError

from datapackage_pipelines.utilities.resources import streaming
from datapackage_pipelines.wrapper import ingest


def unify_schema(schema):
    schema = copy.deepcopy(schema)
    schema.setdefault('fields', [])
    schema.setdefault('missingValues', [''])
    return schema


def cast_rows(resource, schema):
    field_names = [f['name'] for f in schema['fields']]
    table_schema = Schema(schema)
    for i, row in enumerate(resource):
        values = [row.get(field_name) for field_name in field_names]
        try:
            values = table_schema.cast_row(values)
        except CastError as e:
            messages = '; '.join(str(err) for err in e.errors)
            logging.error('Row %d does not conform to the unified schema: %s',
                          i, messages)
            raise ValueError(
                'Row %d of resource %r does not conform to the unified schema: %s'
                % (i, resource.spec.get('name'), messages)) from e
        yield dict(zip(field_names, values))


def flow(parameters, datapackage, resource_iterator):
    schema = unify_schema(parameters['schema'])
    resources = parameters.get('resources')
    matched = set()

    for resource in datapackage.get('resources', []):
        if resources is None or resource.get('name') in resources:
            resource['schema'] = copy.deepcopy(schema)
            matched.add(resource['name'])

    assert len(matched) > 0, \
        'Failed to find any resource matching %r' % (resources,)

    def new_resource_iterator(_resources):
        for resource in _resources:
            if streaming(resource.spec) and \
                    resource.spec.get('name') in matched:
                yield cast_rows(resource, schema)
            else:
                yield resource

    return datapackage, new_resource_iterator(resource_iterator)


if __name__ == '__main__':
    with ingest() as ctx:
        ctx.datapackage, ctx.resource_iterator = \
            flow(ctx.parameters, ctx.datapackage, ctx.resource_iterator)
