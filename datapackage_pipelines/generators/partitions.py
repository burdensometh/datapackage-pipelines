"""Source-spec generator for partition-based data packages.

A source spec (conventionally ``partitions.source-spec.yaml``) contains a
manifest of partitions - each one has a *key*, a *path* (where its data lives)
and a *digest* (a summary of the partition's content).  From that manifest this
generator produces:

- one independent pipeline per partition, which loads that partition, applies
  the cleaning steps and dumps a qualified output into a per-partition,
  digest-addressed directory.  The output-producing step is cached, so an
  unchanged partition (same key, path and digest) reuses its previous output
  instead of being cleaned again.
- one merge pipeline, which depends on all of the partition pipelines of this
  round and merges *exactly* those confirmed partition outputs into the
  published data package.  Partitions which were removed from the manifest are
  not part of the merge, and a partition whose digest changed is recomputed
  before the merge can run.

The merge pipeline is generated even for an empty manifest, in which case it
publishes a well-defined empty result (one empty resource with the unified
schema, and an empty partition manifest).
"""
import copy
import hashlib
import json
import os
import re

from slugify import slugify

from .generator_base import GeneratorBase

GENERATOR_NAME = 'partitions'

#: Fields of a partition entry which are forwarded to the `load` processor.
_LOAD_FIELDS = ('format', 'encoding', 'compression', 'headers', 'sheet',
                'sheet_name', 'skip_rows', 'limit_rows')

_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'type': 'object',
    'properties': {
        'name': {'type': 'string'},
        'title': {'type': 'string'},
        'description': {'type': 'string'},
        'homepage': {'type': 'string'},
        'licenses': {'type': 'array'},
        # The unified schema, shared by all partitions and by the merge output
        'schema': {'type': 'object'},
        'fields': {'type': 'array'},
        # The partition manifest
        'partitions': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'key': {},
                    'name': {},
                    'id': {},
                    'path': {'type': 'string'},
                    'url': {'type': 'string'},
                    'from': {'type': 'string'},
                    'digest': {},
                    'checksum': {},
                    'hash': {},
                    'format': {'type': 'string'},
                    'encoding': {'type': 'string'},
                    'compression': {'type': 'string'},
                    'headers': {},
                    'sheet': {},
                    'options': {'type': 'object'},
                    'load': {'type': 'object'},
                    'clean': {'type': 'array'},
                    'steps': {'type': 'array'},
                }
            }
        },
        # Cleaning steps, applied to every partition by default
        'clean': {'type': 'array'},
        'cleaning': {'type': 'array'},
        'pipeline': {'type': 'array'},
        # Output configuration
        'out-path': {'type': 'string'},
        'output': {'type': 'string'},
        'published-path': {'type': 'string'},
        'publish-to': {'type': 'string'},
        'work-dir': {'type': 'string'},
        'partitions-path': {'type': 'string'},
        'resource-name': {'type': 'string'},
        'load-options': {'type': 'object'},
        'dump-options': {'type': 'object'},
        'merge': {'type': 'object'},
        'schedule': {},
    }
}

_METADATA_FIELDS = ('name', 'title', 'description', 'homepage', 'licenses')


def _first(mapping, keys, default=None):
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


def _as_schema(source):
    schema = _first(source, ('schema', 'fields'))
    if schema is None:
        return None
    schema = copy.deepcopy(schema)
    if isinstance(schema, list):
        schema = {'fields': schema}
    schema.setdefault('fields', [])
    schema.setdefault('missingValues', [''])
    if len(schema['fields']) == 0:
        # An empty schema can't be represented in a data package resource;
        # treat it as "no unified schema was declared".
        return None
    return schema


def _dump_options(value):
    options = copy.deepcopy(value or {})
    # The output location is dictated by the generator, not by the caller.
    options.pop('out-path', None)
    options.pop('out-file', None)
    return options


def _as_digest(value):
    if isinstance(value, dict):
        for key in ('sha256', 'sha1', 'md5', 'value', 'digest'):
            if key in value:
                return str(value[key])
        return json.dumps(value, sort_keys=True)
    return str(value)


def _safe_digest(digest):
    """A filesystem-safe encoding of a partition's digest.

    ``sha256:abc...`` becomes ``sha256-abc...``, so different algorithms never
    share an output directory.  If sanitizing the digests loses information a
    short hash of the full digest is appended as well.
    """
    value = digest
    algorithm = ''
    match = re.match(r'^([A-Za-z][A-Za-z0-9_-]{0,31}):(.*)$', digest)
    if match:
        algorithm, value = match.group(1), match.group(2)

    safe = re.sub(r'[^A-Za-z0-9._-]+', '-', value).strip('-.')
    if safe != value or safe == '' or len(safe) > 64:
        safe = (safe[:48] or 'digest') + '-' + \
            hashlib.sha256(digest.encode('utf8')).hexdigest()[:12]
    return '%s-%s' % (algorithm, safe) if algorithm else safe


def _safe_slug(value, fallback):
    ret = slugify(str(value))
    if not ret:
        ret = fallback + '-' + hashlib.sha256(
            str(value).encode('utf8')).hexdigest()[:8]
    return ret


def normalize_partitions(source):
    """Validate and normalize the manifest.

    Returns a list of ``dict(key, path, digest, load, steps)``.  Raises
    ``ValueError`` for a malformed manifest (duplicate keys, missing
    path/digest, etc.).
    """
    raw = _first(source, ('partitions',))
    if raw is None:
        raw = []
    if not isinstance(raw, list):
        raise ValueError('"partitions" must be a list')

    partitions = []
    seen_keys = set()
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError('Partition #%d must be an object' % index)

        key = _first(item, ('key', 'name', 'id'))
        if key is None:
            raise ValueError('Partition #%d is missing a "key"' % index)
        key = str(key)
        if key in seen_keys:
            raise ValueError('Duplicate partition key %r' % key)
        seen_keys.add(key)

        path = _first(item, ('path', 'url', 'from'))
        if path is None:
            raise ValueError('Partition %r is missing a "path"' % key)

        digest = _first(item, ('digest', 'checksum', 'hash'))
        if digest is None:
            raise ValueError('Partition %r is missing a "digest"' % key)
        digest = _as_digest(digest)
        if digest == '':
            raise ValueError('Partition %r has an empty "digest"' % key)

        load = {}
        load.update(item.get('load') or {})
        load.update(item.get('options') or {})
        for parameter in _LOAD_FIELDS:
            if parameter in item:
                load[parameter] = item[parameter]
        load['from'] = path

        steps = _first(item, ('clean', 'steps'), []) or []
        if not isinstance(steps, list):
            raise ValueError('Partition %r "steps" must be a list' % key)
        for step in steps:
            if not (isinstance(step, dict) and
                    ('run' in step or 'flow' in step)):
                raise ValueError('Partition %r contains an invalid step: %r'
                                 % (key, step))
        steps = copy.deepcopy(steps)

        partitions.append(dict(key=key, path=path, digest=digest,
                               load=load, steps=steps))
    return partitions


def _check_steps(value, what):
    if not isinstance(value, list):
        raise ValueError('"%s" must be a list of pipeline steps' % what)
    for step in value:
        if not (isinstance(step, dict) and ('run' in step or 'flow' in step)):
            raise ValueError('"%s" contains an invalid step: %r' % (what, step))
    return copy.deepcopy(value)


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return copy.deepcopy(_SCHEMA)

    def internal_validate(self, source):
        if not super(Generator, self).internal_validate(source):
            return False
        try:
            normalize_partitions(source)
            _check_steps(_first(source, ('clean', 'cleaning', 'pipeline'), []),
                         'clean')
            merge = source.get('merge') or {}
            _check_steps(merge.get('steps') or [], 'merge.steps')
        except ValueError:
            return False
        return True

    @classmethod
    def generate_pipeline(cls, source, base):
        partitions = normalize_partitions(source)

        dataset_name = _first(source, ('name',), None)
        dataset_slug = _safe_slug(dataset_name or 'partitions', 'partitions')
        resource_name = source.get('resource-name') or dataset_slug
        out_path = _first(
            source,
            ('out-path', 'output', 'published-path', 'publish-to'),
            'published'
        )
        work_dir = _first(
            source,
            ('work-dir', 'partitions-path'),
            out_path.rstrip('/\\') + '.partitions'
        )
        load_options = copy.deepcopy(source.get('load-options') or {})
        dump_options = _dump_options(source.get('dump-options'))

        clean_steps = _check_steps(
            _first(source, ('clean', 'cleaning', 'pipeline'), []), 'clean')

        merge_config = source.get('merge') or {}
        merge_steps = _check_steps(merge_config.get('steps') or [],
                                   'merge.steps')
        merge_out_path = merge_config.get('out-path') or out_path
        merge_options = _dump_options(merge_config.get('options'))
        merge_options.update(_dump_options(
            merge_config.get('dump-options')))

        schema = _as_schema(source)

        used_slugs = set()
        pipelines = []
        merge_partitions = []

        for partition in partitions:
            key = partition['key']
            digest = partition['digest']

            key_slug = _safe_slug(key, 'partition')
            base_slug = key_slug
            suffix = 1
            while key_slug in used_slugs:
                suffix += 1
                key_slug = '%s-%d' % (base_slug, suffix)
            used_slugs.add(key_slug)

            # Outputs are digest-addressed: a changed digest can never read
            # (or overwrite) the output of another version of the partition.
            partition_dir = os.path.join(
                work_dir, key_slug, _safe_digest(digest))

            load_parameters = {'name': resource_name}
            load_parameters.update(load_options)
            load_parameters.update(partition['load'])

            pipeline_steps = [{
                'run': 'load',
                'parameters': load_parameters,
            }]

            pipeline_steps.extend(copy.deepcopy(clean_steps))
            pipeline_steps.extend(copy.deepcopy(partition['steps']))

            if schema is not None:
                # Cleaning steps may add/remove/reorder fields freely - make
                # sure the qualified output still conforms to the unified
                # schema (an unconforming partition fails, it isn't merged).
                pipeline_steps.append({
                    'run': 'partition_schema',
                    'parameters': {
                        'schema': schema,
                        'resources': [resource_name],
                    },
                })

            # Stamp the output with the partition key and digest, and cache
            # this step.  The digest is part of the step's cache hash, so a
            # changed digest always triggers a real recomputation.  The cache
            # file is only renamed into place after the step completes, so a
            # failed partition never leaves a reusable output behind.
            pipeline_steps.append({
                'run': 'update_resource',
                'parameters': {
                    'resources': [resource_name],
                    'metadata': {
                        'partition': {
                            'key': key,
                            'digest': digest,
                        }
                    }
                },
                'cache': True,
            })

            pipeline_steps.append({
                'run': 'publish_datapackage',
                'parameters': {
                    'out-path': partition_dir,
                    'options': dump_options,
                },
            })

            pipeline_id = os.path.join(
                base, dataset_slug, 'partition', key_slug)
            pipelines.append((pipeline_id, {'pipeline': pipeline_steps}))

            merge_partitions.append({
                'key': key,
                'digest': digest,
                'path': partition['path'],
                'datapackage': os.path.join(
                    partition_dir, 'datapackage.json'),
            })

        merge_pipeline = [{
            'run': 'merge_partitions',
            'parameters': {
                # Only the confirmed partitions of this round - never a
                # listing of what happens to exist on disk.
                'partitions': merge_partitions,
                'schema': schema,
                'resource-name': resource_name,
                'metadata': {
                    key: copy.deepcopy(source[key])
                    for key in _METADATA_FIELDS
                    if key in source
                },
            }
        }]
        merge_pipeline.extend(merge_steps)
        merge_pipeline.append({
            'run': 'publish_datapackage',
            'parameters': {
                'out-path': merge_out_path,
                'options': merge_options,
            }
        })

        merge_details = {
            'pipeline': merge_pipeline,
            'dependencies': [
                {'pipeline': pipeline_id}
                for pipeline_id, _ in pipelines
            ],
        }

        schedule = source.get('schedule')
        if schedule is not None:
            if isinstance(schedule, str):
                schedule = {'crontab': schedule}
            merge_details['schedule'] = schedule

        pipelines.append((os.path.join(base, dataset_slug, 'merge'),
                          merge_details))
        return pipelines
