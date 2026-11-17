"""Tests for the partition source-spec generator and its processors."""
import json
import os

import yaml

from datapackage_pipelines.generators.partitions import Generator


SPEC_DIR = './specs'


def generate(source):
    return dict(Generator.generate_pipeline(source, SPEC_DIR))


def partition_steps(pipeline_id):
    return generate(PARTITION_SPEC)[pipeline_id]['pipeline']


PARTITION_SPEC = {
    'name': 'daily',
    'schema': {
        'fields': [
            {'name': 'id', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
        ]
    },
    'clean': [
        {'run': 'set_types', 'parameters': {'types': {'id': {'type': 'integer'}}}},
    ],
    'partitions': [
        {'key': 'p1', 'path': 'data/p1.csv', 'digest': 'sha256:aaa'},
        {'key': 'p2', 'path': 'data/p2.csv', 'digest': 'sha256:bbb'},
    ],
    'out-path': 'published',
}


class TestSourceSpecValidation(object):

    def test_valid_spec(self):
        assert Generator().internal_validate(PARTITION_SPEC)

    def test_empty_manifest_is_valid(self):
        assert Generator().internal_validate({'partitions': []})

    def test_missing_manifest_is_a_valid_empty_manifest(self):
        assert Generator().internal_validate({'name': 'x'})

    def test_duplicate_keys_rejected(self):
        assert not Generator().internal_validate({'partitions': [
            {'key': 'a', 'path': 'a.csv', 'digest': '1'},
            {'key': 'a', 'path': 'b.csv', 'digest': '2'},
        ]})

    def test_missing_digest_rejected(self):
        assert not Generator().internal_validate(
            {'partitions': [{'key': 'a', 'path': 'a.csv'}]})

    def test_missing_path_rejected(self):
        assert not Generator().internal_validate(
            {'partitions': [{'key': 'a', 'digest': '1'}]})

    def test_invalid_clean_step_rejected(self):
        assert not Generator().internal_validate({
            'clean': [{'parameter': 1}],
            'partitions': [{'key': 'a', 'path': 'a.csv', 'digest': '1'}],
        })


class TestGeneratedPipelines(object):

    def test_one_independent_pipeline_per_partition(self):
        pipelines = generate(PARTITION_SPEC)
        assert set(pipelines) == {
            './specs/daily/partition/p1',
            './specs/daily/partition/p2',
            './specs/daily/merge',
        }
        p1 = pipelines['./specs/daily/partition/p1']['pipeline']
        assert [step['run'] for step in p1] == [
            'load', 'set_types', 'partition_schema', 'update_resource',
            'publish_datapackage',
        ]
        assert p1[0]['parameters']['from'] == 'data/p1.csv'
        assert p1[0]['parameters']['name'] == 'daily'

    def test_partition_output_is_digest_addressed_and_cached(self):
        p1 = partition_steps('./specs/daily/partition/p1')
        cached = [step for step in p1 if step.get('cache')]
        assert len(cached) == 1
        # The digest participates in the cached step's parameters - and
        # therefore in its cache hash.
        assert cached[0]['parameters']['metadata']['partition'] == {
            'key': 'p1', 'digest': 'sha256:aaa'}
        dump = p1[-1]
        assert dump['parameters']['out-path'] == \
            os.path.join('published.partitions', 'p1', 'sha256-aaa')

    def test_digest_change_changes_cache_hash_inputs(self):
        spec = json.loads(json.dumps(PARTITION_SPEC))
        spec['partitions'][0]['digest'] = 'sha256:changed'
        changed = generate(spec)['./specs/daily/partition/p1']['pipeline']
        original = partition_steps('./specs/daily/partition/p1')
        assert changed[-1]['parameters']['out-path'] != \
            original[-1]['parameters']['out-path']

    def test_merge_depends_on_all_this_round_partitions(self):
        merge = generate(PARTITION_SPEC)['./specs/daily/merge']
        assert merge['dependencies'] == [
            {'pipeline': './specs/daily/partition/p1'},
            {'pipeline': './specs/daily/partition/p2'},
        ]
        assert merge['pipeline'][0]['run'] == 'merge_partitions'
        assert merge['pipeline'][-1]['run'] == 'publish_datapackage'

    def test_merge_uses_only_confirmed_partitions(self):
        merge = generate(PARTITION_SPEC)['./specs/daily/merge']
        partitions = merge['pipeline'][0]['parameters']['partitions']
        assert [p['key'] for p in partitions] == ['p1', 'p2']
        # The manifest (key, digest) is carried into the merge, so the merge
        # can only read outputs computed for those exact versions.
        assert [p['digest'] for p in partitions] == \
            ['sha256:aaa', 'sha256:bbb']
        assert all(p['datapackage'].endswith('datapackage.json')
                   for p in partitions)

    def test_removed_partition_is_not_merged(self):
        spec = json.loads(json.dumps(PARTITION_SPEC))
        del spec['partitions'][1]
        pipelines = generate(spec)
        assert './specs/daily/partition/p2' not in pipelines
        merge = pipelines['./specs/daily/merge']
        assert [p['key'] for p in
                merge['pipeline'][0]['parameters']['partitions']] == ['p1']
        assert merge['dependencies'] == [
            {'pipeline': './specs/daily/partition/p1'}]

    def test_empty_manifest_generates_merge_only(self):
        pipelines = generate({'name': 'empty', 'partitions': [],
                              'schema': PARTITION_SPEC['schema']})
        assert set(pipelines) == {'./specs/empty/merge'}
        merge = pipelines['./specs/empty/merge']
        assert merge['dependencies'] == []
        assert merge['pipeline'][0]['parameters']['partitions'] == []

    def test_no_schema_means_no_schema_step(self):
        spec = json.loads(json.dumps(PARTITION_SPEC))
        del spec['schema']
        p1 = generate(spec)['./specs/daily/partition/p1']['pipeline']
        assert 'partition_schema' not in [step['run'] for step in p1]
        merge = generate(spec)['./specs/daily/merge']
        assert merge['pipeline'][0]['parameters']['schema'] is None

    def test_schedule_is_attached_to_merge(self):
        spec = json.loads(json.dumps(PARTITION_SPEC))
        spec['schedule'] = '0 3 * * *'
        merge = generate(spec)['./specs/daily/merge']
        assert merge['schedule'] == {'crontab': '0 3 * * *'}

    def test_generated_pipeline_specs_are_valid(self):
        from datapackage_pipelines.specs.schemas.validator import \
            validate_pipeline
        for pipeline_id, details in generate(PARTITION_SPEC).items():
            errors = []
            assert validate_pipeline(details, errors), (pipeline_id, errors)

    def test_generated_specs_are_yaml_serializable(self):
        for details in generate(PARTITION_SPEC).values():
            yaml.safe_dump(details)
