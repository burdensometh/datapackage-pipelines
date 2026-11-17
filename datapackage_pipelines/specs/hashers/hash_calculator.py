import hashlib

from ...utilities.extended_json import json
from ..parsers.base_parser import PipelineSpec
from ..dependency_bindings import live_bindings

from ..errors import SpecError
from .dependency_resolver import compose_dependency_hash, resolve_dependencies


def calculate_step_hashes(spec: PipelineSpec, cache_hash):
    """Derive the per-step cache hashes from a pipeline-level hash."""
    for step in spec.pipeline_details['pipeline']:
        m = hashlib.md5()
        m.update(cache_hash.encode('ascii'))
        with open(step['executor'], 'rb') as f:
            m.update(f.read())
        serialized_step = dict(step)
        serialized_step.pop('_cache_hash', None)
        m.update(json.dumps(serialized_step, ensure_ascii=True, sort_keys=True)
                 .encode('ascii'))
        cache_hash = m.hexdigest()
        step['_cache_hash'] = cache_hash
    return cache_hash


def rebase_with_bindings(spec: PipelineSpec, dependency_bindings):
    """Recalculate the spec's hashes against a fixed set of bindings.

    Used when an execution is started with dependency versions that were
    selected earlier (e.g. a queued task, or a runner subprocess).  The step
    cache hashes must be derived from the same bindings that will actually be
    used, otherwise the run could pick up a cache computed against a
    different data version.
    """
    parts = getattr(spec, 'dependency_hash_parts', None)
    if not parts:
        # No dependency hash recorded (missing dependencies were ignored) -
        # there is nothing to rebase against, keep the scan-time hash.
        spec.dependency_bindings = dependency_bindings
        return spec.cache_hash

    if dependency_bindings == getattr(spec, 'dependency_bindings', None):
        return spec.cache_hash

    cache_hash = compose_dependency_hash(parts, dependency_bindings)
    cache_hash = calculate_step_hashes(spec, cache_hash)
    spec.cache_hash = cache_hash
    spec.dependency_bindings = dependency_bindings
    return cache_hash


class HashCalculator(object):

    def __init__(self):
        self.all_pipeline_ids = {}

    def calculate_hash(self, spec: PipelineSpec, status_mgr, ignore_missing_deps=False):

        cache_hash = None
        if spec.pipeline_id in self.all_pipeline_ids:
            message = 'Duplicate key {0} in {1}' \
                .format(spec.pipeline_id, spec.path)
            spec.validation_errors.append(SpecError('Duplicate Pipeline Id', message))

        else:
            if ignore_missing_deps:
                cache_hash = ''
            else:
                # Select the concrete data version of each dependency that
                # this pipeline will be calculated against.
                spec.dependency_bindings = live_bindings(
                    spec.pipeline_details.get('dependencies', ()),
                    status_mgr,
                    spec.path)
                cache_hash = resolve_dependencies(
                    spec, self.all_pipeline_ids, status_mgr,
                    spec.dependency_bindings)

            self.all_pipeline_ids[spec.pipeline_id] = spec
            if len(spec.validation_errors) > 0:
                return cache_hash

            cache_hash = calculate_step_hashes(spec, cache_hash)

        spec.cache_hash = cache_hash
