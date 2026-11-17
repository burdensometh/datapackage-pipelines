"""Publish a data package in an all-or-nothing manner.

The package is first dumped into a temporary sibling directory.  Only once the
dump has fully completed - every row was serialized, and the descriptor and all
data files were written to the stream - is the result swapped into the
published location.  A failure anywhere upstream (in any partition pipeline, in
the merge step, or while writing the package) therefore leaves the previously
published data package untouched, instead of publishing half of a new one.
"""
import os
import shutil
import tempfile

from dataflows import Flow, dump_to_path

from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.utilities.stat_utils import STATS_DPP_KEY, \
    STATS_OUT_DP_URL_KEY
from datapackage_pipelines.wrapper import ingest


def _swap_into_place(temp_path, out_path):
    """Move ``temp_path`` to ``out_path``, replacing whatever was there."""
    out_path = os.path.abspath(out_path)
    if not os.path.exists(out_path):
        os.rename(temp_path, out_path)
        return

    old_path = tempfile.mkdtemp(
        prefix='.' + os.path.basename(out_path) + '.old-',
        dir=os.path.dirname(out_path))
    old_target = os.path.join(old_path, 'previous')
    os.rename(out_path, old_target)
    try:
        os.rename(temp_path, out_path)
    except Exception:
        # Put the previous package back before propagating the failure
        os.rename(old_target, out_path)
        raise
    else:
        shutil.rmtree(old_path, ignore_errors=True)


def flow(parameters, state):
    out_path = os.path.abspath(parameters['out-path'])
    options = dict(parameters.get('options') or {})
    options.pop('out-path', None)

    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    state['out-path'] = out_path
    state['temp-path'] = tempfile.mkdtemp(
        prefix='.' + os.path.basename(out_path) + '.new-',
        dir=os.path.dirname(out_path) or '.')
    return Flow(dump_to_path(state['temp-path'], **options))


def publish(parameters, ctx, state):
    _swap_into_place(state['temp-path'], state['out-path'])
    state['published'] = True


def cleanup(state):
    temp_path = state.get('temp-path')
    if temp_path and not state.get('published'):
        shutil.rmtree(temp_path, ignore_errors=True)


if __name__ == '__main__':
    state = {}
    try:
        # `spew` (and with it, the dump) happens when the context exits, so
        # `publish` only runs after the whole package has been written.
        with ingest() as ctx:
            # Set before `spew_flow`, which replaces ctx.stats with
            # `MergeableStats` during the dump.
            ctx.stats[STATS_DPP_KEY] = {
                STATS_OUT_DP_URL_KEY: os.path.join(ctx.parameters['out-path'],
                                                   'datapackage.json')
            }
            spew_flow(flow(ctx.parameters, state), ctx)
        publish(ctx.parameters, ctx, state)
    finally:
        cleanup(state)
