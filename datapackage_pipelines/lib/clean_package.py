"""
Whole-package cleaning processor.

Pipeline step that validates every row of the selected resources against
their declared table schema, enforces primary-key uniqueness and resolves
all foreign-key references to a fixed point.  Each selected resource is
emitted as a clean stream (ready for an SQL sink) alongside a quarantine
stream containing every rejected row with its original payload, source and
rejection reason.

Parameters
----------
resources (optional):
    Resource name / list of names to clean.  Defaults to every resource.
quarantine_suffix (optional):
    Suffix appended to a resource name to build its quarantine sibling.
    Defaults to ``_quarantine``.
"""
from dataflows import Flow

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.lib.cleaning.processor import clean_package


def flow(parameters):
    return Flow(
        clean_package(
            resources=parameters.get('resources'),
            quarantine_suffix=parameters.get('quarantine_suffix',
                                             '_quarantine'),
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
