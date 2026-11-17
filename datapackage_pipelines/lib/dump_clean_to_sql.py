"""
SQL sink for cleaned data packages with cross-table foreign keys.

Unlike ``dump_to_sql`` (whose per-table storage prefix prevents cross-table
foreign keys from being created), this processor writes through a single
prefix-free storage so the clean resources' primary and foreign key
constraints are created in the database and the rows are verified after the
load.  Use it downstream of ``clean_package`` and map only the clean
resources to tables; quarantine resources stay in the stream.
"""
from dataflows import Flow

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.lib.cleaning.sql_sink import dump_clean_to_sql


def flow(parameters):
    return Flow(
        dump_clean_to_sql(
            parameters['tables'],
            engine=parameters.get('engine', 'env://DPP_DB_ENGINE'),
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
