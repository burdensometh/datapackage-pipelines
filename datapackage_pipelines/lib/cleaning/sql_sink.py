"""
SQL sink for cleaned data packages.

The standard ``dump_to_sql`` processor builds one tableschema-sql storage
per table using the table name as a storage prefix.  Two problems follow:

* the storage prefix is prepended to referenced table names, so cross-table
  foreign keys resolve to names that do not exist, and
* the shared schema mapper reuses a local table-name variable while
  generating foreign keys, so a table mixing self-references and
  cross-table references gets the wrong table on some of its constraints.

Both failures occur even on perfectly valid data.  ``dump_clean_to_sql``
therefore builds the SQLAlchemy table metadata directly from the declared
table schemas (correct primary keys, foreign keys and nullability) and
inserts the clean rows through a single engine.  Only the field-type
mapping of tableschema-sql is reused.

Only resources named in the ``tables`` parameter are written - quarantine
siblings are not mapped and therefore stay in the stream instead of being
loaded into the database.

On SQLite, foreign key enforcement is relaxed for the duration of the load
(via a connection-level pragma) and the whole load is verified afterwards
with ``PRAGMA foreign_key_check``.  This permits loading self-references and
healthy circular reference groups that cannot be inserted statement by
statement.  Any remaining violation raises.
"""
from __future__ import absolute_import, division, print_function

import copy
import logging
import os

from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    create_engine,
    event,
    text,
)
from tableschema_sql.mapper import Mapper

from dataflows import DataStreamProcessor

from dataflows.processors.dumpers.to_sql import OBJECT_FIXERS


class dump_clean_to_sql(DataStreamProcessor):

    def __init__(self, tables, engine='env://DPP_DB_ENGINE'):
        super(dump_clean_to_sql, self).__init__()
        if isinstance(engine, str):
            if engine.startswith('env://'):
                env_var = engine[6:]
                engine = os.environ.get(env_var)
                if engine is None:
                    raise ValueError(
                        "Couldn't connect to DB - Please set your '%s' "
                        'environment variable' % env_var)
            engine = create_engine(engine)
        self.engine = engine
        # resource name -> table name
        self.table_for_resource = {
            spec['resource-name']: table_name
            for table_name, spec in tables.items()
        }
        self._fk_listener = None

    def process_datapackage(self, dp):
        mapped = set(self.table_for_resource)
        known = {resource['name'] for resource in dp.descriptor['resources']}
        unknown = mapped - known
        if unknown:
            raise ValueError(
                'dump_clean_to_sql: unknown resources %r' % sorted(unknown))
        return dp

    def process_resources(self, resources):
        dialect = self.engine.dialect.name
        wrappers = list(resources)
        descriptors = {
            wrapper.res.name: wrapper.res.descriptor for wrapper in wrappers
        }
        original_schemas = {
            name: descriptor.get('schema', {})
            for name, descriptor in descriptors.items()
            if name in self.table_for_resource
        }
        mapped = {
            name: self._normalize_schema(dialect, schema)
            for name, schema in original_schemas.items()
        }

        metadata = self._build_metadata(mapped)

        self._install_fk_relaxation(dialect)
        try:
            # Rewrite semantics: replace every mapped table outright.
            metadata.drop_all(self.engine)
            metadata.create_all(self.engine)
            tables = metadata.tables

            for wrapper in wrappers:
                name = wrapper.res.name
                table_name = self.table_for_resource.get(name)
                if table_name is None:
                    # Unmapped resources (e.g. quarantine streams) pass
                    # through untouched.
                    yield wrapper
                    continue
                actions = self._row_actions(dialect, original_schemas[name])
                yield self._writer(wrapper, tables[table_name], actions)

            self._verify_foreign_keys(dialect)
        finally:
            self._remove_fk_relaxation()

    # ------------------------------------------------------------------
    # Schema construction
    # ------------------------------------------------------------------

    def _build_metadata(self, mapped):
        dialect = self.engine.dialect.name
        mapper = Mapper(prefix='', dialect=dialect)
        metadata = MetaData()

        # Build all Table objects first so foreign keys can reference tables
        # declared later (forward references).
        tables = {}
        for resource_name, schema in mapped.items():
            table_name = self.table_for_resource[resource_name]
            columns = self._build_columns(schema, mapper)
            tables[resource_name] = Table(table_name, metadata, *columns)

        for resource_name, schema in mapped.items():
            table = tables[resource_name]
            self._append_constraints(table, schema, mapped)
        return metadata

    def _build_columns(self, schema, mapper):
        columns = []
        for field in schema.get('fields', []):
            sql_type = mapper.convert_type(field.get('type', 'string'))
            if sql_type is None:
                # Unsupported/complex type: keep the value as text.
                sql_type = mapper.convert_type('string')
            columns.append(Column(
                field['name'],
                sql_type,
                nullable=not self._field_required(field),
                unique=bool(field.get('constraints', {}).get('unique', False)),
            ))
        return columns

    def _append_constraints(self, table, schema, mapped):
        primary_key = schema.get('primaryKey')
        if primary_key:
            if isinstance(primary_key, str):
                primary_key = [primary_key]
            table.append_constraint(PrimaryKeyConstraint(*primary_key))

        for fk in schema.get('foreignKeys', []) or []:
            fields = fk.get('fields', [])
            if isinstance(fields, str):
                fields = [fields]
            reference = fk.get('reference', {}) or {}
            ref_resource = reference.get('resource', '')
            ref_fields = reference.get('fields', [])
            if isinstance(ref_fields, str):
                ref_fields = [ref_fields]
            if not fields or not ref_fields:
                continue
            # Empty reference resource means a self reference.
            target_resource = ref_resource or table.name
            target_table = self.table_for_resource.get(
                target_resource, target_resource)
            referenced = [
                '%s.%s' % (target_table, column) for column in ref_fields
            ]
            table.append_constraint(
                ForeignKeyConstraint(fields, referenced))

    @staticmethod
    def _field_required(field):
        return bool(field.get('constraints', {}).get('required', False))

    # ------------------------------------------------------------------
    # Row writing
    # ------------------------------------------------------------------

    def _writer(self, wrapper, table, actions):
        def generate():
            logging.info('Writing resource %s -> table %s',
                         wrapper.res.name, table.name)
            rows = list(self._normalize_rows(wrapper, actions))
            if rows:
                with self.engine.begin() as conn:
                    conn.execute(table.insert(), rows)
            # Re-emit the rows so the resource iterator stays a real,
            # non-None iterator for any downstream processor.
            for row in rows:
                yield row
        return generate()

    def _normalize_schema(self, dialect, schema):
        schema = copy.deepcopy(schema)
        if dialect == 'sqlite':
            for field in schema['fields']:
                if field['type'] in ('object', 'array'):
                    field['type'] = 'string'
        return schema

    def _row_actions(self, dialect, schema):
        actions = {}
        fixers = OBJECT_FIXERS.get(dialect, [])
        for field in schema['fields']:
            if field['type'] in ('array', 'object') and fixers:
                actions[field['name']] = list(fixers)
        return actions

    def _normalize_rows(self, wrapper, actions):
        for row in wrapper:
            for name, action_list in actions.items():
                for action in action_list:
                    row[name] = action(row.get(name))
            yield row

    # ------------------------------------------------------------------
    # Foreign key relaxation / verification (SQLite)
    # ------------------------------------------------------------------

    def _install_fk_relaxation(self, dialect):
        if dialect != 'sqlite':
            return

        def listener(dbapi_connection, _record):
            cursor = dbapi_connection.cursor()
            cursor.execute('PRAGMA foreign_keys=OFF')
            cursor.close()

        event.listen(self.engine, 'connect', listener)
        self._fk_listener = listener

    def _remove_fk_relaxation(self):
        if self._fk_listener is not None:
            event.remove(self.engine, 'connect', self._fk_listener)
            self._fk_listener = None

    def _verify_foreign_keys(self, dialect):
        if dialect != 'sqlite':
            return
        with self.engine.connect() as conn:
            violations = conn.execute(
                text('PRAGMA foreign_key_check')).fetchall()
        if violations:
            raise RuntimeError(
                'Foreign key verification failed after load: %r'
                % (violations,))
