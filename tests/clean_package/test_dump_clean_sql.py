"""
Tests for the dump_clean_to_sql sink: cross-table and self-referencing
foreign keys, rewrite semantics and quarantine exclusion.
"""
import os

from sqlalchemy import create_engine, inspect, text

from dataflows import Flow, load

from datapackage_pipelines.lib.cleaning.processor import clean_package
from datapackage_pipelines.lib.cleaning.sql_sink import dump_clean_to_sql


def _package():
    return {
        'name': 'pkg',
        'resources': [
            {
                'name': 'departments',
                'path': 'departments.csv',
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'integer',
                         'constraints': {'required': True}},
                        {'name': 'manager_id', 'type': 'integer'},
                    ],
                    'primaryKey': ['id'],
                    'foreignKeys': [
                        {'fields': ['manager_id'],
                         'reference': {'resource': '', 'fields': ['id']}},
                    ],
                },
            },
            {
                'name': 'employees',
                'path': 'employees.csv',
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'integer',
                         'constraints': {'required': True}},
                        {'name': 'dept_id', 'type': 'integer',
                         'constraints': {'required': True}},
                        {'name': 'manager_id', 'type': 'integer'},
                    ],
                    'primaryKey': ['id'],
                    'foreignKeys': [
                        {'fields': ['dept_id'],
                         'reference': {'resource': 'departments',
                                       'fields': ['id']}},
                        {'fields': ['manager_id'],
                         'reference': {'resource': '', 'fields': ['id']}},
                    ],
                },
            },
        ],
    }


def _raw_rows():
    return [
        iter([
            {'id': '1', 'manager_id': '2'},     # self cycle 1<->2
            {'id': '2', 'manager_id': '1'},
            {'id': '9', 'manager_id': ''},
            {'id': '9', 'manager_id': ''},      # duplicate PK
        ]),
        iter([
            {'id': '10', 'dept_id': '1', 'manager_id': ''},
            {'id': '11', 'dept_id': '1', 'manager_id': '12'},
            {'id': '12', 'dept_id': '1', 'manager_id': '11'},
            {'id': '13', 'dept_id': '9', 'manager_id': ''},  # orphaned
        ]),
    ]


def _run(db_path):
    engine_url = 'sqlite:///%s' % db_path
    results, dp, _ = Flow(
        load((_package(), _raw_rows())),
        clean_package(),
        dump_clean_to_sql(
            {
                'departments': {'resource-name': 'departments'},
                'employees': {'resource-name': 'employees'},
            },
            engine=engine_url,
        ),
    ).results()
    return engine_url, results, dp


def test_creates_real_primary_and_foreign_keys(tmp_path):
    db_path = os.path.join(str(tmp_path), 'clean.sqlite')
    engine_url, _results, dp = _run(db_path)
    engine = create_engine(engine_url)
    inspector = inspect(engine)

    # Only mapped clean resources became tables; quarantine did not.
    assert set(inspector.get_table_names()) == {'departments', 'employees'}
    dp_names = [r['name'] for r in dp.descriptor['resources']]
    assert 'departments_quarantine' in dp_names
    assert 'employees_quarantine' in dp_names

    assert inspector.get_pk_constraint(
        'departments')['constrained_columns'] == ['id']
    assert inspector.get_pk_constraint(
        'employees')['constrained_columns'] == ['id']

    dept_fk = [(f['constrained_columns'], f['referred_table'],
                f['referred_columns'])
               for f in inspector.get_foreign_keys('departments')]
    assert dept_fk == [
        (['manager_id'], 'departments', ['id'])]

    emp_fk = sorted(
        (f['constrained_columns'], f['referred_table'],
         f['referred_columns'])
        for f in inspector.get_foreign_keys('employees'))
    assert emp_fk == sorted([
        (['dept_id'], 'departments', ['id']),
        (['manager_id'], 'employees', ['id']),
    ])


def test_only_clean_rows_are_loaded_and_verified(tmp_path):
    db_path = os.path.join(str(tmp_path), 'clean.sqlite')
    engine_url, _results, _dp = _run(db_path)
    engine = create_engine(engine_url)

    with engine.connect() as conn:
        departments = conn.execute(text(
            'SELECT id, manager_id FROM departments ORDER BY id')).fetchall()
        employees = conn.execute(text(
            'SELECT id, dept_id, manager_id FROM employees ORDER BY id')
        ).fetchall()
        fk_check = conn.execute(
            text('PRAGMA foreign_key_check')).fetchall()

    # Circular groups and nullable keys load; duplicates/orphans do not.
    assert [tuple(r) for r in departments] == [(1, 2), (2, 1)]
    assert [tuple(r) for r in employees] == [
        (10, 1, None), (11, 1, 12), (12, 1, 11)]
    assert fk_check == []


def test_rewrite_mode_replaces_tables_on_rerun(tmp_path):
    db_path = os.path.join(str(tmp_path), 'clean.sqlite')
    _run(db_path)
    # A second run drops and recreates the tables rather than appending.
    _run(db_path)
    engine = create_engine('sqlite:///%s' % db_path)
    with engine.connect() as conn:
        count = conn.execute(text(
            'SELECT COUNT(*) FROM departments')).scalar()
    assert count == 2


def test_unknown_resource_raises(tmp_path):
    db_path = os.path.join(str(tmp_path), 'clean.sqlite')
    flow = Flow(
        load((_package(), _raw_rows())),
        dump_clean_to_sql(
            {'t': {'resource-name': 'does-not-exist'}},
            engine='sqlite:///%s' % db_path,
        ),
    )
    try:
        flow.results()
    except Exception as exc:
        assert 'does-not-exist' in str(exc)
    else:
        raise AssertionError('expected failure for unknown resource')


def test_object_and_array_fields_are_serialized_on_sqlite(tmp_path):
    db_path = os.path.join(str(tmp_path), 'objects.sqlite')
    descriptor = {
        'name': 'pkg',
        'resources': [{
            'name': 't',
            'path': 't.csv',
            'schema': {
                'fields': [
                    {'name': 'id', 'type': 'integer'},
                    {'name': 'meta', 'type': 'object'},
                    {'name': 'tags', 'type': 'array'},
                ],
            },
        }],
    }
    rows = [iter([{'id': 1, 'meta': {'a': 1}, 'tags': ['x', 'y']}])]
    Flow(
        load((descriptor, rows)),
        dump_clean_to_sql(
            {'t': {'resource-name': 't'}},
            engine='sqlite:///%s' % db_path,
        ),
    ).results()

    engine = create_engine('sqlite:///%s' % db_path)
    with engine.connect() as conn:
        stored = conn.execute(text('SELECT id, meta, tags FROM t')).fetchall()
    assert stored == [(1, '{"a": 1}', '["x", "y"]')]


def test_all_rows_quarantined_still_creates_empty_table(tmp_path):
    db_path = os.path.join(str(tmp_path), 'empty.sqlite')
    descriptor = {
        'name': 'pkg',
        'resources': [{
            'name': 't',
            'path': 't.csv',
            'schema': {
                'fields': [
                    {'name': 'id', 'type': 'integer',
                     'constraints': {'required': True}},
                ],
                'primaryKey': ['id'],
            },
        }],
    }
    rows = [iter([{'id': 'bad'}, {'id': 'also-bad'}])]
    Flow(
        load((descriptor, rows)),
        clean_package(),
        dump_clean_to_sql(
            {'t': {'resource-name': 't'}},
            engine='sqlite:///%s' % db_path,
        ),
    ).results()

    engine = create_engine('sqlite:///%s' % db_path)
    inspector = inspect(engine)
    assert inspector.get_table_names() == ['t']
    assert inspector.get_pk_constraint('t')['constrained_columns'] == ['id']
    with engine.connect() as conn:
        assert conn.execute(text('SELECT COUNT(*) FROM t')).scalar() == 0
        assert conn.execute(
            text('PRAGMA foreign_key_check')).fetchall() == []
