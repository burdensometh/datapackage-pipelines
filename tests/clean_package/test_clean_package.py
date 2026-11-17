"""
Tests for the whole-package cleaning engine and its dataflows processor.
"""
import os

import pytest
from sqlalchemy import create_engine, inspect, text

from dataflows import Flow, load

from datapackage_pipelines.lib.cleaning.engine import (
    CleanReport,
    REASON_DANGLING_FK,
    REASON_DUPLICATE_PK,
    REASON_SCHEMA,
    clean_package_data,
)
from datapackage_pipelines.lib.cleaning.processor import clean_package


def _schema(fields, primary_key=None, foreign_keys=None):
    schema = {'fields': fields}
    if primary_key is not None:
        schema['primaryKey'] = primary_key
    if foreign_keys is not None:
        schema['foreignKeys'] = foreign_keys
    return schema


def _descriptor(name, schema, path=None):
    return {
        'name': name,
        'path': path or name + '.csv',
        'schema': schema,
    }


def _reasons(quarantine):
    return [record['reason'] for record in quarantine]


def test_schema_violations_are_isolated_with_original_row():
    descriptors = [_descriptor('things', _schema(
        [
            {'name': 'id', 'type': 'integer',
             'constraints': {'required': True}},
            {'name': 'kind', 'type': 'string',
             'constraints': {'enum': ['a', 'b']}},
        ],
        primary_key=['id'],
    ))]
    rows = {
        'things': [
            {'id': '1', 'kind': 'a'},                       # valid, cast
            {'id': 'not-int', 'kind': 'a'},                # bad type
            {'id': None, 'kind': 'a'},                     # null primary key
            {'id': '2', 'kind': 'z'},                      # enum violation
        ],
    }

    valid, quarantined = clean_package_data(descriptors, rows)

    assert valid['things'] == [{'id': 1, 'kind': 'a'}]
    assert len(quarantined['things']) == 3
    assert _reasons(quarantined['things']) == [REASON_SCHEMA] * 3
    # The original payload is preserved verbatim, with source and position.
    bad_type = quarantined['things'][0]
    assert bad_type['original_row'] == {'id': 'not-int', 'kind': 'a'}
    assert bad_type['resource'] == 'things'
    assert bad_type['source'] == 'things.csv'
    assert bad_type['source_row_index'] == 1
    assert 'integer' in bad_type['reason_detail']


def test_every_duplicate_primary_key_row_is_isolated():
    descriptors = [_descriptor('t', _schema(
        [{'name': 'k', 'type': 'integer'}, {'name': 'v', 'type': 'string'}],
        primary_key=['k'],
    ))]
    rows = {'t': [
        {'k': '1', 'v': 'a'},
        {'k': '2', 'v': 'b'},
        {'k': '1', 'v': 'c'},   # duplicate of the first row
        {'k': '2', 'v': 'd'},   # duplicate of the second row
        {'k': '3', 'v': 'e'},
    ]}

    valid, quarantined = clean_package_data(descriptors, rows)

    assert [row['k'] for row in valid['t']] == [3]
    assert _reasons(quarantined['t']) == [REASON_DUPLICATE_PK] * 4
    # All duplicated rows are reported, including the first occurrence.
    assert [r['original_row']['v'] for r in quarantined['t']] == \
        ['a', 'b', 'c', 'd']


def test_composite_primary_key_duplicates():
    descriptors = [_descriptor('t', _schema(
        [{'name': 'a', 'type': 'integer'}, {'name': 'b', 'type': 'integer'}],
        primary_key=['a', 'b'],
    ))]
    rows = {'t': [
        {'a': 1, 'b': 1},
        {'a': 1, 'b': 2},
        {'a': 1, 'b': 1},  # full-tuple duplicate
    ]}

    valid, quarantined = clean_package_data(descriptors, rows)

    assert valid['t'] == [{'a': 1, 'b': 2}]
    assert len(quarantined['t']) == 2
    assert all(r['reason'] == REASON_DUPLICATE_PK
               for r in quarantined['t'])


def test_dangling_foreign_key_to_missing_parent():
    descriptors = [
        _descriptor('parents', _schema(
            [{'name': 'id', 'type': 'integer',
              'constraints': {'required': True}}],
            primary_key=['id'],
        )),
        _descriptor('children', _schema(
            [
                {'name': 'id', 'type': 'integer',
                 'constraints': {'required': True}},
                {'name': 'parent_id', 'type': 'integer',
                 'constraints': {'required': True}},
            ],
            primary_key=['id'],
            foreign_keys=[{
                'fields': ['parent_id'],
                'reference': {'resource': 'parents', 'fields': ['id']},
            }],
        )),
    ]
    rows = {
        'parents': [{'id': 1}],
        'children': [
            {'id': 10, 'parent_id': 1},    # resolves
            {'id': 11, 'parent_id': 2},    # parent does not exist
        ],
    }

    valid, quarantined = clean_package_data(descriptors, rows)

    assert [r['id'] for r in valid['children']] == [10]
    q = quarantined['children']
    assert len(q) == 1
    assert q[0]['reason'] == REASON_DANGLING_FK
    assert q[0]['original_row']['id'] == 11


def test_foreign_key_closure_runs_to_a_fixed_point():
    # A -> B -> C; C's target row is quarantined for a schema violation,
    # so B must be quarantined in the first iteration and A in the next.
    descriptors = [
        _descriptor('c', _schema(
            [{'name': 'id', 'type': 'integer',
              'constraints': {'required': True}}],
            primary_key=['id'],
        )),
        _descriptor('b', _schema(
            [
                {'name': 'id', 'type': 'integer',
                 'constraints': {'required': True}},
                {'name': 'c_id', 'type': 'integer',
                 'constraints': {'required': True}},
            ],
            primary_key=['id'],
            foreign_keys=[{
                'fields': ['c_id'],
                'reference': {'resource': 'c', 'fields': ['id']},
            }],
        )),
        _descriptor('a', _schema(
            [
                {'name': 'id', 'type': 'integer',
                 'constraints': {'required': True}},
                {'name': 'b_id', 'type': 'integer',
                 'constraints': {'required': True}},
            ],
            primary_key=['id'],
            foreign_keys=[{
                'fields': ['b_id'],
                'reference': {'resource': 'b', 'fields': ['id']},
            }],
        )),
    ]
    rows = {
        'c': [{'id': 'bad'}],                       # schema violation
        'b': [{'id': 1, 'c_id': 1}],                # loses its parent
        'a': [{'id': 1, 'b_id': 1}],                # loses b transitively
    }

    valid, quarantined = clean_package_data(descriptors, rows)

    assert valid == {'c': [], 'b': [], 'a': []}
    assert quarantined['c'][0]['reason'] == REASON_SCHEMA
    assert quarantined['b'][0]['reason'] == REASON_DANGLING_FK
    assert quarantined['a'][0]['reason'] == REASON_DANGLING_FK


def test_healthy_circular_reference_groups_are_kept():
    # Self reference and a three-node cycle must survive every iteration.
    descriptors = [
        _descriptor('nodes', _schema(
            [
                {'name': 'id', 'type': 'integer',
                 'constraints': {'required': True}},
                {'name': 'next_id', 'type': 'integer'},
            ],
            primary_key=['id'],
            foreign_keys=[{
                'fields': ['next_id'],
                'reference': {'resource': '', 'fields': ['id']},
            }],
        )),
    ]
    rows = {'nodes': [
        {'id': 1, 'next_id': 2},
        {'id': 2, 'next_id': 3},
        {'id': 3, 'next_id': 1},   # closes the cycle back to row 1
        {'id': 4, 'next_id': 4},   # points at itself
    ]}

    valid, quarantined = clean_package_data(descriptors, rows)

    assert [r['id'] for r in valid['nodes']] == [1, 2, 3, 4]
    assert quarantined['nodes'] == []


def test_nullable_foreign_key_semantics():
    descriptors = [
        _descriptor('parents', _schema(
            [{'name': 'id', 'type': 'integer',
              'constraints': {'required': True}}],
            primary_key=['id'],
        )),
        _descriptor('children', _schema(
            [
                {'name': 'id', 'type': 'integer',
                 'constraints': {'required': True}},
                # nullable: no "required" constraint
                {'name': 'parent_id', 'type': 'integer'},
            ],
            primary_key=['id'],
            foreign_keys=[{
                'fields': ['parent_id'],
                'reference': {'resource': 'parents', 'fields': ['id']},
            }],
        )),
    ]
    rows = {
        'parents': [{'id': 1}],
        'children': [
            {'id': 10, 'parent_id': None},   # allowed: nullable, fully null
            {'id': 11, 'parent_id': ''},     # empty string counts as null
            {'id': 12, 'parent_id': 1},      # resolves
        ],
    }

    valid, quarantined = clean_package_data(descriptors, rows)

    assert [r['id'] for r in valid['children']] == [10, 11, 12]
    assert quarantined['children'] == []


def test_partially_null_composite_foreign_key_is_dangling():
    descriptors = [
        _descriptor('parents', _schema(
            [
                {'name': 'a', 'type': 'integer'},
                {'name': 'b', 'type': 'integer'},
            ],
            primary_key=['a', 'b'],
        )),
        _descriptor('children', _schema(
            [
                {'name': 'id', 'type': 'integer',
                 'constraints': {'required': True}},
                {'name': 'pa', 'type': 'integer'},
                {'name': 'pb', 'type': 'integer'},
            ],
            primary_key=['id'],
            foreign_keys=[{
                'fields': ['pa', 'pb'],
                'reference': {'resource': 'parents',
                              'fields': ['a', 'b']},
            }],
        )),
    ]
    rows = {
        'parents': [{'a': 1, 'b': 1}],
        'children': [
            {'id': 1, 'pa': None, 'pb': None},  # fully null: allowed
            {'id': 2, 'pa': 1, 'pb': None},     # partial: cannot resolve
            {'id': 3, 'pa': 1, 'pb': 1},        # resolves
        ],
    }

    valid, quarantined = clean_package_data(descriptors, rows)

    assert [r['id'] for r in valid['children']] == [1, 3]
    assert len(quarantined['children']) == 1
    assert quarantined['children'][0]['original_row']['id'] == 2
    assert quarantined['children'][0]['reason'] == REASON_DANGLING_FK


def test_foreign_key_targeting_non_primary_key_columns():
    descriptors = [
        _descriptor('parents', _schema([
            {'name': 'id', 'type': 'integer',
             'constraints': {'required': True}},
            {'name': 'code', 'type': 'string'},
        ], primary_key=['id'])),
        _descriptor('children', _schema(
            [
                {'name': 'id', 'type': 'integer',
                 'constraints': {'required': True}},
                {'name': 'parent_code', 'type': 'string'},
            ],
            primary_key=['id'],
            foreign_keys=[{
                'fields': ['parent_code'],
                'reference': {'resource': 'parents', 'fields': ['code']},
            }],
        )),
    ]
    rows = {
        'parents': [{'id': 1, 'code': 'X'}, {'id': 2, 'code': 'Y'}],
        'children': [
            {'id': 1, 'parent_code': 'X'},   # exists
            {'id': 2, 'parent_code': 'Z'},   # no parent with code Z
        ],
    }

    valid, quarantined = clean_package_data(descriptors, rows)

    assert [r['id'] for r in valid['children']] == [1]
    assert quarantined['children'][0]['reason'] == REASON_DANGLING_FK


def test_report_counts_reconcile_and_include_reasons():
    descriptors = [_descriptor('t', _schema(
        [{'name': 'id', 'type': 'integer',
          'constraints': {'required': True}}],
        primary_key=['id'],
    ))]
    rows = {'t': [
        {'id': 1}, {'id': 1}, {'id': 'bad'},
    ]}

    report = CleanReport()
    valid, quarantined = clean_package_data(descriptors, rows, report)
    summary = report.to_dict()

    assert summary['input_counts'] == {'t': 3}
    assert summary['valid_counts'] == {'t': len(valid['t'])}
    assert summary['quarantine_counts'] == {'t': len(quarantined['t'])}
    assert summary['input_counts']['t'] == (
        summary['valid_counts']['t'] + summary['quarantine_counts']['t'])
    assert summary['reason_counts'][REASON_DUPLICATE_PK] == {'t': 2}
    assert summary['reason_counts'][REASON_SCHEMA] == {'t': 1}


def test_unknown_foreign_resource_is_skipped(caplog):
    descriptors = [_descriptor('t', _schema(
        [
            {'name': 'id', 'type': 'integer',
             'constraints': {'required': True}},
            {'name': 'ghost_id', 'type': 'integer'},
        ],
        primary_key=['id'],
        foreign_keys=[{
            'fields': ['ghost_id'],
            'reference': {'resource': 'ghost', 'fields': ['id']},
        }],
    ))]
    rows = {'t': [{'id': 1, 'ghost_id': 42}]}

    with caplog.at_level('WARNING'):
        valid, quarantined = clean_package_data(descriptors, rows)

    assert valid['t'] == [{'id': 1, 'ghost_id': 42}]
    assert quarantined['t'] == []
    assert any('ghost' in record.message for record in caplog.records)


# --------------------------------------------------------------------------
# Processor (dataflows) level
# --------------------------------------------------------------------------

def _flow(descriptor, raw_streams, **kwargs):
    return Flow(
        load((descriptor, iter([iter(s) for s in raw_streams]))),
        clean_package(**kwargs),
    )


def test_processor_emits_clean_and_quarantine_streams():
    descriptor = {
        'name': 'pkg',
        'resources': [_descriptor('t', _schema(
            [{'name': 'id', 'type': 'integer',
              'constraints': {'required': True}}],
            primary_key=['id'],
        ))],
    }
    raw = [[{'id': '1'}, {'id': 'bad'}]]

    results, dp, _ = _flow(descriptor, raw).results()
    names = [r['name'] for r in dp.descriptor['resources']]
    assert names == ['t', 't_quarantine']

    clean_rows, quarantine_rows = results
    assert clean_rows == [{'id': 1}]
    assert quarantine_rows[0]['reason'] == REASON_SCHEMA
    assert quarantine_rows[0]['original_row'] == {'id': 'bad'}


def test_processor_resource_selection_and_passthrough():
    descriptor = {
        'name': 'pkg',
        'resources': [
            _descriptor('cleaned', _schema(
                [{'name': 'id', 'type': 'integer',
                  'constraints': {'required': True}}],
                primary_key=['id'],
            )),
            _descriptor('untouched', _schema(
                [{'name': 'id', 'type': 'string'}],
            )),
        ],
    }
    raw = [[{'id': 'bad'}], [{'id': 'whatever'}]]

    results, dp, _ = _flow(
        descriptor, raw, resources=['cleaned']).results()

    names = [r['name'] for r in dp.descriptor['resources']]
    assert names == ['cleaned', 'cleaned_quarantine', 'untouched']
    cleaned, cleaned_q, untouched = results
    assert cleaned == []
    assert cleaned_q[0]['reason'] == REASON_SCHEMA
    # The unselected resource passes through with its original (bad) row.
    assert untouched == [{'id': 'whatever'}]


def test_processor_rejects_colliding_quarantine_name():
    descriptor = {
        'name': 'pkg',
        'resources': [
            _descriptor('t', _schema(
                [{'name': 'id', 'type': 'integer'}])),
            _descriptor('t_quarantine', _schema(
                [{'name': 'id', 'type': 'integer'}])),
        ],
    }
    raw = [[{'id': 1}], [{'id': 2}]]

    with pytest.raises(Exception):
        _flow(descriptor, raw).results()


def test_clean_rows_enter_sql_with_primary_and_foreign_keys(tmp_path):
    db_path = os.path.join(str(tmp_path), 'clean.sqlite')
    engine = create_engine('sqlite:///%s' % db_path)

    descriptor = {
        'name': 'pkg',
        'resources': [
            _descriptor('departments', _schema(
                [
                    {'name': 'id', 'type': 'integer',
                     'constraints': {'required': True}},
                    {'name': 'manager_id', 'type': 'integer'},
                ],
                primary_key=['id'],
                foreign_keys=[{
                    'fields': ['manager_id'],
                    'reference': {'resource': '', 'fields': ['id']},
                }],
            )),
            _descriptor('employees', _schema(
                [
                    {'name': 'id', 'type': 'integer',
                     'constraints': {'required': True}},
                    {'name': 'dept_id', 'type': 'integer',
                     'constraints': {'required': True}},
                    {'name': 'manager_id', 'type': 'integer'},
                ],
                primary_key=['id'],
                foreign_keys=[
                    {
                        'fields': ['dept_id'],
                        'reference': {'resource': 'departments',
                                      'fields': ['id']},
                    },
                    {
                        'fields': ['manager_id'],
                        'reference': {'resource': '', 'fields': ['id']},
                    },
                ],
            )),
        ],
    }
    raw = [
        [
            {'id': '1', 'manager_id': '2'},     # valid cycle
            {'id': '2', 'manager_id': '1'},
            {'id': '9', 'manager_id': None},
            {'id': '9', 'manager_id': None},   # duplicate PK
        ],
        [
            {'id': '10', 'dept_id': '1', 'manager_id': None},
            {'id': '11', 'dept_id': '1', 'manager_id': '12'},
            {'id': '12', 'dept_id': '1', 'manager_id': '11'},
            {'id': '13', 'dept_id': '9', 'manager_id': None},  # orphaned
        ],
    ]

    results, dp, _ = _flow(descriptor, raw).results()
    by_name = dict(zip(
        [r['name'] for r in dp.descriptor['resources']], results))
    clean_depts = by_name['departments']
    clean_emps = by_name['employees']

    with engine.begin() as conn:
        conn.execute(text('PRAGMA foreign_keys=OFF'))
        conn.execute(text(
            'CREATE TABLE departments ('
            'id INTEGER NOT NULL PRIMARY KEY, manager_id INTEGER, '
            'FOREIGN KEY(manager_id) REFERENCES departments(id))'))
        conn.execute(text(
            'CREATE TABLE employees ('
            'id INTEGER NOT NULL PRIMARY KEY, '
            'dept_id INTEGER NOT NULL, manager_id INTEGER, '
            'FOREIGN KEY(dept_id) REFERENCES departments(id), '
            'FOREIGN KEY(manager_id) REFERENCES employees(id))'))
        for row in clean_depts:
            conn.execute(text(
                'INSERT INTO departments (id, manager_id) '
                'VALUES (:id, :manager_id)'), row)
        for row in clean_emps:
            conn.execute(text(
                'INSERT INTO employees (id, dept_id, manager_id) '
                'VALUES (:id, :dept_id, :manager_id)'), row)
        violations = conn.execute(
            text('PRAGMA foreign_key_check')).fetchall()
        assert violations == []

    insp = inspect(engine)
    assert insp.get_pk_constraint('employees')['constrained_columns'] == ['id']
    assert len(insp.get_foreign_keys('employees')) == 2

    # Quarantine stream explains every rejected row.
    q_emp = by_name['employees_quarantine']
    assert len(q_emp) == 1
    assert q_emp[0]['reason'] == REASON_DANGLING_FK
    assert q_emp[0]['original_row']['id'] == '13'
