"""
Whole-package data cleaning engine.

The engine takes a fully buffered data package (resource descriptors + rows)
and splits it into two parts:

* a **valid stream** - rows that obey their declared table schema, have no
  duplicate primary keys and satisfy every foreign key reference, and
* a **quarantine stream** - every row that failed validation, duplicated a
  primary key or references a parent row that is itself quarantined or does
  not exist.

Foreign key validation is run as a fixed-point computation: when a row is
quarantined, rows of other resources that reference it are quarantined as
well, and so on until the relationship graph is stable.  Rows that reference
each other in a healthy cycle are never removed - every member of the cycle
exists and survives, so every reference inside the cycle resolves.

The engine itself is transport agnostic: it works on plain descriptors and
lists of dict rows, which keeps it unit testable without the pipeline
machinery.  The ``clean_package`` processor wraps it for use inside a
datapackage-pipelines flow.
"""
from __future__ import absolute_import, division, print_function

import copy
import logging

from tableschema import Schema
from tableschema.exceptions import CastError


# Reasons reported on quarantined rows.  Kept as stable string constants so
# downstream consumers can branch on them.
REASON_SCHEMA = 'schema-violation'
REASON_DUPLICATE_PK = 'duplicate-primary-key'
REASON_DANGLING_FK = 'dangling-foreign-key'


class CleanReport(object):
    """Summary of a cleaning run, keyed by resource name."""

    def __init__(self):
        self.input_counts = {}
        self.valid_counts = {}
        self.quarantine_counts = {}
        # reason -> resource -> count
        self.reason_counts = {}

    def to_dict(self):
        return {
            'input_counts': dict(self.input_counts),
            'valid_counts': dict(self.valid_counts),
            'quarantine_counts': dict(self.quarantine_counts),
            'reason_counts': copy.deepcopy(self.reason_counts),
        }


class _Envelope(object):
    """A schema-valid row travelling through the cleaning phases."""

    __slots__ = ('raw', 'cast', 'index')

    def __init__(self, raw, cast, index):
        # raw:  the original row exactly as received
        # cast: the row with declared field types/constraints applied
        # index: zero-based position in the source resource
        self.raw = raw
        self.cast = cast
        self.index = index


def _primary_key(descriptor):
    """Return the normalized primary-key field list for a resource."""
    pk = (descriptor.get('schema') or {}).get('primaryKey')
    if pk is None:
        return []
    if isinstance(pk, str):
        return [pk]
    return list(pk)


def _foreign_keys(descriptor):
    """Yield normalized (local fields, reference resource, remote fields)."""
    fks = (descriptor.get('schema') or {}).get('foreignKeys', []) or []
    for fk in fks:
        fields = fk.get('fields', [])
        if isinstance(fields, str):
            fields = [fields]
        reference = fk.get('reference', {}) or {}
        ref_resource = reference.get('resource', '')
        ref_fields = reference.get('fields', [])
        if isinstance(ref_fields, str):
            ref_fields = [ref_fields]
        if not fields or not ref_fields or len(fields) != len(ref_fields):
            # Malformed foreign key declaration - nothing we can validate.
            continue
        yield list(fields), ref_resource, list(ref_fields)


def _is_missing(value):
    # tableschema treats empty strings (and None) as missing values.
    return value is None or value == ''


def validate_row(row, schema, pk_fields):
    """
    Validate and cast a single row against its declared schema.

    Returns ``(cast_row, errors)`` where ``errors`` is a list of human
    readable messages (empty when the row is valid).  A valid row has the
    declared types and field-level constraints applied.  Primary key fields
    must additionally be present, as a SQL primary key cannot be NULL.
    """
    cast_row = dict(row)
    errors = []

    for field in schema.fields:
        try:
            cast_row[field.name] = field.cast_value(row.get(field.name))
        except CastError as exc:
            messages = [str(err) for err in exc.errors] or [str(exc)]
            errors.extend(messages)

    if not errors:
        for name in pk_fields:
            field = schema.get_field(name)
            value = cast_row.get(name)
            if field is None:
                errors.append(
                    'Primary key field "%s" is not declared in the schema'
                    % name
                )
            elif _is_missing(value):
                errors.append(
                    'Primary key field "%s" is required but missing' % name
                )

    return cast_row, errors


def _key_tuple(row, fields):
    return tuple(row.get(name) for name in fields)


def _field_required(schema, field_name):
    field = schema.get_field(field_name)
    if field is None:
        return False
    return bool(field.constraints.get('required', False))


def clean_package_data(descriptors, rows_by_name, report=None):
    """
    Clean a buffered data package.

    :param descriptors: list of resource descriptors (dicts), in output order
    :param rows_by_name: mapping ``resource name -> list of raw rows``
    :param report: optional :class:`CleanReport` updated during the run
    :returns: ``(valid_rows, quarantine_rows)`` mappings keyed by resource
              name.  Quarantine rows are dicts containing the original raw
              row plus the source resource, the source row index and the
              reason (and detail) for quarantine.
    """
    if report is None:
        report = CleanReport()

    names = [d['name'] for d in descriptors]
    descriptor_by_name = {d['name']: d for d in descriptors}
    schemas = {
        name: Schema(descriptor.get('schema', {}) or {})
        for name, descriptor in descriptor_by_name.items()
    }
    pk_fields = {name: _primary_key(d) for name, d in descriptor_by_name.items()}

    # Phase 1 - per-row schema validation.  The original row is preserved
    # untouched in the quarantine stream; only surviving rows are cast.
    pending = {name: [] for name in names}
    quarantined = {name: [] for name in names}

    for descriptor in descriptors:
        name = descriptor['name']
        source = descriptor.get('path', name)
        schema = schemas[name]
        for index, raw_row in enumerate(rows_by_name.get(name, [])):
            report.input_counts[name] = report.input_counts.get(name, 0) + 1
            cast_row, errors = validate_row(raw_row, schema, pk_fields[name])
            if errors:
                quarantined[name].append(
                    _quarantine_record(
                        raw_row, name, source, index,
                        REASON_SCHEMA, '; '.join(errors)
                    )
                )
                _count(report, name, REASON_SCHEMA)
            else:
                pending[name].append(_Envelope(raw_row, cast_row, index))

    # Phase 2 - primary key uniqueness.  Every row sharing a duplicated key
    # is quarantined (none of the duplicates is allowed to win).
    for name in names:
        fields = pk_fields[name]
        if not fields:
            continue
        seen = set()
        duplicates = set()
        for envelope in pending[name]:
            key = _key_tuple(envelope.cast, fields)
            if key in seen:
                duplicates.add(key)
            seen.add(key)
        if not duplicates:
            continue
        kept = []
        source = _source(descriptor_by_name, name)
        for envelope in pending[name]:
            key = _key_tuple(envelope.cast, fields)
            if key in duplicates:
                quarantined[name].append(
                    _quarantine_record(
                        envelope.raw, name, source, envelope.index,
                        REASON_DUPLICATE_PK,
                        'Duplicate primary key %s=%r' % (fields, list(key))
                    )
                )
                _count(report, name, REASON_DUPLICATE_PK)
            else:
                kept.append(envelope)
        pending[name] = kept

    # Phase 3 - transitive foreign key closure to a fixed point.
    fk_quarantine, survivors = _resolve_foreign_keys(
        descriptors, pending, schemas, pk_fields, report
    )
    for name in names:
        quarantined[name].extend(fk_quarantine[name])

    valid = {name: [envelope.cast for envelope in survivors[name]]
             for name in names}

    for name in names:
        report.valid_counts[name] = len(valid[name])
        report.quarantine_counts[name] = len(quarantined[name])

    return valid, quarantined


def _source(descriptor_by_name, name):
    descriptor = descriptor_by_name.get(name)
    if descriptor is None:
        return name
    return descriptor.get('path', name)


def _quarantine_record(raw_row, resource, source, index, reason, detail):
    return {
        'original_row': copy.deepcopy(raw_row),
        'resource': resource,
        'source': source,
        'source_row_index': index,
        'reason': reason,
        'reason_detail': detail,
    }


def _count(report, resource, reason):
    by_reason = report.reason_counts.setdefault(reason, {})
    by_reason[resource] = by_reason.get(resource, 0) + 1


def _resolve_foreign_keys(descriptors, pending, schemas, pk_fields, report):
    """
    Repeatedly quarantine rows whose foreign keys do not resolve until no
    more rows can be removed.  Self references and circular reference
    groups survive because every referenced row is still present.
    """
    names = [d['name'] for d in descriptors]
    descriptor_by_name = {d['name']: d for d in descriptors}
    sources = {name: _source(descriptor_by_name, name) for name in names}

    # Foreign keys declared per resource, resolved up front.
    fks_by_name = {}
    for descriptor in descriptors:
        name = descriptor['name']
        resolved = []
        for fields, ref_resource, ref_fields in _foreign_keys(descriptor):
            target = name if ref_resource == '' else ref_resource
            if target not in descriptor_by_name:
                logging.warning(
                    'clean_package: resource %r declares a foreign key '
                    'against unknown resource %r - skipping',
                    name, target,
                )
                continue
            # Nullable per the schema: the local FK columns may be NULL only
            # when they carry no "required" constraint.
            nullable = {
                f: not _field_required(schemas[name], f) for f in fields
            }
            resolved.append((fields, target, ref_fields, nullable))
        fks_by_name[name] = resolved

    current = {name: list(envelopes) for name, envelopes in pending.items()}
    fk_quarantine = {name: [] for name in names}

    while True:
        # Exact tuple index of every surviving row for every (target
        # resource, target columns) combination referenced anywhere.  All
        # rows are buffered, so the index can be precise rather than an
        # approximation.
        target_tuples = _build_target_tuples(names, current, fks_by_name)

        changed = False
        survivors = {name: [] for name in names}

        for name in names:
            for envelope in current[name]:
                failure = _first_fk_failure(
                    envelope.cast, fks_by_name[name], target_tuples
                )
                if failure is None:
                    survivors[name].append(envelope)
                else:
                    fields, target, ref_fields, values, nullable = failure
                    fk_quarantine[name].append(
                        _quarantine_record(
                            envelope.raw, name, sources[name], envelope.index,
                            REASON_DANGLING_FK,
                            'Foreign key %s=%r references missing '
                            '%s.%s (nullable local fields: %s)'
                            % (fields, list(values), target, ref_fields,
                               [f for f in fields if nullable[f]])
                        )
                    )
                    _count(report, name, REASON_DANGLING_FK)
                    changed = True

        current = survivors
        if not changed:
            break

    return fk_quarantine, current


def _build_target_tuples(names, current, fks_by_name):
    """Map (target resource, target columns) -> set of surviving tuples."""
    wanted = {name: set() for name in names}
    for local_name in names:
        for _fields, target, ref_fields, _nullable in fks_by_name[local_name]:
            wanted[target].add(tuple(ref_fields))

    target_tuples = {}
    for name in names:
        for columns in wanted[name]:
            target_tuples[(name, columns)] = {
                _key_tuple(envelope.cast, list(columns))
                for envelope in current[name]
            }
    return target_tuples


def _first_fk_failure(row, fks, target_tuples):
    for fields, target, ref_fields, nullable in fks:
        values = _key_tuple(row, fields)
        all_missing = all(_is_missing(value) for value in values)
        if all_missing and all(nullable[field] for field in fields):
            # A nullable foreign key that is entirely NULL is allowed to
            # reference no parent row.
            continue
        if values in target_tuples.get((target, tuple(ref_fields)), set()):
            continue
        # A partially NULL composite key can never match a complete parent
        # key, so it is reported as a dangling reference.
        return fields, target, ref_fields, values, nullable
    return None
