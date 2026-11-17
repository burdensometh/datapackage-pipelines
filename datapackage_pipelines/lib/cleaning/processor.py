"""
``clean_package`` dataflows processor.

The processor buffers every (selected) resource of the package, runs the
whole-package cleaning engine and replaces each selected resource with:

* the original resource, now containing only rows that passed schema,
  primary-key and foreign-key validation, and
* a sibling quarantine resource holding every rejected row together with
  the original row payload, its source descriptor/path, its source row index
  and the reason for the rejection.

Resources not selected by the ``resources`` parameter pass through
untouched.
"""
from __future__ import absolute_import, division, print_function

import copy

from dataflows import DataStreamProcessor
from dataflows.helpers.resource_matcher import ResourceMatcher

from .engine import CleanReport, clean_package_data


QUARANTINE_SCHEMA = {
    'fields': [
        {'name': 'original_row', 'type': 'object'},
        {'name': 'resource', 'type': 'string'},
        {'name': 'source', 'type': 'string'},
        {'name': 'source_row_index', 'type': 'integer'},
        {'name': 'reason', 'type': 'string'},
        {'name': 'reason_detail', 'type': 'string'},
    ]
}


def quarantine_name(resource_name, suffix):
    return resource_name + suffix


class clean_package(DataStreamProcessor):

    def __init__(self, resources=None, quarantine_suffix='_quarantine'):
        super(clean_package, self).__init__()
        self._resources_param = resources
        self._quarantine_suffix = quarantine_suffix
        self._matcher = None
        self._selected = []
        self._report = CleanReport()

    def process_datapackage(self, dp):
        self._matcher = ResourceMatcher(self._resources_param, dp)
        descriptor = dp.descriptor
        existing_names = {r['name'] for r in descriptor['resources']}

        new_resources = []
        for resource in descriptor['resources']:
            name = resource['name']
            new_resources.append(resource)
            if not self._matcher.match(name):
                continue
            q_name = quarantine_name(name, self._quarantine_suffix)
            if q_name in existing_names:
                raise ValueError(
                    'clean_package: quarantine resource name %r already '
                    'exists in the data package' % q_name
                )
            existing_names.add(q_name)
            self._selected.append(name)
            new_resources.append(self._build_quarantine_descriptor(resource,
                                                                   q_name))

        descriptor['resources'] = new_resources
        dp.commit()
        return dp

    def process_resources(self, resources):
        # Buffer all input resources: cleaning is a whole-package operation
        # because foreign keys may point at any other resource.
        buffered = {}
        passthrough = {}
        for res_wrapper in resources:
            name = res_wrapper.res.name
            if name in self._selected:
                buffered[name] = list(res_wrapper)
            else:
                passthrough[name] = res_wrapper

        descriptors = {
            r['name']: r for r in self.datapackage.descriptor['resources']
        }
        selected_descriptors = [descriptors[name] for name in self._selected]

        valid, quarantined = clean_package_data(
            selected_descriptors, buffered, report=self._report
        )

        self.stats['clean_package'] = self._report.to_dict()

        # Emit resources in the exact order declared on the output package.
        for resource in self.datapackage.descriptor['resources']:
            name = resource['name']
            if name in passthrough:
                yield passthrough[name]
            elif name in valid:
                yield iter(valid[name])
            else:
                # Quarantine sibling of a selected resource.
                base_name = name[:-len(self._quarantine_suffix)]
                yield iter(quarantined.get(base_name, []))

    def _build_quarantine_descriptor(self, resource_descriptor, q_name):
        descriptor = copy.deepcopy(QUARANTINE_SCHEMA)
        q_descriptor = {
            'name': q_name,
            'path': q_name + '.csv',
            'schema': descriptor,
        }
        if resource_descriptor.get('dpp:streaming'):
            q_descriptor['dpp:streaming'] = True
        return q_descriptor
