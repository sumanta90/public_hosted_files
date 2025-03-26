import unittest

from frictionless import steps

from tests.test_transform.data.cdm_mapping_for_test_files import (
    mapping,
    required_output,
)

from ebflow.transform.generate_cdm_fields import generate_cdm_fields


class TestCreateReportContent(unittest.TestCase):

    def test_create_report_content(self):
        output, errors = generate_cdm_fields(mapping)
        resource = output[0]["resource"]
        pipeline = output[0]["pipeline"]
        cdm_field_order = output[0]["field_order"]

        pipeline.steps.append(steps.field_filter(names=cdm_field_order))
        resource.transform(pipeline)
        data = resource.read_rows()

        required_header = [cmap["cdm_field"] for cmap in mapping[0]["mapping"]]
        required_header.insert(required_header.index("split") + 1, "extra")

        actual_header = resource.header
        self.assertListEqual(actual_header, required_header)

        for field in required_header:
            actual_values = [row[field] for row in data]
            required_values = required_output[field]

            self.assertListEqual(actual_values, required_values)
