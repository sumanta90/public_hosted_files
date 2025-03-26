import unittest

from frictionless import Pipeline, steps
from frictionless.resources import TableResource

from ebflow.analytics.dqs.coa_dqs import COADQS


class TestCOADQS(unittest.TestCase):
    def setUp(self):
        resource = TableResource(
            path="tests/test_analytics/test_dqs/data/dqs_test_data.csv"
        )
        required_fields = ["glAccountNumber", "businessUnitCode"]
        resource.infer()
        resource.transform(
            Pipeline(
                steps=[
                    steps.table_normalize(),
                    steps.field_filter(names=required_fields),
                    steps.cell_replace(pattern="", replace=""),
                ]
            )
        )
        cdm_fields = []
        self.coa_dqs = COADQS(
            resource=resource, entity_type=None, cdm_fields=cdm_fields
        )

    def test_statistical(self):
        values_to_check = [
            {"recordCount": 6},
            {"fieldCount": 2},
            {"glAccountCodeCount": 3},
            {"businessUnitCounts": 6},
        ]
        result = self.coa_dqs.statistical()
        for idx, value in enumerate(values_to_check):
            self.assertDictEqual(result[idx], value)

    def test_business_rule(self):
        values_to_check = []
        result = self.coa_dqs.business_rule()
        for idx, value in enumerate(values_to_check):
            self.assertDictEqual(result[idx], value)
