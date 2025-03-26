import unittest

from frictionless import Pipeline, steps
from frictionless.resources import TableResource

from ebflow.analytics.dqs.tb_dqs import TBDQS


class TestTBDQS(unittest.TestCase):
    def setUp(self):
        resource = TableResource(
            path="tests/test_analytics/test_dqs/data/dqs_test_data.csv"
        )
        required_fields = [
            "glAccountNumber",
            "businessUnitCode",
            "amount",
            "localAmount",
            "amountCreditDebitIndicator",
            "amountBeginning",
            "amountEnding",
        ]
        resource.infer()
        resource.schema.set_field_type("amount", "number")
        resource.schema.set_field_type("localAmount", "number")
        resource.schema.set_field_type("amountBeginning", "number")
        resource.schema.set_field_type("amountEnding", "number")
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
        self.tb_dqs = TBDQS(resource=resource, entity_type=None, cdm_fields=cdm_fields)

    def test_statistical(self):
        values_to_check = [
            {"recordCount": 6},
            {"fieldCount": 7},
            {"amountBeginningSum": 442.848},
            {"amountEndingSum": 0.0},
            {"glAccountCodeCount": 3},
            {"businessUnitCounts": 6},
        ]
        result = self.tb_dqs.statistical()
        for idx, value in enumerate(values_to_check):
            self.assertDictEqual(result[idx], value)

    def test_business_rule(self):
        values_to_check = [{"netting": "pass"}]
        result = self.tb_dqs.business_rule()
        for idx, value in enumerate(values_to_check):
            self.assertDictEqual(result[idx], value)
