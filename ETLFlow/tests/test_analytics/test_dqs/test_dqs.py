import unittest

from frictionless import Pipeline, steps
from frictionless.resources import TableResource

from ebflow.analytics.dqs.dqs import DQS


class DQSBase(DQS):

    def business_rule(self):
        return None

    def statistical(self):
        return None


class TestDQS(unittest.TestCase):
    def setUp(self):
        resource = TableResource(
            path="tests/test_analytics/test_dqs/data/dqs_test_data.csv"
        )
        resource.infer()
        required_fields = [
            "glAccountNumber",
            "amount",
            "amountCreditDebitIndicator",
            "amountBeginning",
            "enteredDateTime",
        ]
        resource.transform(
            Pipeline(
                steps=[
                    steps.table_normalize(),
                    steps.field_filter(names=required_fields),
                    steps.cell_replace(pattern="", replace=""),
                ]
            )
        )
        cdm_fields = [
            {"cdm_field": "glAccountNumber", "data_type": "string"},
            {"cdm_field": "amount", "data_type": "number"},
            {"cdm_field": "amountCreditDebitIndicator", "data_type": "string"},
            {"cdm_field": "amountBeginning", "data_type": "number"},
            {"cdm_field": "enteredDateTime", "data_type": "string"},
            {"cdm_field": "enteredDateTime", "data_type": "number"},
        ]
        self.dqs = DQSBase(resource=resource, entity_type=None, cdm_fields=cdm_fields)

    def test_get_percentage(self):
        # Test total = 0
        result = self.dqs.get_percent(10, 0)
        self.assertEqual(result, 0)

        # Test total != 0
        result = self.dqs.get_percent(11.4, 100)
        self.assertEqual(result, 11)

    def test_get_unique_value_count(self):
        field_name = ["glAccountNumber"]
        value = self.dqs.get_unique_value_count(field_name)
        self.assertEqual(value, 3)

    def test_get_field_sum(self):
        field_name = "amountBeginning"
        result = self.dqs.get_field_sum(field_name)
        self.assertEqual(result, 442.8480025)

    def test_all_rows_blank(self):
        field_names = ["glAccountNumber", "enteredDateTime"]
        percentage, result_list = self.dqs.all_rows_blank(field_names)

        self.assertEqual(percentage, 50)
        self.assertTrue(result_list[0]["glAccountNumber"])
        self.assertFalse(result_list[1]["enteredDateTime"])

    def test_profile(self):
        values_to_check = [
            {
                "fieldsFilled": {
                    "percent": 80,
                    "total": 5,
                    "filled": 4,
                    "fields": [
                        "glAccountNumber",
                        "amount",
                        "amountCreditDebitIndicator",
                        "amountBeginning",
                    ],
                }
            },
            {
                "fieldsEmpty": {
                    "percent": 20,
                    "total": 5,
                    "empty": 1,
                    "fields": ["enteredDateTime"],
                }
            },
            {
                "textFieldsEmpty": {
                    "percent": 67,
                    "total": 3,
                    "empty": 2,
                    "fields": ["amountCreditDebitIndicator", "enteredDateTime"],
                }
            },
            {
                "numericFieldsEmpty": {
                    "percent": 67,
                    "total": 3,
                    "empty": 2,
                    "fields": ["amount", "enteredDateTime"],
                }
            },
            {
                "missingRecords": {
                    "percent": 50,
                    "total": 4,
                    "empty": 2,
                    "fields": [{"amount": 83}, {"amountCreditDebitIndicator": 83}],
                }
            },
        ]
        result = self.dqs.profile()
        for idx, value in enumerate(values_to_check):
            self.assertDictEqual(result[idx], value)
