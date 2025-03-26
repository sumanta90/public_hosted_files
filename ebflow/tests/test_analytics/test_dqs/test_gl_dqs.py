import unittest

from frictionless import Pipeline, steps
from frictionless.resources import TableResource

from ebflow.analytics.dqs.gl_dqs import GLDQS


class TestGLDQS(unittest.TestCase):
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
            "journalId",
            "journalIdNonNumeric",
            "enteredBy",
            "enteredDateTime",
            "approvedBy",
            "approvedDateTime",
            "lastModifiedBy",
            "reversalIndicator",
            "reversalJournalId",
            "journalEntryType",
            "sourceId",
        ]
        resource.infer()
        resource.schema.set_field_type("amount", "number")
        resource.schema.set_field_type("localAmount", "number")
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
        self.gl_dqs = GLDQS(resource=resource, entity_type=None, cdm_fields=cdm_fields)

    def test_statistical(self):
        values_to_check = [
            {"recordCount": 6},
            {"fieldCount": 18},
            {"amountSum": 900.0},
            {"localAmountSum": 100.0},
            {"debitCredits": 100.0},
            {"glAccountCodeCount": 3},
            {"businessUnitCounts": 6},
        ]
        result = self.gl_dqs.statistical()
        for idx, value in enumerate(values_to_check):
            self.assertDictEqual(result[idx], value)

    def test_business_rule(self):
        values_to_check = [
            {"netting": "fail"},
            {
                "controlChecks": {
                    "overall": 80,
                    "fields": [
                        {"enteredBy": True},
                        {"enteredDateTime": False},
                        {"approvedBy": True},
                        {"approvedDateTime": True},
                        {"lastModifiedBy": True},
                    ],
                }
            },
            {
                "reversals": {
                    "overall": 100,
                    "fields": [
                        {"reversalIndicator": True},
                        {"reversalJournalId": True},
                    ],
                }
            },
            {
                "manualJournals": {
                    "overall": 100,
                    "fields": [{"journalEntryType": True}, {"sourceId": True}],
                }
            },
            {
                "missingSequentialItems": {
                    "overall": 50,
                    "fields": [{"journalId": False, "show_warning": False}],
                }
            },
        ]
        result = self.gl_dqs.business_rule()
        for idx, value in enumerate(values_to_check):
            self.assertDictEqual(result[idx], value)
