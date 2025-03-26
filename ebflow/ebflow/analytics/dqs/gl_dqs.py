import math
from typing import Any

from frictionless import Pipeline, steps

from ebflow.analytics.dqs.dqs import DQS
from ebflow.utils.custom_steps import custom_sum, custom_aggregate, jid_details


class GLDQS(DQS):
    def statistical(self):
        # transform resource for required data
        resource = self.resource.to_copy()
        resource.transform(
            Pipeline(
                steps=[
                    steps.field_add(
                        name="net_amount",
                        descriptor={"type": "number"},
                        formula='amount * (-1 if amountCreditDebitIndicator == "C" else 1) if amount else 0',
                    ),
                    custom_aggregate(
                        aggregation={
                            "num_records": len,
                            "total_amount_sum": ("amount", custom_sum),
                            "local_amount_sum": ("localAmount", custom_sum),
                            "sum_net_amount": ("net_amount", custom_sum),
                        }
                    ),
                ]
            )
        )

        data = resource.read_rows()[0]

        # gl-detail-statistical score -total record
        gl_detail_total_record = data["num_records"]
        total_fields = len(self.resource.header)

        # gl-detail-statistical score -total amount sum
        gl_detail_total_amount_sum = data["total_amount_sum"]

        # gl-detail-statistical score - sum of local amount
        gl_detail_local_amount_sum = data["local_amount_sum"]

        # gl-detail-statistical score - Debits/Credits
        sum_net_amount = data["sum_net_amount"]

        # gl-detail-statistical score - GL Account Code Count
        gl_detail_account_code_count = self.get_unique_value_count(["glAccountNumber"])

        # gl-detail-statistical score - Number of Business Units
        gl_detail_business_unit_code_count = self.get_unique_value_count(
            ["businessUnitCode"]
        )

        return [
            {"recordCount": float(round(gl_detail_total_record, 5))},
            {"fieldCount": float(round(total_fields, 5))},
            {"amountSum": float(round(gl_detail_total_amount_sum, 5))},
            {"localAmountSum": float(round(gl_detail_local_amount_sum, 5))},
            {"debitCredits": float(round(sum_net_amount, 5))},
            {"glAccountCodeCount": float(round(gl_detail_account_code_count, 5))},
            {"businessUnitCounts": float(round(gl_detail_business_unit_code_count, 5))},
        ]

    def business_rule(self):
        resource = self.resource.to_copy()
        all_fields = self.resource.header

        aggregation_list: [str, Any] = {
            "num_records": len,
            "total_amount_sum": ("amount", custom_sum),
            "jid_data": ("journalId", jid_details),
        }
        aggregation_list.update(
            {field_name: (field_name, any) for field_name in all_fields}
        )

        resource.transform(
            Pipeline(
                steps=[
                    steps.field_add(
                        name="net_amount",
                        descriptor={"type": "number"},
                        formula='amount * (-1 if amountCreditDebitIndicator == "C" else 1) if amount else 0',
                    ),
                    custom_aggregate(aggregation=aggregation_list),
                ]
            )
        )

        data = resource.read_rows()[0]

        total_rows = data["num_records"]

        # GL - Detail Business Rule ----------------- Netting
        sum_net_amount = data["total_amount_sum"]

        # GL - Detail Business Rule ----------------- Controls checks:
        controls_columns = [
            "enteredBy",
            "enteredDateTime",
            "approvedBy",
            "approvedDateTime",
            "lastModifiedBy",
        ]
        controls_columns_total = len(controls_columns)
        controls_new_list = [{field: data[field]} for field in controls_columns]
        controls_columns_present = len(
            [field for field in controls_columns if data[field]]
        )

        # GL - Detail Business Rule ----------------- Reversals:
        reversal_columns = ["reversalIndicator", "reversalJournalId"]
        reversal_columns_total = len(reversal_columns)
        reversals_new_list = [{field: data[field]} for field in reversal_columns]
        reversal_columns_present = len(
            [field for field in reversal_columns if data[field]]
        )

        # GL - Detail Business Rule - ---------------- Manual  Journals:
        journals_columns = ["journalEntryType", "sourceId"]
        journals_columns_total = len(journals_columns)
        journals_new_list = [{field: data[field]} for field in journals_columns]
        journals_columns_present = len(
            [field for field in journals_columns if data[field]]
        )

        non_numeric_journal_id, missing_seq_count, null_seq_count = data["jid_data"]

        control_check_percentage = self.get_percent(
            controls_columns_present, controls_columns_total
        )
        reversal_percentage = self.get_percent(
            reversal_columns_present, reversal_columns_total
        )
        journals_percentage = self.get_percent(
            journals_columns_present, journals_columns_total
        )

        # Netting
        gl_detail_netting = "pass" if -0.001 <= sum_net_amount <= 0.001 else "fail"

        # Missing Sequence:
        if non_numeric_journal_id:
            seq_detail = {
                "overall": 0,
                "fields": [
                    {
                        "journalId": False,
                        "show_warning": True,
                        "warning": "JournalId does not appear to be a numeric sequence",
                    }
                ],
            }
        elif null_seq_count == total_rows:
            seq_detail = {
                "overall": 0,
                "fields": [
                    {
                        "journalId": False,
                        "show_warning": True,
                        "warning": "Could not locate missing sequences, all values are blank.",
                    }
                ],
            }
        else:
            # considering the 1st row to always be in sequence or the percentage will never be 100.
            # missing_seq_count only include cases where the current row is numeric and prev row is not in sequence
            # null_seq_count only includes cases where current row is None
            seq_count = total_rows - (missing_seq_count + null_seq_count)
            seq_percentage = (seq_count / total_rows * 100) if total_rows else 0
            seq_detail = {
                "overall": math.floor(seq_percentage),
                "fields": [
                    {
                        "journalId": seq_percentage == 100,
                        "show_warning": False,
                    }
                ],
            }
        return [
            {"netting": gl_detail_netting},
            {
                "controlChecks": {
                    "overall": float(control_check_percentage),
                    "fields": list(controls_new_list),
                }
            },
            {
                "reversals": {
                    "overall": float(reversal_percentage),
                    "fields": list(reversals_new_list),
                }
            },
            {
                "manualJournals": {
                    "overall": float(journals_percentage),
                    "fields": list(journals_new_list),
                }
            },
            {"missingSequentialItems": seq_detail},
        ]
