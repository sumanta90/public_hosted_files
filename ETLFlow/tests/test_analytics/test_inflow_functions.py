import os
import unittest
from decimal import Decimal

import petl as etl
from frictionless import fields
from frictionless.resources import TableResource

from ebflow.analytics.inflow_functions import (
    generate_inflow_general_ledger,
    generate_inflow_trial_balance,
)


class TestInflowFunctions(unittest.TestCase):

    def test_generate_inflow_general_ledger(self):
        resource = TableResource(
            path="tests/test_analytics/data/inflow_general_ledger.csv"
        )
        resource.infer()

        effective_date_field = fields.DateField(name="effectiveDate", format="%d/%m/%Y")
        entered_date_time_field = fields.DatetimeField(
            name="enteredDateTime", format="%d/%m/%Y %H:%M:%S"
        )
        amount_field = fields.NumberField(name="amount")
        resource.schema.set_field(effective_date_field)
        resource.schema.set_field(entered_date_time_field)
        resource.schema.set_field(amount_field)

        temp_path = "tests/test_analytics/data/temp/"

        output_path = generate_inflow_general_ledger(resource, temp_path=temp_path)
        self.assertEqual(
            output_path, "tests/test_analytics/data/temp/Inflow_General_Ledger.csv"
        )

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        required_header = [
            "Account Code",
            "Transaction Id",
            "Net",
            "Effective Date",
            "Created Date/Time",
            "Document Type",
            "User Id",
            "Reference",
            "Journal Description",
            "Line Description",
        ]
        actual_header = output_resource.header
        self.assertListEqual(actual_header, required_header)

        required_row = {
            "Account Code": 1001,
            "Transaction Id": "T123",
            "Net": Decimal("619157.11"),
            "Effective Date": "15/01/2022",
            "Created Date/Time": "15/01/2022 10:30:00",
            "Document Type": "Sales",
            "User Id": "User1",
            "Reference": "Ref123",
            "Journal Description": "Sale Invoice",
            "Line Description": "Product A Sale",
        }
        actual_row = output_resource.read_rows()[0].to_dict()
        self.assertDictEqual(actual_row, required_row)

        os.remove(output_path)

    def test_generate_inflow_trial_balance(self):
        resource = TableResource(
            path="tests/test_analytics/data/inflow_trial_balance.csv"
        )
        resource.infer()
        temp_path = "tests/test_analytics/data/temp/"

        output = generate_inflow_trial_balance(resource, temp_path=temp_path)

        open_path, close_path = output[0], output[1]
        self.assertEqual(
            open_path, "tests/test_analytics/data/temp/Inflow_Opening_Trial_Balance.csv"
        )
        self.assertEqual(
            close_path,
            "tests/test_analytics/data/temp/Inflow_Closing_Trial_Balance.csv",
        )

        open_resource = TableResource(path=open_path)
        open_resource.infer()
        close_resource = TableResource(path=close_path)
        close_resource.infer()

        open_required_header = ["AccountCode", "AccountDescription", "OpeningNet"]
        close_required_header = ["AccountCode", "AccountDescription", "ClosingNet"]

        open_actual_header = open_resource.header
        close_actual_header = close_resource.header

        self.assertListEqual(open_actual_header, open_required_header)
        self.assertListEqual(close_actual_header, close_required_header)

        os.remove(open_path)
        os.remove(close_path)
