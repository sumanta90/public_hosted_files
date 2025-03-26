import unittest
import yaml
import datetime
from ebflow.extract.file_checks import FileChecks
from ebflow.utils.schemas import ErpFile, ErpField
from ebflow.utils.constants import Constants

from tests.test_extract.data.table_schema_for_tests import schemas_for_test_files

from frictionless import Dialect, Schema, Pipeline, steps
from frictionless.resources import TableResource


class TestFileChecks(unittest.TestCase):
    def setUp(self):
        self.file_checks = FileChecks()

        dialect = Dialect(
            comment_rows=list(range(1, 2)),
            skip_blank_rows=True,
        )
        self.resource_perfect = TableResource(
            path="tests/test_extract/data/gld.csv", format="csv", dialect=dialect
        )
        self.resource_imperfect = TableResource(
            path="tests/test_extract/data/gld_mperfect.csv",
            format="csv",
            dialect=dialect,
        )

        # Add the corresponding schemas to the resources
        self.resource_perfect.infer()
        self.resource_imperfect.infer()
        self.resource_perfect.schema = Schema.from_descriptor(
            schemas_for_test_files["schema_gl_details_perfect"]
        )
        self.resource_imperfect.schema = Schema.from_descriptor(
            schemas_for_test_files["schema_gl_details_imperfect"]
        )

        # Open the YAML file and load it into a dictionary
        with open("tests/test_extract/data/sage200.yml", "r") as file:
            erp_yml = yaml.safe_load(file)
        self.file_config = None
        for report in erp_yml["mappings"][0]["reports"]:
            if report["report_type"] == "glDetail":
                self.file_config = report["input_files"][0]
                break

        self.file_config = ErpFile(**self.file_config)
        self.mandatory_fields = list(
            filter(
                lambda field: len(field.cdm_field_names) > 0, self.file_config.fields
            )
        )

    def test_mandatory_field_check_perfect(self):
        self.resource_perfect.infer()
        mandatory_field_check = self.file_checks.mandatory_field_check(
            self.resource_perfect.header,
            self.mandatory_fields,
            self.file_config.mandatory_field_check,
        )
        self.assertTrue(mandatory_field_check.valid)

        # all fields should be present
        for field_validation in mandatory_field_check.data:
            self.assertTrue(field_validation.present)

    def test_mandatory_field_check_imperfect(self):
        self.resource_imperfect.infer()

        mandatory_field_check = self.file_checks.mandatory_field_check(
            self.resource_imperfect.header,
            self.mandatory_fields,
            self.file_config.mandatory_field_check,
        )
        self.assertFalse(mandatory_field_check.valid)

        # missing field should have present = False
        for field_validation in mandatory_field_check.data:
            if field_validation.input_field == "NLNominalAccounts.AccountNumber":
                self.assertFalse(field_validation.present)
            elif field_validation.input_field == "Custom1":
                self.assertFalse(field_validation.present)
            elif field_validation.input_field == "Custom2":
                self.assertFalse(field_validation.present)
            elif field_validation.input_field == "Custom3":
                self.assertTrue(field_validation.present)
            elif field_validation.input_field == "Custom4":
                self.assertTrue(field_validation.present)
            else:
                self.assertTrue(field_validation.present)

    def test_imperfect_file_check_perfect(self):
        self.resource_perfect.infer()
        imperfect_file_check = self.file_checks.imperfect_file_check(
            self.resource_perfect, self.file_config.fields
        )
        self.assertTrue(imperfect_file_check.valid)

        # checks if summary output is correct
        for ifc_summary in imperfect_file_check.summary:
            self.assertTrue(ifc_summary.valid)

        for ifc_field in imperfect_file_check.fields:
            self.assertTrue(ifc_field["valid"])
            self.assertTrue(ifc_field["present"])
            self.assertTrue(ifc_field["null_check_valid"])
            self.assertTrue(ifc_field["type_check_valid"])
            self.assertEqual([], ifc_field["null_check_data"])
            self.assertEqual([], ifc_field["type_check_data"])

    def test_imperfect_file_check_imperfect(self):
        self.resource_imperfect.infer()
        imperfect_file_check = self.file_checks.imperfect_file_check(
            self.resource_imperfect, self.file_config.fields
        )
        self.assertFalse(imperfect_file_check.valid)

        # check is summary output is correct
        for ifc_summary in imperfect_file_check.summary:
            self.assertFalse(ifc_summary.valid)
            if ifc_summary.name == "Missing Data":
                self.assertEqual(
                    "1 Missing Data element identified", ifc_summary.message
                )
            if ifc_summary.name == "Missing Column":
                self.assertEqual("1 Missing column identified", ifc_summary.message)
            if ifc_summary.name == "Data Format":
                self.assertEqual(
                    "1 Data format compatibility issue identified", ifc_summary.message
                )

        # check if only specified field are reported invalid or missing for null or type check
        # also checks if the correct rows are metioned for null or type check
        # any other fields should be valid in every case and no rows should be reported for null or type error
        for ifc_field in imperfect_file_check.fields:
            if ifc_field["field_name"] == "NLNominalAccounts.AccountNumber":
                self.assertFalse(ifc_field["valid"])
                self.assertFalse(ifc_field["present"])
            elif ifc_field["field_name"] == "NLPostedNominalTrans.TransactionDate":
                self.assertFalse(ifc_field["valid"])
                self.assertTrue(ifc_field["present"])
                self.assertTrue(ifc_field["null_check_valid"])
                self.assertFalse(ifc_field["type_check_valid"])
                self.assertEqual(
                    "Contains 1 row with invalid data type",
                    ifc_field["type_check_comment"],
                )
                self.assertEqual([], ifc_field["null_check_data"])
                self.assertEqual(["7"], ifc_field["type_check_data"])
            elif (
                ifc_field["field_name"] == "NLPostedNominalTrans.UniqueReferenceNumber"
            ):
                self.assertFalse(ifc_field["valid"])
                self.assertTrue(ifc_field["present"])
                self.assertFalse(ifc_field["null_check_valid"])
                self.assertTrue(ifc_field["type_check_valid"])
                self.assertEqual(
                    "Contains 1 null values", ifc_field["null_check_comment"]
                )
                self.assertEqual(["15"], ifc_field["null_check_data"])
                self.assertEqual([], ifc_field["type_check_data"])
            else:
                self.assertTrue(ifc_field["valid"])
                self.assertTrue(ifc_field["present"])
                self.assertTrue(ifc_field["null_check_valid"])
                self.assertTrue(ifc_field["type_check_valid"])
                self.assertEqual([], ifc_field["null_check_data"])
                self.assertEqual([], ifc_field["type_check_data"])

    def test_imperfect_file_check_all_data_types(self):
        fields = [
            {
                "field_name": "Sr.no",
                "data_type": "number",
                "date_format": "",
                "nullable": False,
            },
            {"field_name": "AccountName", "data_type": "string", "date_format": ""},
            {"field_name": "Balance", "data_type": "number", "date_format": ""},
            {
                "field_name": "URN",
                "data_type": "string",
                "date_format": "",
                "nullable": False,
            },
            {
                "field_name": "TransactionDate",
                "data_type": "datetime",
                "date_format": "dd-mm-yyyy",
            },
            {"field_name": "Narrative", "data_type": "string", "date_format": ""},
            {"field_name": "GoodsValue", "data_type": "decimal", "date_format": ""},
            {"field_name": "Amount", "data_type": "currency", "date_format": ""},
            {"field_name": "Completed", "data_type": "boolean", "date_format": ""},
        ]

        resource = TableResource(path="tests/test_extract/data/custom_imperfect.csv")
        resource.infer()
        resource.schema = Schema.from_descriptor(
            schemas_for_test_files["schema_CustomImperfect"]
        )
        erp_fields = [ErpField(**field) for field in fields]
        ifc_all_data_types = self.file_checks.imperfect_file_check(resource, erp_fields)

        # Check if Data Format summary is correct
        data_format_summary = ifc_all_data_types.summary[-1]
        self.assertFalse(data_format_summary.valid)
        self.assertEqual(
            data_format_summary.message, "7 Data format compatibility issues identified"
        )

        for (
            field
        ) in (
            ifc_all_data_types.fields
        ):  # check if correct rows have type error for each field
            if field["field_name"] == "Amount":
                self.assertFalse(field["valid"])
                self.assertFalse(field["type_check_valid"])
                self.assertEqual(
                    field["type_check_comment"],
                    "Contains 2 rows with invalid data type",
                )
                self.assertEqual(field["type_check_data"], ["3", "6"])
            elif field["field_name"] == "Balance":
                self.assertFalse(field["valid"])
                self.assertFalse(field["type_check_valid"])
                self.assertEqual(
                    field["type_check_comment"], "Contains 1 row with invalid data type"
                )
                self.assertEqual(field["type_check_data"], ["5"])
            elif field["field_name"] == "Completed":
                self.assertFalse(field["valid"])
                self.assertFalse(field["type_check_valid"])
                self.assertEqual(
                    field["type_check_comment"], "Contains 1 row with invalid data type"
                )
                self.assertEqual(field["type_check_data"], ["7"])
            elif field["field_name"] == "GoodsValue":
                self.assertFalse(field["valid"])
                self.assertFalse(field["type_check_valid"])
                self.assertEqual(
                    field["type_check_comment"], "Contains 1 row with invalid data type"
                )
                self.assertEqual(field["type_check_data"], ["9"])
            elif field["field_name"] == "TransactionDate":
                self.assertFalse(field["valid"])
                self.assertFalse(field["type_check_valid"])
                self.assertEqual(
                    field["type_check_comment"],
                    "Contains 2 rows with invalid data type",
                )
                self.assertEqual(field["type_check_data"], ["4", "6"])

        data_rows = resource.read_rows()

        # What the rows should look like after they are read correctly | Wrong cells are replaced with None
        balance = [
            1798592.97,
            1798592.97,
            1798592.97,
            1798592.97,
            None,
            1798592.97,
            1798592,
            1798592.97,
            1798592.97,
        ]
        transaction_date = [
            datetime.datetime(2020, 7, 4, 0, 0),
            datetime.datetime(2020, 7, 5, 0, 0),
            datetime.datetime(2022, 7, 4, 0, 0),
            None,
            datetime.datetime(2024, 7, 4, 0, 0),
            None,
            datetime.datetime(2026, 7, 4, 0, 0),
            datetime.datetime(2027, 7, 4, 0, 0),
            datetime.datetime(2028, 7, 4, 0, 0),
        ]
        goods_value = [
            6536.62,
            3088.39,
            1800.02,
            10754.76,
            8965.81,
            150,
            225,
            160,
            None,
        ]
        amount = [240, 230, None, 220, 220, None, 250, -5000, 250]
        completed = [True, True, True, False, True, False, None, False, True]

        # Match each rows values with correct ones | converting to float as frictionless reads number into a 'Decimal' type
        for index, row in enumerate(data_rows):
            self.assertEqual(
                float(row["Balance"]) if row["Balance"] else row["Balance"],
                balance[index],
            )
            self.assertEqual(row["TransactionDate"], transaction_date[index])
            self.assertEqual(
                float(row["GoodsValue"]) if row["GoodsValue"] else row["GoodsValue"],
                goods_value[index],
            )
            self.assertEqual(
                float(row["Amount"]) if row["Amount"] else row["Amount"], amount[index]
            )
            self.assertEqual(row["Completed"], completed[index])

    def test_imperfect_file_check_all_datetime_formats(self):
        # This function checks if all datetime formats will be read by frictionless correctly
        schema = {
            "fields": [],  # should be in the same order as csv header
            "missingValues": [
                "None",
                " ",
                "",
            ],  # these values in csv will be read as None
        }
        erp_fields = []
        for key, value in Constants.DATETIME_FORMATS.items():
            if "TZ" in key:
                continue
            field_details = {
                "name": key,
                "title": key,
                "type": "datetime",
                "format": value,
                "constraints": {
                    "required": True,
                },
            }
            schema["fields"].append(field_details)

            erp_field = {
                "field_name": key,
                "data_type": "datetime",
                "date_format": key,
                "nullable": False,
                "required": True,
                "cdm_field_names": [],
            }
            erp_fields.append(ErpField(**erp_field))

        resource = TableResource(path="tests/test_extract/data/date_time.csv")
        resource.infer()
        resource.schema = Schema.from_descriptor(schema)
        ifc_all_datetime_formats = self.file_checks.imperfect_file_check(
            resource, erp_fields
        )

        self.assertTrue(ifc_all_datetime_formats.valid)
        for ifc_field in ifc_all_datetime_formats.fields:
            self.assertTrue(ifc_field["valid"])
            self.assertTrue(ifc_field["present"])
            self.assertTrue(ifc_field["type_check_valid"])
            self.assertEqual(ifc_field["type_check_data"], [])

    def test_validate_resource_head(self):
        self.resource_perfect.infer()
        pipeline = Pipeline(steps=[steps.table_normalize(), steps.row_slice(head=0)])

        _, view = self.file_checks.validate_resource_head(
            self.resource_perfect, pipeline, self.file_config, n_rows=1
        )

        required_view = (
            "+---------------------------------+-------------------------------------+-------------------------------------+-------------------------------+-----------------------------------------+--------------------------------------------+--------------------------+--------------------------------------+-----------------------------------+--------------------------------+--------------------------------+-----------------------------------------------+----------------------------------------------+---------------------------+--------------------------+---------+---------+---------+---------+\n"
            "| NLNominalAccounts.AccountNumber | NLNominalAccounts.AccountCostCentre | NLNominalAccounts.AccountDepartment | NLNominalAccounts.AccountName | NLNominalAccounts.BroughtForwardBalance | NLPostedNominalTrans.UniqueReferenceNumber | NLPostedNominalTrans.URN | NLPostedNominalTrans.TransactionDate | SYSAccountingPeriods.PeriodNumber | NLPostedNominalTrans.Reference | NLPostedNominalTrans.Narrative | NLPostedNominalTrans.GoodsValueInBaseCurrency | NLPostedNominalTrans.TransactionAnalysisCode | NLNominalAccounts.Balance | SYSCompanies.CompanyName | Custom1 | Custom2 | Custom3 | Custom4 |\n"
            "+=================================+=====================================+=====================================+===============================+=========================================+============================================+==========================+======================================+===================================+================================+================================+===============================================+==============================================+===========================+==========================+=========+=========+=========+=========+\n"
            "| '13101'                         | None                                | None                                | 'Stocks - Finished Goods'     |                              1798592.97 | '2458'                                     | '003-11-0000002458'      | datetime.datetime(2020, 7, 4, 0, 0)  | '7'                               | 'BOM Build'                    | '2.00506E+13'                  |                                       6536.62 | None                                         |                5017520.39 | 'HomeStyle Kitchens Ltd' | None    | None    | None    | None    |\n"
            "+---------------------------------+-------------------------------------+-------------------------------------+-------------------------------+-----------------------------------------+--------------------------------------------+--------------------------+--------------------------------------+-----------------------------------+--------------------------------+--------------------------------+-----------------------------------------------+----------------------------------------------+---------------------------+--------------------------+---------+---------+---------+---------+\n"
        )

        self.assertEqual(required_view, view)
