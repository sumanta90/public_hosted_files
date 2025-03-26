import datetime
import unittest

from frictionless import Pipeline
from frictionless.resources import TableResource

from ebflow.extract.file_manager import FileManager
from ebflow.utils.schemas import ErpField, ErpFile, ExclusionRulesSet
from tests.test_extract.data.table_schema_for_tests import schemas_for_test_files


class TestFileManager(unittest.TestCase):
    def setUp(self):
        self.file_manager = FileManager()
        self.file_config = ErpFile(
            **{
                "file_name": "test name",
                "conform_dates": False,
                "encoding_format": "utf-8",
                "blank_replacement": False,
                "template": {"start_row": 1, "sheet_name": "Sheet2", "exclusion": None},
                "fields": [],
                "mandatory_field_check": [],
            }
        )

    def check_row_similarity(self, resource_rows, test_rows):
        for row_num, resource_row in enumerate(resource_rows):
            for field_name, value in resource_row.items():
                self.assertEqual(value, test_rows[row_num][field_name])

    def test_read_file_wrong_path(self):
        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/wrong_path.csv"
        )
        self.assertIsNone(resource)

    def test_read_file_csv_file(self):
        schema = [
            {"name": "Name", "type": "string"},
            {"name": "NO_NAME1", "type": "integer"},
            {"name": "DOB", "type": "string"},
        ]

        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/data.csv"
        )
        resource.transform(pipeline)
        self.assertEqual(resource.type, "table")
        self.assertEqual(resource.format, "inline")

        resource.infer()
        for i, field in enumerate(resource.schema.fields):
            self.assertEqual(field.name, schema[i]["name"])
            self.assertEqual(field.type, schema[i]["type"])

    def test_read_file_zip_file(self):
        schema = [
            {"name": "Name", "type": "string"},
            {"name": "NO_NAME1", "type": "integer"},
            {"name": "DOB", "type": "string"},
        ]

        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/data.zip", innerpath="data.csv"
        )
        resource.transform(pipeline)
        self.assertEqual(resource.type, "table")
        self.assertEqual(resource.format, "inline")

        resource.infer()
        for i, field in enumerate(resource.schema.fields):
            self.assertEqual(field.name, schema[i]["name"])
            self.assertEqual(field.type, schema[i]["type"])

    def test_read_file_csv_byte_file(self):
        schema = [
            {"name": "Name", "type": "string"},
            {"name": "NO_NAME1", "type": "integer"},
            {"name": "DOB", "type": "string"},
        ]

        file = open("tests/test_extract/data/data.csv", "rb")
        resource, pipeline = self.file_manager.read_file(
            content=file, content_format="csv"
        )
        resource.transform(pipeline)
        self.assertEqual(resource.type, "table")
        self.assertEqual(resource.format, "inline")

        resource.infer()
        for i, field in enumerate(resource.schema.fields):
            self.assertEqual(field.name, schema[i]["name"])
            self.assertEqual(field.type, schema[i]["type"])

    def test_read_file_excel_format(self):
        rows = [
            {"Name": "abc", "Age": 20, "DOB": datetime.datetime(2003, 12, 10, 0, 0)},
            {"Name": "def", "Age": 30, "DOB": None},
            {"Name": "ghi", "Age": None, "DOB": datetime.datetime(1983, 6, 10, 0, 0)},
        ]

        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/data.xlsx", file_config=self.file_config
        )
        resource.transform(pipeline)
        resource.infer()
        self.assertEqual(resource.type, "table")
        self.assertEqual(resource.format, "inline")
        self.assertEqual(resource.mediatype, "application/vnd.ms-excel")
        self.check_row_similarity(resource.read_rows(), rows)

    # def test_read_file_other_formats(self):
    #     rows = [{'Name': 'abc', 'Age': 20, 'DOB': '10-12-2003'},
    #             {'Name': 'def', 'Age': 30, 'DOB': None},
    #             {'Name': 'ghi', 'Age': None, 'DOB': '10-6-1983'}]

    #     path_details = {
    #         "tests/test_extract/data/data.txt": ['table', 'inline', 'text/csv'],
    #         "tests/test_extract/data/data.json": ['table', 'inline', 'text/json']
    #     }

    #     for path, details in path_details.items():
    #         resource, pipeline = self.file_manager.read_file(content=path)
    #         resource = resource.transform(pipeline)
    #         resource.infer()
    #         self.assertEqual(resource.type, details[0])
    #         self.assertEqual(resource.format, details[1])
    #         self.assertEqual(resource.mediatype, details[2])
    #         self.check_row_similarity(resource.read_rows(), rows)

    def test_read_file_empty(self):
        file_types = ["txt", "csv", "json", "xlsx"]
        for file_type in file_types:
            path = f"tests/test_extract/data/data_empty.{file_type}"
            resource, pipeline = self.file_manager.read_file(path)
            self.assertIsNone(resource)

    def test_read_file_with_clean_slice_and_skip_rows(self):
        self.file_config.template.start_row = 2
        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/data.csv",
            n_rows=2,
            clean_data=True,
            file_config=self.file_config,
        )
        resource.transform(pipeline)

        rows = [
            {"abc": None, "22": 30, "10-12-2003": None},
            {"abc": "ghi", "22": 25, "10-12-2003": None},
        ]

        self.check_row_similarity(resource.read_rows(), rows)

    def test_clean_data(self):
        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/data.csv"
        )

        rows_before = [
            {"Name": "abc", "NO_NAME1": 22, "DOB": "10-12-2003"},
            {"Name": "  ", "NO_NAME1": 30, "DOB": None},
            {"Name": "ghi", "NO_NAME1": 25, "DOB": "blank"},
            {"Name": None, "NO_NAME1": 18, "DOB": None},
        ]
        rows_after = [
            {"Name": "abc", "NO_NAME1": 22, "DOB": "10-12-2003"},
            {"Name": None, "NO_NAME1": 30, "DOB": None},
            {"Name": "ghi", "NO_NAME1": 25, "DOB": None},
            {"Name": None, "NO_NAME1": 18, "DOB": None},
        ]

        # check before cleaning
        self.check_row_similarity(resource.read_rows(), rows_before)

        # check after cleaning
        clean_steps = self.file_manager.clean_data()
        pipeline.steps.extend(clean_steps)
        resource.transform(pipeline)
        self.check_row_similarity(resource.read_rows(), rows_after)

    def test_read_file_exclude_rows(self):
        exclusion_rule_set = {
            "aggr": None,
            "rules": None,
            "expr": "(1 and 5 and (4 or 6)) and (2 or 3)",
            "column_expr": {
                "1": {"column": 2, "contains": "Store1", "exact": True},  # StoreID
                "2": {
                    "column": 3,
                    "contains": "Company",
                    "exact": False,
                },  # CompanyName
                "3": {"column": 5, "contains": "Item", "exact": False},  # ItemsCode
                "4": {
                    "column": 6,
                    "contains": "type",
                    "exact": False,
                },  # TransactionType
                "5": {"column": 7, "contains": "null", "exact": False},  # NullColumn
                "6": {
                    "column": 8,
                    "contains": "not_null",
                    "exact": False,
                },  # TransactionComplete
            },
        }

        self.file_config.template.exclusion = ExclusionRulesSet(**exclusion_rule_set)
        # Read the description column of this csv to know which tests are being conducted
        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/data_exclude.csv",
            file_config=self.file_config,
        )
        resource.transform(pipeline)
        expected_rows = [2, 6, 8, 10, 13, 14]
        actual_rows = []
        for row in resource.read_rows():
            actual_rows.append(row["Srno"])
        self.assertEqual(expected_rows, actual_rows)

    def test_read_file_exclude_rows_legacy_aggr_and(self):
        exclusion_rule_set = {
            "aggr": "and",
            "rules": [
                {"column": 2, "contains": "Store1", "exact": True},
                {"column": 3, "contains": "Company", "exact": False},
                {"column": 5, "contains": "Item", "exact": False},
                {"column": 6, "contains": "type", "exact": False},
                {"column": 7, "contains": "null", "exact": False},
                {"column": 8, "contains": "not_null", "exact": False},
            ],
            "expr": None,
            "column_expr": None,
        }

        self.file_config.template.exclusion = ExclusionRulesSet(**exclusion_rule_set)
        # Read the description column of this csv to know which tests are being conducted
        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/data_exclude.csv",
            file_config=self.file_config,
        )
        resource.transform(pipeline)
        # this is an 'and' expression so rows with any rule exception will be included
        expected_rows = [2, 3, 4, 5, 6, 8, 10, 11, 13, 14]
        actual_rows = []
        for row in resource.read_rows():
            actual_rows.append(row["Srno"])
        self.assertEqual(expected_rows, actual_rows)

    def test_read_file_exclude_rows_legacy_aggr_or(self):
        exclusion_rule_set = {
            "aggr": "or",
            "rules": [
                {"column": 2, "contains": "Store1", "exact": True},
                {"column": 3, "contains": "Company", "exact": False},
                {"column": 5, "contains": "Item", "exact": False},
                {"column": 6, "contains": "type", "exact": False},
                {"column": 7, "contains": "null", "exact": False},
                {"column": 8, "contains": "not_null", "exact": False},
            ],
            "expr": None,
            "column_expr": None,
        }

        self.file_config.template.exclusion = ExclusionRulesSet(**exclusion_rule_set)
        # Read the description column of this csv to know which tests are being conducted
        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/data_exclude.csv",
            file_config=self.file_config,
        )
        resource.transform(pipeline)
        # this is an 'or' expression so only row 13 with everything wrong is mentioned
        expected_rows = [13]
        actual_rows = []
        for row in resource.read_rows():
            actual_rows.append(row["Srno"])
        self.assertEqual(expected_rows, actual_rows)

    def test_read_file_generate_schema(self):
        fields = [
            {
                "field_name": "SrNo.",
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
        self.file_config.fields = [ErpField(**field) for field in fields]
        resource, pipeline = self.file_manager.read_file(
            content="tests/test_extract/data/custom_imperfect.csv",
            file_config=self.file_config,
        )
        resource.transform(pipeline)

        for i, field in enumerate(resource.schema.to_dict()["fields"]):
            self.assertDictEqual(
                field, schemas_for_test_files["schema_CustomImperfect"]["fields"][i]
            )

    def test_pre_mapping_fill_down_1(self):
        test_data = [
            {"A": "a", "B": None},
            {"A": None, "B": None},
            {"A": None, "B": None},
            {"A": None, "B": "a"},
            {"A": None, "B": None},
            {"A": None, "B": "b"},
            {"A": "b", "B": "c"},
            {"A": None, "B": "d"},
            {"A": None, "B": None},
            {"A": "c", "B": None},
        ]
        excepted_output = [
            {"A": "a", "B": None},
            {"A": "a", "B": None},
            {"A": "a", "B": None},
            {"A": "a", "B": "a"},
            {"A": "a", "B": "a"},
            {"A": "a", "B": "b"},
            {"A": "b", "B": "c"},
            {"A": "b", "B": "d"},
            {"A": "b", "B": "d"},
            {"A": "c", "B": "d"},
        ]
        fill_down_options = [
            {
                "name": "A",
                "fill_down": True,
            },
            {
                "name": "B",
                "fill_down": True,
            },
        ]

        resource = TableResource(data=test_data)
        fill_down_steps = self.file_manager.get_fill_down_steps(fill_down_options)
        resource.transform(Pipeline(steps=fill_down_steps))
        self.check_row_similarity(excepted_output, resource.read_rows())
