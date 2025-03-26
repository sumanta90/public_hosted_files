from datetime import datetime

from frictionless import Schema
from frictionless.resources import TableResource

test_resource = TableResource(
    path="tests/test_transform/data/generate_cdm_fields_test.csv"
)
schema = {
    "fields": [
        {"name": "erp_string", "type": "string"},
        {"name": "erp_number1", "type": "number"},
        {"name": "erp_number2", "type": "number"},
        {"name": "erp_bool", "type": "boolean"},
        {"name": "erp_date", "type": "date", "format": "%d-%m-%Y"},
        {"name": "erp_time", "type": "time", "format": "%H:%M"},
        {"name": "erp_datetime", "type": "datetime", "format": "%d-%m-%Y %H:%M"},
        {"name": "erp_fill_down", "type": "string"},
        {"name": "erp_account", "type": "string"},
    ],
    "missingValues": ["None", " ", ""],
}

test_resource.schema = Schema.from_descriptor(schema)

mapping = [
    {
        "report_name": "test_report_name",
        "file_info": {
            "test_file_name": {"resource": test_resource, "file_checks": None}
        },
        "mapping": [
            {
                "cdm_field": "dummy",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [],
                "map_type": "dummy",
            },
            {
                "cdm_field": "direct_none",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [],
                "map_type": "direct",
            },
            {
                "cdm_field": "direct_string",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_string", "file_name": "test_file_name"}
                ],
                "map_type": "direct",
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "direct_number",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"}
                ],
                "map_type": "direct",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "direct_bool",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_bool", "file_name": "test_file_name"}
                ],
                "map_type": "direct",
                "data_type": "boolean",
                "date_format": None,
            },
            {
                "cdm_field": "direct_datetime",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_datetime", "file_name": "test_file_name"}
                ],
                "map_type": "direct",
                "data_type": "datetime",
                "date_format": "dd/mm/yyyy HH:MM:SS",
            },
            {
                "cdm_field": "date_mask",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_datetime", "file_name": "test_file_name"}
                ],
                "map_type": "date-mask",
                "data_type": "datetime",
                "date_format": "dd/mm/yyyy HH:MM:SS",
            },
            {
                "cdm_field": "dconcat",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_date",
                        "order": 1,
                        "file_name": "test_file_name",
                    },
                    {
                        "field_name": "erp_time",
                        "order": 2,
                        "file_name": "test_file_name",
                    },
                ],
                "map_type": "calculated",
                "operation": "DCONCAT",
                "data_type": "datetime",
                "date_format": "dd/mm/yyyy",
                "time_format": "HH:MM:SS",
            },
            {
                "cdm_field": "free_text",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [],
                "map_type": "calculated",
                "operation": "FT",
                "free_text": "test_text",
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "manual_calculation",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_number1",
                        "order": 1,
                        "file_name": "test_file_name",
                    },
                    {
                        "field_name": "erp_number2",
                        "order": 2,
                        "file_name": "test_file_name",
                    },
                ],
                "map_type": "calculated",
                "operation": "MC",
                "expression": " (<1> + <2> - <1>) * (<1>/<2>) ^ 2 ",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "fill_down",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_fill_down", "file_name": "test_file_name"}
                ],
                "map_type": "calculated",
                "operation": "FILLDOWN",
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "journal_line_number",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_string", "file_name": "test_file_name"}
                ],
                "map_type": "calculated",
                "operation": "Journal Line Number",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "rba",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"},
                    {"field_name": "erp_number2", "file_name": "test_file_name"},
                ],
                "map_type": "calculated",
                "operation": "RBA",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "abs",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"}
                ],
                "map_type": "calculated",
                "operation": "ABS",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "dci1",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"}
                ],
                "map_type": "calculated",
                "operation": "DCI1",
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "split",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_string", "file_name": "test_file_name"}
                ],
                "map_type": "calculated",
                "operation": "SPLIT",
                "extra": "True",
                "characters": 2,
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "filename_mismatch",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"},
                    {"field_name": "erp_number2", "file_name": "test_file_name_wrong"},
                ],
                "map_type": "calculated",
                "operation": "DIV",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "division",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"},
                    {"field_name": "erp_number2", "file_name": "test_file_name"},
                ],
                "map_type": "calculated",
                "operation": "DIV",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "range_overlap",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"},
                    {"field_name": "erp_number2", "file_name": "test_file_name"},
                ],
                "map_type": "calculated",
                "operation": "DCI3",
                "rangeDetails": [
                    {"start": 5, "end": 15, "negatives": "Credit"},
                    {"start": 10, "end": 20, "negatives": "Debit"},
                ],
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "dci3",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number2", "file_name": "test_file_name"},
                    {"field_name": "erp_number1", "file_name": "test_file_name"},
                ],
                "map_type": "calculated",
                "operation": "DCI3",
                "rangeDetails": [
                    {"start": 5, "end": 15, "negatives": "Credit"},
                    {"start": 16, "end": 25, "negatives": "Debit"},
                ],
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "cs",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"},
                    {"field_name": "erp_number2", "file_name": "test_file_name"},
                ],
                "map_type": "calculated",
                "operation": "CS",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "dc",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {"field_name": "erp_number1", "file_name": "test_file_name"},
                    {"field_name": "erp_number2", "file_name": "test_file_name"},
                ],
                "map_type": "calculated",
                "operation": "DC",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "sum",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_number1",
                        "file_name": "test_file_name",
                        "order": 1,
                    },
                    {
                        "field_name": "erp_number2",
                        "file_name": "test_file_name",
                        "order": 2,
                    },
                ],
                "map_type": "calculated",
                "operation": "SUM",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "difference",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_number1",
                        "file_name": "test_file_name",
                        "order": 1,
                    },
                    {
                        "field_name": "erp_number2",
                        "file_name": "test_file_name",
                        "order": 2,
                    },
                ],
                "map_type": "calculated",
                "operation": "DIFF",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "product",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_number1",
                        "file_name": "test_file_name",
                        "order": 1,
                    },
                    {
                        "field_name": "erp_number2",
                        "file_name": "test_file_name",
                        "order": 2,
                    },
                ],
                "map_type": "calculated",
                "operation": "PROD",
                "data_type": "number",
                "date_format": None,
            },
            {
                "cdm_field": "concat",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_number1",
                        "file_name": "test_file_name",
                        "order": 1,
                    },
                    {
                        "field_name": "erp_number2",
                        "file_name": "test_file_name",
                        "order": 2,
                    },
                ],
                "map_type": "calculated",
                "operation": "CONCAT",
                "separator": " (@_@ ) ",
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "dci2",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_number1",
                        "file_name": "test_file_name",
                        "order": 1,
                    },
                    {
                        "field_name": "erp_number2",
                        "file_name": "test_file_name",
                        "order": 2,
                    },
                ],
                "map_type": "calculated",
                "operation": "DCI2",
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "sglan_inside",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_account",
                        "file_name": "test_file_name",
                        "order": 1,
                    }
                ],
                "map_type": "calculated",
                "operation": "SGLAN",
                "extract_split_fields": {
                    "opening_char": "<|",
                    "opening_char_number": 2,
                    "closing_char": "|>",
                    "closing_char_number": 2,
                    "inside_outside": "inside",
                },
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "sglan_outside",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_account",
                        "file_name": "test_file_name",
                        "order": 1,
                    }
                ],
                "map_type": "calculated",
                "operation": "SGLAN",
                "extract_split_fields": {
                    "opening_char": "<|",
                    "opening_char_number": 2,
                    "closing_char": "|>",
                    "closing_char_number": 2,
                    "inside_outside": "outside",
                },
                "data_type": "string",
                "date_format": None,
            },
            {
                "cdm_field": "conditional_column",
                "original_filename": "test_original_filename",
                "file_name": "test_file_name",
                "erp_fields": [
                    {
                        "field_name": "erp_bool",
                        "file_name": "test_file_name",
                        "order": 1,
                    }
                ],
                "map_type": "calculated",
                "operation": "CC",
                "conditions": [{"result": {"variable": "erp_bool"}}],
                "data_type": "boolean",
                "date_format": None,
            },
        ],
    }
]

required_output = {
    "dummy": [None, None, None, None, None, None],
    "direct_none": [None, None, None, None, None, None],
    "direct_number": [10.0, -30.0, 40.0, -20.0, 40.0, 20.0],
    "direct_string": ["Abhay", "Raj", "Raj", "Abhay", "Raj", "Abhay"],
    "direct_bool": [True, False, True, True, False, False],
    "direct_datetime": [
        datetime(2004, 4, 25, 16, 40),
        datetime(2005, 3, 15, 15, 30),
        datetime(2005, 3, 15, 15, 30),
        datetime(2004, 4, 25, 16, 40),
        datetime(2005, 3, 15, 15, 30),
        datetime(2004, 4, 25, 16, 40),
    ],
    "date_mask": [
        datetime(2004, 4, 25, 16, 40),
        datetime(2005, 3, 15, 15, 30),
        datetime(2005, 3, 15, 15, 30),
        datetime(2004, 4, 25, 16, 40),
        datetime(2005, 3, 15, 15, 30),
        datetime(2004, 4, 25, 16, 40),
    ],
    "dconcat": [
        datetime(2000, 12, 10, 13, 20),
        datetime(2000, 6, 11, 5, 40),
        datetime(2000, 6, 11, 5, 40),
        datetime(2000, 12, 10, 13, 20),
        datetime(2000, 6, 11, 5, 40),
        datetime(2000, 12, 10, 13, 20),
    ],
    "free_text": [
        "test_text",
        "test_text",
        "test_text",
        "test_text",
        "test_text",
        "test_text",
    ],
    "manual_calculation": [5.0, 90.0, None, 20.0, None, 20.0],
    "fill_down": [None, None, "xyz", "xyz", "abc", "abc"],
    "journal_line_number": [1.0, 1.0, 2.0, 2.0, 3.0, 3.0],
    "rba": [20.0, 10.0, 0.0, 20.0, 0.0, 20.0],
    "abs": [10.0, 30.0, 40.0, 20.0, 40.0, 20.0],
    "dci1": ["D", "C", "D", "C", "D", "D"],
    "split": ["Ab", "Ra", "Ra", "Ab", "Ra", "Ab"],
    "extra": ["hay", "j", "j", "hay", "j", "hay"],
    "filename_mismatch": [None, None, None, None, None, None],
    "division": [0.5, -3.0, None, -1.0, None, 1.0],
    "range_overlap": [None, None, None, None, None, None],
    "dci3": ["C", "C", None, "D", None, "C"],
    "cs": [10.0, -30.0, 40.0, -20.0, 40.0, 20.0],
    "dc": [None, None, 40.0, None, 40.0, None],
    "sum": [30.0, -20.0, 40.0, 0.0, 40.0, 40.0],
    "difference": [-10.0, -40.0, 40.0, -40.0, 40.0, 0.0],
    "product": [200.0, -300.0, 0.0, -400.0, 0.0, 400.0],
    "concat": [
        "10 (@_@ ) 20",
        "-30 (@_@ ) 10",
        "40 (@_@ ) 0",
        "-20 (@_@ ) 20",
        "40 (@_@ ) 0",
        "20 (@_@ ) 20",
    ],
    "dci2": [None, None, "D", None, "D", None],
    "sglan_inside": [
        "account name",
        None,
        "abc<|account name|>xyz",
        "account name",
        None,
        None,
    ],
    "sglan_outside": ["<|abc|>xyz", "account <|abc|> name", None, None, None, None],
    "conditional_column": [True, False, True, True, False, False],
}
