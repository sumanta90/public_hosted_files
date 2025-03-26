import datetime
import unittest

from ebflow.utils.conditional_columns import check_for_condition


class TestUtilsFunctions(unittest.TestCase):
    def test_check_for_condition(self):
        record = {"age": 25}
        condition = {"variable": "age", "operator": "==", "value": 25}
        self.assertTrue(check_for_condition(record, condition))

        condition = {"variable": "age", "operator": "!=", "value": 30}
        self.assertTrue(check_for_condition(record, condition))

    def test_operation_cc_should_pass_test_1(self) -> None:
        records = {
            "AccountNumber": 2100,
            "Year": 2023,
        }
        condition = {
            "type": "logical",
            "operator": "and",
            "operands": [
                {
                    "type": "comparison",
                    "operator": ">",
                    "operands": [{"variable": "AccountNumber"}, {"value": 10}],
                },
                {
                    "type": "logical",
                    "operator": "and",
                    "operands": [
                        {
                            "type": "comparison",
                            "operator": "==",
                            "operands": [
                                {
                                    "type": "arithmetic",
                                    "operator": "+",
                                    "operands": [
                                        {"variable": "AccountNumber"},
                                        {"variable": "Year"},
                                    ],
                                },
                                {"value": 4123},
                            ],
                        },
                        {
                            "type": "comparison",
                            "operator": "==",
                            "operands": [
                                {"value": 2094},
                                {
                                    "type": "arithmetic",
                                    "operator": "+",
                                    "operands": [
                                        {"variable": "AccountNumber"},
                                        {"value": -6},
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
            "result": {"value": "Matches"},
        }
        result = check_for_condition(records, condition)
        self.assertTrue(result)

    def test_operation_cc_should_pass_test_2(self) -> None:
        records = {
            "AccountNumber": 2100,
            "AllowedAccountNumer": 10,
        }
        conditions = {
            "type": "logical",
            "operator": "and",
            "operands": [
                {
                    "type": "comparison",
                    "operator": ">",
                    "operands": [
                        {"variable": "AccountNumber"},
                        {"variable": "AllowedAccountNumer"},
                    ],
                }
            ],
            "result": {"value": "Matches"},
        }
        result = check_for_condition(records, conditions)
        self.assertTrue(result)

    def test_operation_cc_should_pass_test_3(self) -> None:
        records = {"Year": 2023}
        conditions = {
            "type": "logical",
            "operator": "and",
            "operands": [
                {
                    "type": "comparison",
                    "operator": "<",
                    "operands": [{"value": 2010}, {"variable": "Year"}],
                }
            ],
            "result": {"value": "Matches"},
        }
        result = check_for_condition(records, conditions)
        self.assertTrue(result)

    def test_operation_cc_should_pass_test_contains(self) -> None:
        records = {"Category": "Freehold"}
        conditions = {
            "type": "logical",
            "operator": "in",
            "operands": [{"value": "Free"}, {"variable": "Category"}],
            "result": {"value": "Matches"},
        }
        result = check_for_condition(records, conditions)
        self.assertTrue(result)

    def test_operation_cc_should_pass_test_no_default_condition(self) -> None:
        records = {"Category": "Freehold"}

        conditions = {
            "type": "logical",
            "operator": "in",
            "operands": [{"value": "Def"}, {"variable": "Category"}],
            "result": {"value": "Matches"},
        }

        result = check_for_condition(records, conditions)
        self.assertFalse(result)

    def test_operation_cc_condition_not_true(self) -> None:
        records = {
            "AccountNumber": "2100",
            "Year": "-6",
        }

        conditions = {
            "type": "logical",
            "operator": "and",
            "operands": [
                {
                    "type": "comparison",
                    "operator": ">",
                    "operands": [{"variable": "AccountNumber"}, {"value": 10}],
                }
            ],
            "result": {"value": "Matches"},
        }

        result = check_for_condition(records, conditions)
        self.assertTrue(result)

    def test_operation_cc_should_pass_date_condition_relativedelta(self) -> None:
        records = {
            "StartDate": datetime.date(day=1, month=2, year=2015),
            "EndDate": datetime.date(day=1, month=2, year=2016),
        }

        conditions = {
            "type": "date",
            "operator": "==",
            "id": 100,
            "operands": [
                {
                    "operator": "+",
                    "id": 10,
                    "type": "date",
                    "operands": [
                        {
                            "type": "date",
                            "date_format": "dd-mm-yyyy",
                            "date_sub_info": "date",
                            "variable": "StartDate",
                        },
                        {
                            "type": "date",
                            "date_format": "dd-mm-yyyy",
                            "date_sub_info": "relativedelta",
                            "delta_args": {"days": 1},
                        },
                    ],
                },
                {
                    "type": "date",
                    "date_format": "dd-mm-yyyy",
                    "date_sub_info": "date",
                    "value": "02-02-2015",
                },
            ],
            "result": {"value": "Matches"},
        }

        result = check_for_condition(records, conditions)
        self.assertTrue(result)

    def test_operation_cc_should_pass_date_condition_day_comparision(self) -> None:
        records = {
            "StartDate": datetime.date(day=1, month=2, year=2015)
            # datetime.date(day=1, month=2, year=2015)
        }

        conditions = {
            "type": "date",
            "operator": "==",
            "id": 100,
            "operands": [
                {
                    "operator": "+",
                    "id": 10,
                    "type": "date",
                    "date_format": "dd-mm-yyyy",
                    "date_sub_info": "day",
                    "variable": "StartDate",
                },
                {"value": 1},
            ],
            "result": {"value": "Matches"},
        }

        result = check_for_condition(records, conditions)
        self.assertTrue(result)
        # self.assertFalse(result)

    def test_operation_cc_should_pass_date_condition_month_comparision(self) -> None:
        records = {"StartDate": datetime.date(day=1, month=2, year=2015)}

        conditions = {
            "type": "date",
            "operator": "==",
            "id": 100,
            "operands": [
                {
                    "operator": "+",
                    "id": 10,
                    "type": "date",
                    "date_format": "dd-mm-yyyy",
                    "date_sub_info": "month",
                    "variable": "StartDate",
                },
                {"value": 2},
            ],
            "result": {"value": "Matches"},
        }

        result = check_for_condition(records, conditions)
        self.assertTrue(result)
        # self.assertFalse(result)

    def test_operation_cc_should_pass_date_condition_year_comparision(self) -> None:
        records = {"StartDate": datetime.date(day=1, month=2, year=2015)}

        conditions = {
            "type": "date",
            "operator": "==",
            "id": 100,
            "operands": [
                {
                    "operator": "+",
                    "id": 10,
                    "type": "datetime",
                    "date_format": "dd-mm-yyyy",
                    "date_sub_info": "year",
                    "variable": "StartDate",
                },
                {"value": 2015},
            ],
            "result": {"value": "Matches"},
        }

        result = check_for_condition(records, conditions)
        self.assertTrue(result)
        # self.assertFalse(result)

    def test_operation_cc_should_pass_arithmatic_with_different_operations(
        self,
    ) -> None:
        records = {"Debit": "£1", "Credit": "£1,000", "Net Movement": "-£1.00"}

        conditions = {
            "type": "comparision",
            "id": "clrexk5ak00003b6mbalso53f",
            "operator": "==",
            "operands": [
                {
                    "operands": [
                        {"type": "currency", "variable": "Debit"},
                        {"symbol": "*", "value": ""},
                        {"symbol": "(", "value": ""},
                        {"type": "currency", "variable": "Credit"},
                        {"symbol": "+", "type": "currency", "variable": "Net Movement"},
                        {"symbol": ")", "value": ""},
                    ]
                },
                {"value": 999.0},
            ],
            "result": {"value": "Matches"},
        }

        result = check_for_condition(records, conditions)
        self.assertTrue(result)

    def test_operation_cc_should_pass_arithmatic_with_different_operations_exception(
        self,
    ) -> None:
        records = {"Debit": "£10", "Credit": "£100", "Net Movement": "abc"}

        conditions = {
            "type": "comparision",
            "id": "clrexk5ak00003b6mbalso53f",
            "operator": "==",
            "operands": [
                {
                    "operands": [
                        {"type": "currency", "variable": "Debit"},
                        {"symbol": "+", "type": "currency", "variable": "Credit"},
                        {"symbol": "*", "type": "currency", "variable": "Net Movement"},
                    ]
                },
                {"value": -90.0},
            ],
            "result": {"value": "Matches"},
        }

        self.assertRaises(Exception, check_for_condition, records, conditions)
