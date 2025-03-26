import unittest
from decimal import Decimal
from typing import List, Dict, Any
from frictionless import Pipeline, steps
from frictionless.resources import TableResource
import json
from ebflow.analytics.analytics import DataAndAnalytics
from ebflow.analytics.analytics_schema import (
    AnalyticsPipeline,
    Edge,
    Node,
    NodeData,
    FilterOperations,
)
import os


class TestNumericNoneToZero(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestNumericNoneToZero, self).__init__(*args, **kwargs)
        self.input_columns = [
            "EMPLOYEE_ID",
            "FIRST_NAME",
            "LAST_NAME",
            "EMAIL",
            "PHONE_NUMBER",
            "HIRE_DATE",
            "JOB_ID",
            "SALARY",
            "COMMISSION_PCT",
            "MANAGER_ID",
            "DEPARTMENT_ID",
        ]

    def get_extract_node(self, id: str) -> Node:
        extract_node = Node(
            id="n1",
            data=NodeData(
                type="extract",
                file_name="dna_test_file.csv",
                file_path="tests/test_analytics/test_dna/data/dna_test_file.csv",
                ingestion="ingestion",
                cdm_file="cdm_file",
            ),
        )
        return extract_node

    def get_load_node(self, id: str) -> Node:
        load_node = Node(
            id=id,
            data=NodeData(
                type="load",
                file_name="output",
                file_path="path/to/output",
            ),
        )
        return load_node

    def check_audit_trail(self, audit_trail: List[Dict[str, str]]) -> None:
        last_trail = audit_trail[-1]
        self.assertEqual(type(audit_trail), list)
        self.assertGreater(len(audit_trail), 0)
        self.assertIn("timestamp", last_trail.keys())
        self.assertIn("message", last_trail.keys())
        self.assertIn("Time taken:", last_trail["message"])

    def check_error_audit_trail(self, audit_trail: List[Dict[str, str]]) -> None:
        last_trail = audit_trail[-1]
        second_last_trail = audit_trail[-2]
        self.assertEqual(type(audit_trail), list)
        self.assertGreater(len(audit_trail), 0)
        self.assertIn("timestamp", last_trail.keys())
        self.assertIn("message", last_trail.keys())
        self.assertEqual(
            "EBFlow Analytics | Data & Analytics pipeline process Failed",
            last_trail["message"],
        )
        self.assertIn("Error", second_last_trail["message"])

    def get_analytics_pipeline(
        self, operation: str, input_column: str, output_path: str
    ) -> DataAndAnalytics:
        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name=operation,
            column_name=input_column,
            unique=False,
            input_columns=self.input_columns,
            output_columns=self.input_columns,
        )
        transform_node = Node(id="n2", data=transform_node_data)

        edge1 = Edge(id="e1", source="n1", target="n2")
        edge2 = Edge(id="e2", source="n2", target="n3")

        pipeline = AnalyticsPipeline(
            nodes=[extract_node, transform_node, load_node],
            edges=[edge1, edge2],
        )
        self.assertIsInstance(pipeline, AnalyticsPipeline)

        analytics = DataAndAnalytics(pipeline=pipeline)
        self.assertIsInstance(analytics, DataAndAnalytics)

        return analytics

    def validate_output(
        self,
        output_path: str,
        output_columns: List[str],
        output_data: List[Dict[str, Any]],
        analytics: DataAndAnalytics,
    ) -> None:
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 1)
        self.assertEqual(output_resource.read_rows(), output_data)

        os.remove(output_path)

    def validate_exception(
        self,
        output_path: str,
        analytics: DataAndAnalytics,
    ) -> None:
        with self.assertRaises(Exception):
            analytics.process()
        self.assertFalse(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_error_audit_trail(audit_trail)

    def test_count_on_numeric(self):
        input_column = "MANAGER_ID"
        operation = "count"

        output_path = "tests/test_analytics/test_dna/data/temp/test_count_on_string.csv"
        output_columns = [f"{operation}_{input_column}"]
        output_data = [{f"{operation}_{input_column}": 49}]

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        analytics.process()

        self.validate_output(output_path, output_columns, output_data, analytics)

    def test_max_on_numeric(self):
        input_column = "MANAGER_ID"
        operation = "max"

        output_path = "tests/test_analytics/test_dna/data/temp/test_max_on_numeric.csv"
        output_columns = [f"{operation}_{input_column}"]
        output_data = [{f"{operation}_{input_column}": 205}]

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        analytics.process()

        self.validate_output(output_path, output_columns, output_data, analytics)

    def test_min_on_numeric(self):
        input_column = "MANAGER_ID"
        operation = "min"

        output_path = "tests/test_analytics/test_dna/data/temp/test_min_on_numeric.csv"
        output_columns = [f"{operation}_{input_column}"]
        output_data = [{f"{operation}_{input_column}": 0}]

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        analytics.process()

        self.validate_output(output_path, output_columns, output_data, analytics)

    def test_avg_on_numeric(self):
        input_column = "MANAGER_ID"
        operation = "avg"

        output_path = "tests/test_analytics/test_dna/data/temp/test_avg_on_numeric.csv"
        output_columns = [f"{operation}_{input_column}"]
        output_data = [{f"{operation}_{input_column}": Decimal("109.79592")}]

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        analytics.process()

        self.validate_output(output_path, output_columns, output_data, analytics)

    def test_sum_on_numeric(self):
        input_column = "MANAGER_ID"
        operation = "sum"

        output_path = "tests/test_analytics/test_dna/data/temp/test_avg_on_numeric.csv"
        output_columns = [f"{operation}_{input_column}"]
        output_data = [{f"{operation}_{input_column}": 5380}]

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        analytics.process()

        self.validate_output(output_path, output_columns, output_data, analytics)

    def test_count_on_string(self):
        input_column = "PHONE_NUMBER"
        operation = "count"

        output_path = "tests/test_analytics/test_dna/data/temp/test_count_on_string.csv"
        output_columns = [f"{operation}_{input_column}"]
        output_data = [{f"{operation}_{input_column}": 48}]

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        analytics.process()

        self.validate_output(output_path, output_columns, output_data, analytics)

    def test_max_on_string(self):
        input_column = "PHONE_NUMBER"
        operation = "max"

        output_path = "tests/test_analytics/test_dna/data/temp/test_max_on_string.csv"

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)

        self.validate_exception(output_path, analytics)

    def test_min_on_string(self):
        input_column = "PHONE_NUMBER"
        operation = "min"

        output_path = "tests/test_analytics/test_dna/data/temp/test_min_on_string.csv"

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        self.validate_exception(output_path, analytics)

    def test_avg_on_string(self):
        input_column = "PHONE_NUMBER"
        operation = "avg"
        output_path = "tests/test_analytics/test_dna/data/temp/test_avg_on_string.csv"

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        self.validate_exception(output_path, analytics)

    def test_sum_on_string(self):
        input_column = "PHONE_NUMBER"
        operation = "sum"
        output_path = "tests/test_analytics/test_dna/data/temp/test_sum_on_string.csv"

        analytics = self.get_analytics_pipeline(operation, input_column, output_path)
        self.validate_exception(output_path, analytics)
