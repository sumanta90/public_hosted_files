import unittest
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


class TestDNAOperations(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDNAOperations, self).__init__(*args, **kwargs)
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
            "JOINED_DATE",
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

    def check_audit_trail(self, audit_trail):
        last_trail = audit_trail[-1]
        self.assertEqual(type(audit_trail), list)
        self.assertGreater(len(audit_trail), 0)
        self.assertIn("timestamp", last_trail.keys())
        self.assertIn("message", last_trail.keys())
        self.assertIn("Time taken:", last_trail["message"])

    def check_error_audit_trail(self, audit_trail):
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

    def test_count_operation_non_unique(self):
        output_path = "tests/test_analytics/test_dna/data/temp/emp_id_count.csv"
        input_column = "EMPLOYEE_ID"
        output_columns = [f"count_{input_column}"]
        output_data = [{f"count_{input_column}": 49}]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="count",
            unique=False,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
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

        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 1)
        self.assertEqual(output_resource.read_rows(), output_data)

        os.remove(output_path)

    def test_count_operation_unique(self):
        output_path = "tests/test_analytics/test_dna/data/temp/job_id_count_unique.csv"
        input_column = "JOB_ID"
        output_columns = [f"count_unique_{input_column}"]
        output_data = [{f"count_unique_{input_column}": 17}]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="count",
            unique=True,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
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

        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 1)
        self.assertEqual(output_resource.read_rows(), output_data)

        os.remove(output_path)

    def test_count_operation_with_null_value_numeric(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_count_operation_with_null_value_numeric.csv"
        input_column = "MANAGER_ID"
        output_columns = [f"count_{input_column}"]
        output_data = [{f"count_{input_column}": 49}]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="count",
            unique=False,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
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

        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 1)
        self.assertEqual(output_resource.read_rows(), output_data)

        os.remove(output_path)

    def test_count_operation_with_null_value_string(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_count_operation_with_null_value_string.csv"
        input_column = "PHONE_NUMBER"
        output_columns = [f"count_{input_column}"]
        output_data = [{f"count_{input_column}": 48}]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="count",
            unique=False,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
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

        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 1)
        self.assertEqual(output_resource.read_rows(), output_data)

        os.remove(output_path)
