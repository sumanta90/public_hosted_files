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


class TestNWDOperation(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestNWDOperation, self).__init__(*args, **kwargs)
        self.input_columns = ["id", "transaction_date"]

    def get_extract_node(self, id: str) -> Node:
        extract_node = Node(
            id="n1",
            data=NodeData(
                type="extract",
                file_name="outside_working_hours.csv",
                file_path="tests/test_analytics/test_dna/data/outside_working_hours.csv",
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

    def test_owh_positive_without_timezone(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/outside_working_hours.csv"
        )
        input_column = "transaction_date"
        output_columns = self.input_columns + ["owh_transaction_date"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="owh",
            column_name=input_column,
            start_time="09:00",
            end_time="17:00",
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
        self.assertEqual(len(output_resource.read_rows()), 6)

        outside_working_hours_column = [
            row.to_dict()["owh_transaction_date"] for row in output_resource.read_rows()
        ]
        self.assertEqual(outside_working_hours_column.count("Y"), 4)
        self.assertEqual(outside_working_hours_column.count("N"), 2)

        os.remove(output_path)

    def test_owh_positive_with_timezone(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/outside_working_hours_timezone.csv"
        )
        input_column = "transaction_date"
        output_columns = self.input_columns + ["owh_transaction_date"]

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "outside_working_hours_time_zone.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/outside_working_hours_time_zone.csv"
        )

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="owh",
            column_name=input_column,
            start_time="09:00",
            end_time="17:00",
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
        self.assertEqual(len(output_resource.read_rows()), 6)

        outside_working_hours_column = [
            row.to_dict()["owh_transaction_date"] for row in output_resource.read_rows()
        ]
        self.assertEqual(outside_working_hours_column.count("Y"), 4)
        self.assertEqual(outside_working_hours_column.count("N"), 2)

        os.remove(output_path)
