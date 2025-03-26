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
        self.input_columns = ["id", "date"]

    def get_extract_node(self, id: str) -> Node:
        extract_node = Node(
            id="n1",
            data=NodeData(
                type="extract",
                file_name="date_operations.csv",
                file_path="tests/test_analytics/test_dna/data/date_operations.csv",
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

    def test_nwd_positive_weekends_only(self):
        output_path = "tests/test_analytics/test_dna/data/temp/non_working_days.csv"
        input_column = "date"
        output_columns = self.input_columns + ["nwd_date"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="nwd",
            column_name=input_column,
            weekdays=[5, 6],
            custom_dates=[],
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
        self.assertEqual(len(output_resource.read_rows()), 92)

        non_working_days_column = [
            row.to_dict()["nwd_date"] for row in output_resource.read_rows()
        ]
        self.assertEqual(non_working_days_column.count("Y"), 26)
        self.assertEqual(non_working_days_column.count("N"), 66)

        os.remove(output_path)

    def test_nwd_positive_custom_dates(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/non_working_days_custom_dates.csv"
        )
        input_column = "date"
        output_columns = self.input_columns + ["nwd_date"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="nwd",
            column_name=input_column,
            weekdays=[],
            custom_dates=["2023-12-01"],
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
        self.assertEqual(len(output_resource.read_rows()), 92)

        non_working_days_column = [
            row.to_dict()["nwd_date"] for row in output_resource.read_rows()
        ]
        self.assertEqual(non_working_days_column.count("Y"), 1)
        self.assertEqual(non_working_days_column.count("N"), 91)

        os.remove(output_path)

    def test_nwd_negative_custom_dates(self):
        output_path = "tests/test_analytics/test_dna/data/temp/non_working_days_custom_date_negative.csv"
        input_column = "date"
        output_columns = self.input_columns + ["nwd_date"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="nwd",
            column_name=input_column,
            weekdays=[],
            custom_dates=["202311-12-01"],
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

        with self.assertRaises(Exception):
            analytics.process()
        self.assertFalse(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_error_audit_trail(audit_trail)

    def test_nwd_positive_weekend_custom_dates(self):
        output_path = "tests/test_analytics/test_dna/data/temp/non_working_days_weekend_custom_dates.csv"
        input_column = "date"
        output_columns = self.input_columns + ["nwd_date"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="nwd",
            column_name=input_column,
            weekdays=[5, 6],
            custom_dates=["2023-12-01"],
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
        self.assertEqual(len(output_resource.read_rows()), 92)

        non_working_days_column = [
            row.to_dict()["nwd_date"] for row in output_resource.read_rows()
        ]
        self.assertEqual(non_working_days_column.count("Y"), 27)
        self.assertEqual(non_working_days_column.count("N"), 65)

        os.remove(output_path)

    def test_nwd_positive_all_days_selected(self):
        output_path = "tests/test_analytics/test_dna/data/temp/non_working_days_all.csv"
        input_column = "date"
        output_columns = self.input_columns + ["nwd_date"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="nwd",
            column_name=input_column,
            weekdays=[0, 1, 2, 3, 4, 5, 6],
            custom_dates=[],
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
        self.assertEqual(len(output_resource.read_rows()), 92)

        non_working_days_column = [
            row.to_dict()["nwd_date"] for row in output_resource.read_rows()
        ]
        self.assertEqual(non_working_days_column.count("Y"), 92)
        self.assertEqual(non_working_days_column.count("N"), 0)

        os.remove(output_path)
