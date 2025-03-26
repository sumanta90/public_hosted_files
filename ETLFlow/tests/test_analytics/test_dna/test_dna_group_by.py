import unittest
from frictionless import Pipeline, steps
from frictionless.resources import TableResource
import json
from ebflow.analytics.analytics import DataAndAnalytics
from ebflow.analytics.analytics_schema import (
    AnalyticsPipeline,
    AggregationType,
    Edge,
    Node,
    NodeData,
    FilterOperations,
)
import os


class TestGroupByOperation(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGroupByOperation, self).__init__(*args, **kwargs)
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

    def test_single_group_by_count(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_single_group_by_count.csv"
        )
        input_column = "MANAGER_ID"
        output_columns = ["DEPARTMENT_ID", "count_MANAGER_ID"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["DEPARTMENT_ID"],
            aggregation_type="count",
            unique=False,
            input_columns=self.input_columns,
            output_columns=output_columns
        )
        print("1111111111111")
        transform_node = Node(id="n2", data=transform_node_data)

        edge1 = Edge(id="e1", source="n1", target="n2")
        edge2 = Edge(id="e2", source="n2", target="n3")

        pipeline = AnalyticsPipeline(
            nodes=[extract_node, transform_node, load_node],
            edges=[edge1, edge2],
        )
        print("100")
        self.assertIsInstance(pipeline, AnalyticsPipeline)
        print("200")
        analytics = DataAndAnalytics(pipeline=pipeline)
        self.assertIsInstance(analytics, DataAndAnalytics)

        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 10)

        fifth_rows = output_resource.read_rows()[4].to_dict()
        self.assertEqual(fifth_rows["DEPARTMENT_ID"], 50)
        self.assertEqual(fifth_rows["count_MANAGER_ID"], 22)

        os.remove(output_path)

    def test_single_group_by_count_unique(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_single_group_by_count_unique.csv"
        input_column = "MANAGER_ID"
        output_columns = ["DEPARTMENT_ID", "count_unique_MANAGER_ID"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["DEPARTMENT_ID"],
            aggregation_type="count",
            unique=True,
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
        self.assertEqual(len(output_resource.read_rows()), 10)

        fifth_rows = output_resource.read_rows()[4].to_dict()
        self.assertEqual(fifth_rows["DEPARTMENT_ID"], 50)
        self.assertEqual(fifth_rows["count_unique_MANAGER_ID"], 7)

        os.remove(output_path)

    def test_single_group_by_sum(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_single_group_by_sum.csv"
        )
        input_column = "SALARY"
        output_columns = ["DEPARTMENT_ID", "sum_SALARY"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["DEPARTMENT_ID"],
            aggregation_type="sum",
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
        self.assertEqual(len(output_resource.read_rows()), 10)

        third_row = output_resource.read_rows()[2].to_dict()
        self.assertEqual(third_row["DEPARTMENT_ID"], 30)
        self.assertEqual(third_row["sum_SALARY"], 24900)

        os.remove(output_path)

    def test_single_group_by_sum_on_string(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_single_group_by_sum_on_string.csv"
        input_column = "FIRST_NAME"
        output_columns = ["MANAGER_ID", "sum_FIRST_NAME"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID"],
            aggregation_type="sum",
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

    def test_single_group_by_avg(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_single_group_by_avg.csv"
        )
        input_column = "SALARY"
        output_columns = ["DEPARTMENT_ID", "avg_SALARY"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["DEPARTMENT_ID"],
            aggregation_type="avg",
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
        self.assertEqual(len(output_resource.read_rows()), 10)

        fifth_row = output_resource.read_rows()[4].to_dict()
        self.assertEqual(fifth_row["DEPARTMENT_ID"], 50)
        self.assertEqual(float(fifth_row["avg_SALARY"]), 3777.27273)

        os.remove(output_path)

    def test_single_group_by_avg_on_string(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_single_group_by_avg_on_string.csv"
        input_column = "FIRST_NAME"
        output_columns = ["MANAGER_ID", "avg_FIRST_NAME"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID"],
            aggregation_type="avg",
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

    def test_single_group_by_max(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_single_group_by_max.csv"
        )
        input_column = "SALARY"
        output_columns = ["DEPARTMENT_ID", "max_SALARY"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["DEPARTMENT_ID"],
            aggregation_type="max",
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
        self.assertEqual(len(output_resource.read_rows()), 10)

        third_row = output_resource.read_rows()[2].to_dict()
        self.assertEqual(third_row["DEPARTMENT_ID"], 30)
        self.assertEqual(third_row["max_SALARY"], 11000)

        os.remove(output_path)

    def test_single_group_by_max_on_string(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_single_group_by_max_on_string.csv"
        input_column = "FIRST_NAME"
        output_columns = ["MANAGER_ID", "max_FIRST_NAME"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID"],
            aggregation_type="max",
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
        self.assertEqual(len(output_resource.read_rows()), 14)

        third_row = output_resource.read_rows()[2].to_dict()
        self.assertEqual(third_row["MANAGER_ID"], 101)
        self.assertEqual(third_row["max_FIRST_NAME"], "Susan")

        os.remove(output_path)

    def test_single_group_by_min(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_single_group_by_min.csv"
        )
        input_column = "SALARY"
        output_columns = ["DEPARTMENT_ID", "min_SALARY"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["DEPARTMENT_ID"],
            aggregation_type="min",
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
        self.assertEqual(len(output_resource.read_rows()), 10)

        third_row = output_resource.read_rows()[2].to_dict()
        self.assertEqual(third_row["DEPARTMENT_ID"], 30)
        self.assertEqual(third_row["min_SALARY"], 2500)

        os.remove(output_path)

    def test_single_group_by_min_on_string(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_single_group_by_min_on_string.csv"
        input_column = "FIRST_NAME"
        output_columns = ["MANAGER_ID", "min_FIRST_NAME"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID"],
            aggregation_type="min",
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
        self.assertEqual(len(output_resource.read_rows()), 14)

        third_row = output_resource.read_rows()[2].to_dict()
        self.assertEqual(third_row["MANAGER_ID"], 101)
        self.assertEqual(third_row["min_FIRST_NAME"], "Hermann")

        os.remove(output_path)

    def test_multiple_group_by_count(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_multiple_group_by_count.csv"
        )
        input_column = "EMPLOYEE_ID"
        output_columns = ["MANAGER_ID", "DEPARTMENT_ID", "count_EMPLOYEE_ID"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID", "DEPARTMENT_ID"],
            aggregation_type="count",
            unique=False,
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
        self.assertEqual(len(output_resource.read_rows()), 22)

        fifth_rows = output_resource.read_rows()[4].to_dict()
        self.assertEqual(fifth_rows["DEPARTMENT_ID"], 50)
        self.assertEqual(fifth_rows["MANAGER_ID"], 100)
        self.assertEqual(fifth_rows["count_EMPLOYEE_ID"], 5)

        os.remove(output_path)

    def test_multiple_group_by_count_unique(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_multiple_group_by_count_unique.csv"
        input_column = "EMPLOYEE_ID"
        output_columns = ["MANAGER_ID", "DEPARTMENT_ID", "count_unique_EMPLOYEE_ID"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID", "DEPARTMENT_ID"],
            aggregation_type="count",
            unique=True,
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
        self.assertEqual(len(output_resource.read_rows()), 22)

        fifth_rows = output_resource.read_rows()[4].to_dict()
        self.assertEqual(fifth_rows["DEPARTMENT_ID"], 50)
        self.assertEqual(fifth_rows["MANAGER_ID"], 100)
        self.assertEqual(fifth_rows["count_unique_EMPLOYEE_ID"], 5)

        os.remove(output_path)

    def test_multiple_group_by_sum(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_multiple_group_by_sum.csv"
        )
        input_column = "SALARY"
        output_columns = ["MANAGER_ID", "DEPARTMENT_ID", "sum_SALARY"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID", "DEPARTMENT_ID"],
            aggregation_type="sum",
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
        self.assertEqual(len(output_resource.read_rows()), 22)

        fifth_rows = output_resource.read_rows()[4].to_dict()
        self.assertEqual(fifth_rows["DEPARTMENT_ID"], 50)
        self.assertEqual(fifth_rows["MANAGER_ID"], 100)
        self.assertEqual(fifth_rows["sum_SALARY"], 36400)

        os.remove(output_path)

    def test_multiple_group_by_max(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_multiple_group_by_max.csv"
        )
        input_column = "SALARY"
        output_columns = ["MANAGER_ID", "DEPARTMENT_ID", "max_SALARY"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID", "DEPARTMENT_ID"],
            aggregation_type="max",
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
        self.assertEqual(len(output_resource.read_rows()), 22)

        fifth_rows = output_resource.read_rows()[4].to_dict()
        self.assertEqual(fifth_rows["DEPARTMENT_ID"], 50)
        self.assertEqual(fifth_rows["MANAGER_ID"], 100)
        self.assertEqual(fifth_rows["max_SALARY"], 8200)

        os.remove(output_path)

    def test_multiple_group_by_min(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_multiple_group_by_min.csv"
        )
        input_column = "SALARY"
        output_columns = ["MANAGER_ID", "DEPARTMENT_ID", "min_SALARY"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="group_by",
            column_name=input_column,
            group_by=["MANAGER_ID", "DEPARTMENT_ID"],
            aggregation_type="min",
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
        self.assertEqual(len(output_resource.read_rows()), 22)

        fifth_rows = output_resource.read_rows()[4].to_dict()
        self.assertEqual(fifth_rows["DEPARTMENT_ID"], 50)
        self.assertEqual(fifth_rows["MANAGER_ID"], 100)
        self.assertEqual(fifth_rows["min_SALARY"], 5800)

        os.remove(output_path)
