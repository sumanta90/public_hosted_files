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

    def test_filter_date_equal_to(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_date_greater_than.csv"
        )
        input_column = "JOINED_DATE"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        JOINED_DATE = "2010-05-18T11:40:22"
        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value=JOINED_DATE,
            operation_formula=FilterOperations(
                title="Equal to",
                value="==",
            ),
            unique=True,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
        )
        transform_node = Node(id="n2", data=transform_node_data)

        edge1 = Edge(id="e1", source="n1", target="n2")
        edge2 = Edge(id="n2", source="n2", target="n3")

        pipeline = AnalyticsPipeline(
            nodes=[extract_node, transform_node, load_node],
            edges=[edge1, edge2],
        )
        analytics = DataAndAnalytics(pipeline=pipeline)
        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()
        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 1)

        os.remove(output_path)

    def test_filter_date_greater_than(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_date_greater_than.csv"
        )
        input_column = "JOINED_DATE"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        JOINED_DATE = "2010-05-18T11:40:22"
        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value=JOINED_DATE,
            operation_formula=FilterOperations(
                title="Greater than",
                value=">",
            ),
            unique=True,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
        )
        transform_node = Node(id="n2", data=transform_node_data)

        edge1 = Edge(id="e1", source="n1", target="n2")
        edge2 = Edge(id="n2", source="n2", target="n3")

        pipeline = AnalyticsPipeline(
            nodes=[extract_node, transform_node, load_node],
            edges=[edge1, edge2],
        )

        analytics = DataAndAnalytics(pipeline=pipeline)
        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 48)

        os.remove(output_path)

    def test_filter_date_greater_than_equal_to(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_date_greater_than.csv"
        )
        input_column = "JOINED_DATE"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        JOINED_DATE = "2010-05-18T11:40:22"
        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value=JOINED_DATE,
            operation_formula=FilterOperations(
                title="Greater than equal ot",
                value=">=",
            ),
            unique=True,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
        )
        transform_node = Node(id="n2", data=transform_node_data)

        edge1 = Edge(id="e1", source="n1", target="n2")
        edge2 = Edge(id="n2", source="n2", target="n3")

        pipeline = AnalyticsPipeline(
            nodes=[extract_node, transform_node, load_node],
            edges=[edge1, edge2],
        )

        analytics = DataAndAnalytics(pipeline=pipeline)
        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 49)

        os.remove(output_path)

    def test_filter_date_less_than(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_date_greater_than.csv"
        )
        input_column = "JOINED_DATE"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        JOINED_DATE = "2010-05-18T11:40:22"
        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value=JOINED_DATE,
            operation_formula=FilterOperations(
                title="less than",
                value="<",
            ),
            unique=True,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
        )
        transform_node = Node(id="n2", data=transform_node_data)

        edge1 = Edge(id="e1", source="n1", target="n2")
        edge2 = Edge(id="n2", source="n2", target="n3")

        pipeline = AnalyticsPipeline(
            nodes=[extract_node, transform_node, load_node],
            edges=[edge1, edge2],
        )

        analytics = DataAndAnalytics(pipeline=pipeline)
        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 0)

        os.remove(output_path)

    def test_filter_date_less_than_equal_to(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_date_greater_than.csv"
        )
        input_column = "JOINED_DATE"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        JOINED_DATE = "2010-05-18T11:40:22"
        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value=JOINED_DATE,
            operation_formula=FilterOperations(
                title="less than equal to",
                value="<=",
            ),
            unique=True,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
        )
        transform_node = Node(id="n2", data=transform_node_data)

        edge1 = Edge(id="e1", source="n1", target="n2")
        edge2 = Edge(id="n2", source="n2", target="n3")

        pipeline = AnalyticsPipeline(
            nodes=[extract_node, transform_node, load_node],
            edges=[edge1, edge2],
        )

        analytics = DataAndAnalytics(pipeline=pipeline)
        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 1)

        os.remove(output_path)

    def test_filter_date_not_equal_to(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_date_greater_than.csv"
        )
        input_column = "JOINED_DATE"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        JOINED_DATE = "2010-05-18T11:40:22"
        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value=JOINED_DATE,
            operation_formula=FilterOperations(
                title="not equal to",
                value="!=",
            ),
            unique=True,
            column_name=input_column,
            input_columns=self.input_columns,
            output_columns=output_columns,
        )
        transform_node = Node(id="n2", data=transform_node_data)

        edge1 = Edge(id="e1", source="n1", target="n2")
        edge2 = Edge(id="n2", source="n2", target="n3")

        pipeline = AnalyticsPipeline(
            nodes=[extract_node, transform_node, load_node],
            edges=[edge1, edge2],
        )

        analytics = DataAndAnalytics(pipeline=pipeline)
        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 48)

        os.remove(output_path)

    def test_filter_operation_greater_than_numeric(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_greater_than_numeric.csv"
        )
        input_column = "DEPARTMENT_ID"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="100",
            operation_formula=FilterOperations(
                title="Greater Than",
                value=">",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 2)

        os.remove(output_path)

    def test_filter_operation_greater_than_equal_to_numeric(self):
        output_path = "tests/test_analytics/test_dna/data/temp/filter_greater_than_equal_to_numeric.csv"
        input_column = "DEPARTMENT_ID"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="100",
            operation_formula=FilterOperations(
                title="Greater Than Equal to",
                value=">=",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 8)

        os.remove(output_path)

    def test_filter_operation_less_than_numeric(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_less_than_numeric.csv"
        )
        input_column = "DEPARTMENT_ID"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="20",
            operation_formula=FilterOperations(
                title="Less Than",
                value="<",
            ),
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

        os.remove(output_path)

    def test_filter_operation_less_than_equal_to_numeric(self):
        output_path = "tests/test_analytics/test_dna/data/temp/filter_less_than_equal_to_numeric.csv"
        input_column = "DEPARTMENT_ID"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="20",
            operation_formula=FilterOperations(
                title="Less Than Equal to",
                value="<=",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 3)

        os.remove(output_path)

    def test_filter_operation_equal_to_numeric(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_equal_to_numeric.csv"
        )
        input_column = "DEPARTMENT_ID"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="50",
            operation_formula=FilterOperations(
                title="Equal to",
                value="==",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 22)

        os.remove(output_path)

    def test_filter_operation_equal_not_to_numeric(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_not_equal_to_numeric.csv"
        )
        input_column = "DEPARTMENT_ID"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="MANAGER_ID",
            operation_formula=FilterOperations(
                title="Not Equal to",
                value="!=",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 49)

        os.remove(output_path)

    def test_filter_operation_greater_than_string(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_greater_than_numeric.csv"
        )
        input_column = "FIRST_NAME"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="Adam",
            operation_formula=FilterOperations(
                title="Greater Than",
                value=">",
            ),
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

        analytics = DataAndAnalytics(pipeline=pipeline)
        self.assertIsInstance(analytics, DataAndAnalytics)

        analytics.process()
        self.assertTrue(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_audit_trail(audit_trail)

        output_resource = TableResource(path=output_path)
        output_resource.infer()

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(len(output_resource.read_rows()), 48)

        os.remove(output_path)

    def test_filter_operation_greater_than_equal_to_string(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_less_than_string.csv"
        )
        input_column = "FIRST_NAME"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="Adam",
            operation_formula=FilterOperations(
                title="Greater Than Equal to",
                value=">=",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 49)

        os.remove(output_path)

    def test_filter_operation_less_than_string(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_greater_than_numeric.csv"
        )
        input_column = "FIRST_NAME"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="Bruce",
            operation_formula=FilterOperations(
                title="Less Than",
                value="<",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 3)

        os.remove(output_path)

    def test_filter_operation_less_than_equal_to_string(self):
        output_path = "tests/test_analytics/test_dna/data/temp/filter_less_than_equal_to_string.csv"
        input_column = "FIRST_NAME"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="Bruce",
            operation_formula=FilterOperations(
                title="Less Than Equal to",
                value="<=",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 4)

        os.remove(output_path)

    def test_filter_operation_equal_to_string(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_equal_to_string.csv"
        )
        input_column = "FIRST_NAME"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="Adam",
            operation_formula=FilterOperations(
                title="Equal to",
                value="==",
            ),
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

        os.remove(output_path)

    def test_filter_operation_equal_not_to_string(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_not_equal_to_string.csv"
        )
        input_column = "FIRST_NAME"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="Adam",
            operation_formula=FilterOperations(
                title="Not Equal to",
                value="!=",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 48)

        os.remove(output_path)

    def test_filter_operation_equal_not_to_string_on_int(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/filter_not_equal_to_string.csv"
        )
        input_column = "DEPARTMENT_ID"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="Adam",
            operation_formula=FilterOperations(
                title="Not Equal to",
                value="!=",
            ),
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
        self.assertEqual(len(output_resource.read_rows()), 49)

        os.remove(output_path)

    def test_filter_operation_on_datetime(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/test_filter_operation_on_dates.csv"
        )
        input_column = "birth_date"
        input_columns = [
            "employee_id",
            "first_name",
            "last_name",
            "email",
            "birth_date",
            "joining_date",
            "salary",
            "job_id",
            "manager_id",
            "department_id",
        ]
        output_columns = [col for col in input_columns]

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "filter_data.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/filter_data.csv"
        )

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="1992-03-01T00:00:00",
            operation_formula=FilterOperations(
                title="Less Than",
                value="<",
            ),
            column_name=input_column,
            input_columns=input_columns,
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
        self.assertEqual(len(output_resource.read_rows()), 3)

        os.remove(output_path)

    def test_filter_operation_on_datetime_with_column_value(self):
        output_path = "tests/test_analytics/test_dna/data/temp/test_filter_operation_on_datetime_with_column_value.csv"
        input_column = "birth_date"
        input_columns = [
            "employee_id",
            "first_name",
            "last_name",
            "email",
            "birth_date",
            "joining_date",
            "salary",
            "job_id",
            "manager_id",
            "department_id",
        ]
        output_columns = [col for col in input_columns]

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "filter_data.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/filter_data.csv"
        )

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="joining_date",
            operation_formula=FilterOperations(
                title="Greater Than",
                value=">",
            ),
            column_name=input_column,
            input_columns=input_columns,
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

        os.remove(output_path)

    def test_department_id_not_equal_to(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/department_id_not_equal_to.csv"
        )
        input_column = "DEPARTMENT_ID"
        output_columns = [col for col in self.input_columns]

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "dna_test_file.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/dna_test_file.csv"
        )

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="filter",
            operation_value="10",
            operation_formula=FilterOperations(
                title="Not Equal to",
                value="==",
            ),
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

        os.remove(output_path)
