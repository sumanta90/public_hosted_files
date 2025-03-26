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

    def test_operation_sum(self):
        output_path = "tests/test_analytics/test_dna/data/temp/sum_salary.csv"
        input_column = "SALARY"
        output_columns = [f"sum_{input_column}"]
        output_data = [{f"sum_{input_column}": 306616}]
        load_node = self.get_load_node(id="n3")

        extract_node = self.get_extract_node(id="n1")

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="sum",
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

    def test_operation_sum_negative(self):
        output_path = "tests/test_analytics/test_dna/data/temp/sum_error.csv"
        input_column = "FIRST_NAME"
        output_columns = [f"sum_{input_column}"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="sum",
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
        analytics = DataAndAnalytics(pipeline=pipeline)
        self.assertIsInstance(pipeline, AnalyticsPipeline)

        with self.assertRaises(Exception):
            analytics.process()
        self.assertFalse(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_error_audit_trail(audit_trail)

    def test_operation_avg(self):
        output_path = "tests/test_analytics/test_dna/data/temp/avg_salary.csv"
        input_column = "SALARY"
        output_columns = [f"avg_{input_column}"]
        output_data = [{f"avg_{input_column}": 6257.46939}]
        load_node = self.get_load_node(id="n3")

        extract_node = self.get_extract_node(id="n1")

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="avg",
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
        self.assertEqual(
            float(output_resource.read_rows()[0][f"avg_{input_column}"]),
            output_data[0][f"avg_{input_column}"],
        )

        os.remove(output_path)

    def test_operation_avg_negative(self):
        output_path = "tests/test_analytics/test_dna/data/temp/avg_error.csv"
        input_column = "FIRST_NAME"
        output_columns = [f"avg_{input_column}"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="avg",
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
        analytics = DataAndAnalytics(pipeline=pipeline)
        self.assertIsInstance(pipeline, AnalyticsPipeline)

        with self.assertRaises(Exception):
            analytics.process()
        self.assertFalse(os.path.exists(output_path))

        audit_trail = analytics.audit_trail
        self.check_error_audit_trail(audit_trail)

    def test_duplicate_operation(self):
        output_path = "tests/test_analytics/test_dna/data/temp/duplicate.csv"
        output_columns = self.input_columns[:-1] + ["is_duplicate"]

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "dna_test_file_duplicate.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/dna_test_file_duplicate.csv"
        )

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="duplicate",
            unique=True,
            input_columns=self.input_columns[:-1],
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
        self.assertEqual(len(output_resource.read_rows()), 50)
        self.assertEqual(output_resource.read_rows()[-1]["is_duplicate"], "Y")

        is_duplicate_column = [
            row.to_dict()["is_duplicate"] for row in output_resource.read_rows()
        ]
        self.assertEqual(is_duplicate_column.count("Y"), 1)
        self.assertEqual(is_duplicate_column.count("N"), 49)

        os.remove(output_path)

    def test_duplicate_operation_no_duplicates(self):
        output_path = "tests/test_analytics/test_dna/data/temp/duplicate.csv"
        output_columns = self.input_columns + ["is_duplicate"]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="duplicate",
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
        self.assertEqual(len(output_resource.read_rows()), 49)

        is_duplicate_column = [
            row.to_dict()["is_duplicate"] for row in output_resource.read_rows()
        ]
        self.assertEqual(is_duplicate_column.count("Y"), 0)
        self.assertEqual(is_duplicate_column.count("N"), 49)

        os.remove(output_path)

    def test_operation_max(self):
        output_path = "tests/test_analytics/test_dna/data/temp/max_salary.csv"
        input_column = "SALARY"
        output_columns = [f"max_{input_column}"]
        output_data = [{f"max_{input_column}": 24000}]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="max",
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

    def test_operation_max_string(self):
        output_path = "tests/test_analytics/test_dna/data/temp/max_first_name.csv"
        input_column = "FIRST_NAME"
        output_columns = [f"max_{input_column}"]
        output_data = [{f"max_{input_column}": "William"}]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="max",
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

    def test_operation_min(self):
        output_path = "tests/test_analytics/test_dna/data/temp/sum_salary.csv"
        input_column = "SALARY"
        output_columns = [f"min_{input_column}"]
        output_data = [{f"min_{input_column}": 2100}]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="min",
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

    def test_operation_min_string(self):
        output_path = "tests/test_analytics/test_dna/data/temp/min_first_name.csv"
        input_column = "FIRST_NAME"
        output_columns = [f"min_{input_column}"]
        output_data = [{f"min_{input_column}": "Adam"}]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="min",
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

    def test_remove_column_operation(self):
        output_path = "tests/test_analytics/test_dna/data/temp/remove_first_name.csv"
        input_column = ["FIRST_NAME", "LAST_NAME"]
        output_columns = [col for col in self.input_columns if col not in input_column]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="delete_column",
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

    def test_remove_column_operation_non_existing_column(self):
        output_path = "tests/test_analytics/test_dna/data/temp/remove_abc.csv"
        input_column = ["ABC"]
        output_columns = [col for col in self.input_columns if col not in input_column]

        extract_node = self.get_extract_node(id="n1")

        load_node = self.get_load_node(id="n3")
        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="delete_column",
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

    def test_net_operation_functionality_debit_credit(self):
        output_path = "tests/test_analytics/test_dna/data/temp/net_debit_credit.csv"
        amount_column = "amount"
        dci_column = "amountCreditDebitIndicator"
        output_columns = [
            "amount",
            "amountCreditDebitIndicator",
            "x",
            "net_amount_amountCreditDebitIndicator",
        ]
        net = 4200.0
        load_node = self.get_load_node(id="n3")

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "net_function_calc.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/net_function_calc.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="net",
            unique=True,
            dci_column=dci_column,
            column_name=amount_column,
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
        self.assertEqual(len(output_resource.read_rows()), 9)
        self.assertEqual(
            float(
                output_resource.read_rows()[0]["net_amount_amountCreditDebitIndicator"]
            ),
            net,
        )

        os.remove(output_path)
