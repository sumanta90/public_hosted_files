import unittest
from frictionless import Pipeline, steps
from frictionless.resources import TableResource
import json
from decimal import Decimal
from ebflow.analytics.analytics import DataAndAnalytics
from ebflow.analytics.analytics_schema import (
    AnalyticsPipeline,
    Edge,
    Node,
    NodeData,
    FilterOperations,
    MultipleCalculationData,
)
import os


class TestDNAOperationsMultipleCalculation(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDNAOperationsMultipleCalculation, self).__init__(*args, **kwargs)
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

    def test_multiple_calculation_operation_with_minus(self):
        output_path = "tests/test_analytics/test_dna/data/temp/multiple_calculation.csv"
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("4078"),
            Decimal("303"),
            Decimal("-9411.75"),
            Decimal("-5155.0"),
            Decimal("-1247.0"),
            Decimal("-1331"),
            Decimal("-1451"),
            Decimal("-1977.5"),
            Decimal("-804.5"),
        ]

        data = MultipleCalculationData(
            expression="A + B - (C * D)",
            column={"A": "A", "B": "B", "C": "C", "D": "D"},
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)

    def test_multiple_calculation_operation_with_multiplication(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/multiple_calculation_two.csv"
        )
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("41868"),
            Decimal("6290"),
            Decimal("-388.0"),
            Decimal("400047.5"),
            Decimal("17634552.0"),
            Decimal("197769"),
            Decimal("198148"),
            Decimal("35916.0"),
            Decimal("8550.0"),
        ]

        data = MultipleCalculationData(
            expression="(A * B) - (C * D)",
            column={"A": "A", "B": "B", "C": "C", "D": "D"},
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)

    def test_multiple_calculation_operation_with_division(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/multiple_calculation_three.csv"
        )
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("318.1818181818182"),
            Decimal("30.952380952380953"),
            Decimal("0.9595991149290641"),
            Decimal("43.32187252049722"),
            Decimal("1828.7935323383085"),
            Decimal("89.64589870013447"),
            Decimal("85.24659863945578"),
            Decimal("15.511515151515152"),
            Decimal("9.55"),
        ]

        data = MultipleCalculationData(
            expression="(A * B) / (C * D)",
            column={"A": "A", "B": "B", "C": "C", "D": "D"},
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)

    def test_multiple_calculation_operation_with_addition(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/multiple_calculation_four.csv"
        )
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("4322"),
            Decimal("697"),
            Decimal("9602.75"),
            Decimal("13555.0"),
            Decimal("9647.0"),
            Decimal("2331"),
            Decimal("2451"),
            Decimal("2168.5"),
            Decimal("995.5"),
        ]

        data = MultipleCalculationData(
            expression="(A - B) + (C * D)",
            column={"A": "A", "B": "B", "C": "C", "D": "D"},
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)

    def test_multiple_calculation_operation_with_squares(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/multiple_calculation_five.csv"
        )
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("-4190"),
            Decimal("-487"),
            Decimal("1.0"),
            Decimal("-18461.25"),
            Decimal("4.5"),
            Decimal("7400"),
            Decimal("7326"),
            Decimal("-22681.0"),
            Decimal("-405.0"),
        ]

        data = MultipleCalculationData(
            expression="(A - B) * (C - D)",
            column={"A": "A", "B": "B", "C": "C", "D": "D"},
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)

    def test_multiple_calculation_operation_with_combination(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/multiple_calculation_six.csv"
        )
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("4078"),
            Decimal("303"),
            Decimal("-9411.75"),
            Decimal("-5155.0"),
            Decimal("-1247.0"),
            Decimal("-1331"),
            Decimal("-1451"),
            Decimal("-1977.5"),
            Decimal("-804.5"),
        ]

        data = MultipleCalculationData(
            expression="A + B - C * D", column={"A": "A", "B": "B", "C": "C", "D": "D"}
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)

    def test_multiple_calculation_operation_with_product(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/multiple_calculation_seven.csv"
        )
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("96830"),
            Decimal("14877"),
            Decimal("37632.0"),
            Decimal("835863.75"),
            Decimal("1650796.5"),
            Decimal("108000"),
            Decimal("109922"),
            Decimal("61690.0"),
            Decimal("21505.0"),
        ]

        data = MultipleCalculationData(
            expression="(A + B) * (C + D)",
            column={"A": "A", "B": "B", "C": "C", "D": "D"},
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)

    def test_multiple_calculation_operation_with_quotient(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/multiple_calculation_eight.csv"
        )
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("420.0"),
            Decimal("38.46153846153846"),
            Decimal("0.9896373056994818"),
            Decimal("43.07692307692308"),
            Decimal("0.9997619614377529"),
            Decimal("1.25"),
            Decimal("1.2468827930174564"),
            Decimal("0.23756218905472637"),
            Decimal("0.955"),
        ]

        data = MultipleCalculationData(
            expression="A/B", column={"A": "A", "B": "B", "C": "C", "D": "D"}
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]
        print("Output Rows:", multiple_calculation_values)

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)

    def test_multiple_calculation_operation_with_exponentiation(self):
        output_path = (
            "tests/test_analytics/test_dna/data/temp/multiple_calculation_nine.csv"
        )
        output_columns = ["A", "B", "C", "D", "maths_A_B_C_D"]
        load_node = self.get_load_node(id="n3")

        expected_output = [
            Decimal("4200"),
            Decimal("1"),
            Decimal("89126.82360529152"),
            Decimal("17640000"),
            Decimal("17640000"),
            Decimal("500"),
            Decimal("500"),
            Decimal("9120.25"),
            Decimal("95.5"),
        ]

        data = MultipleCalculationData(
            expression="A^B", column={"A": "A", "B": "B", "C": "C", "D": "D"}
        )

        extract_node = self.get_extract_node(id="n1")
        extract_node.data.file_name = "mc_function_2.csv"
        extract_node.data.file_path = (
            "tests/test_analytics/test_dna/data/mc_function_2.csv"
        )

        load_node.data.file_path = output_path
        load_node.data.file_name = output_path.split("/")[-1]

        transform_node_data = NodeData(
            id="n2",
            type="transform",
            operation_name="multiple_calculation",
            unique=True,
            data=data,
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

        rows = output_resource.read_rows()
        multiple_calculation_values = [row["maths_A_B_C_D"] for row in rows]
        print("Output Rows:", multiple_calculation_values)

        self.assertEqual(output_resource.header, output_columns)
        self.assertEqual(multiple_calculation_values, expected_output)

        for actual, expected in zip(multiple_calculation_values, expected_output):
            self.assertEqual(actual, expected)

        os.remove(output_path)
