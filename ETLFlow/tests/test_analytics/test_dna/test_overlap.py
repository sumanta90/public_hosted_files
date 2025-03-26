# import unittest
# from frictionless import Pipeline, steps
# from frictionless.resources import TableResource
# import json
# from ebflow.analytics.analytics import DataAndAnalytics
# from ebflow.analytics.analytics_schema import (
#     AnalyticsPipeline,
#     Edge,
#     Node,
#     NodeData,
#     FilterOperations,
# )
# import os


# class TestDnAOverlap(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super(TestDnAOverlap, self).__init__(*args, **kwargs)
#         self.input_columns = ["ID", "CATEGORY", "AMOUNT", "TOP_LIST"]

#     def get_extract_node(self, id: str, file_number=1) -> Node:
#         if file_number == 1:
#             extract_node = Node(
#                 id="n1",
#                 data=NodeData(
#                     type="extract",
#                     file_name="overlap_first.csv",
#                     file_path="tests/test_analytics/test_dna/data/overlap_first.csv",
#                     ingestion="ingestion",
#                     cdm_file="cdm_file",
#                 ),
#             )
#         else:
#             extract_node = Node(
#                 id="n2",
#                 data=NodeData(
#                     type="extract",
#                     file_name="overlap_second.csv",
#                     file_path="tests/test_analytics/test_dna/data/overlap_second.csv",
#                     ingestion="ingestion",
#                     cdm_file="cdm_file",
#                 ),
#             )
#         return extract_node

#     def get_load_node(self, id: str) -> Node:
#         load_node = Node(
#             id=id,
#             data=NodeData(
#                 type="load",
#                 file_name="output",
#                 file_path="path/to/output",
#             ),
#         )
#         return load_node

#     def check_audit_trail(self, audit_trail):
#         last_trail = audit_trail[-1]
#         self.assertEqual(type(audit_trail), list)
#         self.assertGreater(len(audit_trail), 0)
#         self.assertIn("timestamp", last_trail.keys())
#         self.assertIn("message", last_trail.keys())
#         self.assertIn("Time taken:", last_trail["message"])

#     def check_error_audit_trail(self, audit_trail):
#         last_trail = audit_trail[-1]
#         second_last_trail = audit_trail[-2]
#         self.assertEqual(type(audit_trail), list)
#         self.assertGreater(len(audit_trail), 0)
#         self.assertIn("timestamp", last_trail.keys())
#         self.assertIn("message", last_trail.keys())
#         self.assertEqual(
#             "EBFlow Analytics | Data & Analytics pipeline process Failed",
#             last_trail["message"],
#         )
#         self.assertIn("Error", second_last_trail["message"])

#     def test_basic_overlap_files(self):
#         output_path = "tests/test_analytics/test_dna/data/temp/overlap.csv"
#         input_column = "TOP_LIST"
#         output_columns = [f"compare_{input_column}"]
#         output_data = [{f"count_{input_column}": 49}]

#         extract_node_1 = self.get_extract_node(id="n1")
#         extract_node_2 = self.get_extract_node(id="n2", file_number=2)

#         load_node = self.get_load_node(id="n4")
#         load_node.data.file_path = output_path
#         load_node.data.file_name = output_path.split("/")[-1]

#         transform_node_data = NodeData(
#             id="n3",
#             type="transform",
#             operation_name="overlap",
#             input_columns=self.input_columns,
#             output_columns=output_columns,
#             datasource=["n1","n2"],
#         )
#         transform_node = Node(id="n3", data=transform_node_data)

#         edge1 = Edge(id="e1", source="n1", target="n3")
#         edge2 = Edge(id="e2", source="n2", target="n3")
#         edge3 = Edge(id="e3", source="n3", target="n4")

#         pipeline = AnalyticsPipeline(
#             nodes=[extract_node_1, extract_node_2, transform_node, load_node],
#             edges=[edge1, edge2, edge3],
#         )
#         self.assertIsInstance(pipeline, AnalyticsPipeline)

#         analytics = DataAndAnalytics(pipeline=pipeline)
#         self.assertIsInstance(analytics, DataAndAnalytics)

#         analytics.process()
#         self.assertTrue(os.path.exists(output_path))

#         audit_trail = analytics.audit_trail
#         self.check_audit_trail(audit_trail)

#         output_resource = TableResource(path=output_path)
#         output_resource.infer()

#         self.assertEqual(len(output_resource.read_rows()), 4)
        
#         os.remove(output_path)
        
#     def test_basic_overlap_files_with_filter(self):
#         output_path = "tests/test_analytics/test_dna/data/temp/overlap.csv"
#         input_column = "TOP_LIST"
#         output_columns = [f"compare_{input_column}"]
#         output_data = [{f"count_{input_column}": 49}]

#         extract_node_1 = self.get_extract_node(id="n1")
#         extract_node_2 = self.get_extract_node(id="n2", file_number=2)

#         load_node = self.get_load_node(id="n5")
#         load_node.data.file_path = output_path
#         load_node.data.file_name = output_path.split("/")[-1]
#         transform_node_data = NodeData(
#             id="n3",
#             type="transform",
#             operation_name="overlap",
#             input_columns=self.input_columns,
#             output_columns=output_columns,
#             datasource=["n4","n2"],
#         )
#         transform_node = Node(id="n3", data=transform_node_data)
        
#         transform_node_filter_data_2 = NodeData(
#             id="n4",
#             type="transform",
#             operation_name="filter",
#             operation_value="AAA",
#             operation_formula=FilterOperations(
#                 title="Not Equal to",
#                 value="!=",
#             ),
#             unique=True,
#             column_name="ACCOUNT_NAME",
#             input_columns=self.input_columns,
#             output_columns=self.input_columns,
#         )
#         transform_node_filter_2 = Node(id="n4", data=transform_node_filter_data_2)


#         edge1 = Edge(id="e1", source="n1", target="n4")
#         edge2 = Edge(id="e2", source="n4", target="n3")
#         edge3 = Edge(id="e3", source="n2", target="n3")
#         edge4 = Edge(id="e3", source="n3", target="n5")

#         pipeline = AnalyticsPipeline(
#             nodes=[extract_node_1, extract_node_2, transform_node, transform_node_filter_2, load_node],
#             edges=[edge1, edge2, edge3, edge4],
#         )
#         self.assertIsInstance(pipeline, AnalyticsPipeline)

#         analytics = DataAndAnalytics(pipeline=pipeline)
#         self.assertIsInstance(analytics, DataAndAnalytics)

#         analytics.process()
#         self.assertTrue(os.path.exists(output_path))

#         audit_trail = analytics.audit_trail
#         self.check_audit_trail(audit_trail)

#         output_resource = TableResource(path=output_path)
#         output_resource.infer()
#         print("Final Output: ", output_resource.to_view())
#         self.assertEqual(len(output_resource.read_rows()), 3)
        
#         os.remove(output_path)
        
#     def test_basic_overlap_files_with_count(self):
#         output_path = "tests/test_analytics/test_dna/data/temp/overlap.csv"
#         input_column = "TOP_LIST"
#         output_columns = [f"compare_{input_column}"]
#         output_data = [{f"count_{input_column}": 49}]

#         extract_node_1 = self.get_extract_node(id="n1")
#         extract_node_2 = self.get_extract_node(id="n2", file_number=2)

        
#         transform_node_data = NodeData(
#             id="n3",
#             type="transform",
#             operation_name="overlap",
#             input_columns=self.input_columns,
#             output_columns=output_columns,
#             datasource=["n4","n2"],
#         )
#         transform_node = Node(id="n3", data=transform_node_data)
        
#         transform_node_filter_data_2 = NodeData(
#             id="n4",
#             type="transform",
#             operation_name="filter",
#             operation_value="AAA",
#             operation_formula=FilterOperations(
#                 title="Not Equal to",
#                 value="!=",
#             ),
#             unique=True,
#             column_name="ACCOUNT_NAME",
#             input_columns=self.input_columns,
#             output_columns=self.input_columns,
#         )
#         transform_node_filter_2 = Node(id="n4", data=transform_node_filter_data_2)
        
#         transform_node_count_data = NodeData(
#                 id="n5",
#                 type="transform",
#                 operation_name="count",
#                 unique=True,
#                 column_name={'title': 'D_ACCOUNT_NAME', 'value': 'D_ACCOUNT_NAME'},
#                 input_columns=self.input_columns,
#                 output_columns=output_columns
#             )
#         transform_node_count = Node(id="n5", data=transform_node_count_data)
        
#         load_node = self.get_load_node(id="n6")
#         load_node.data.file_path = output_path
#         load_node.data.file_name = output_path.split("/")[-1]


#         edge1 = Edge(id="e1", source="n1", target="n4")
#         edge2 = Edge(id="e2", source="n4", target="n3")
#         edge3 = Edge(id="e3", source="n2", target="n3")
#         edge4 = Edge(id="e4", source="n3", target="n5")
#         edge5 = Edge(id="e5", source="n5", target="n6")

#         pipeline = AnalyticsPipeline(
#             nodes=[extract_node_1, extract_node_2, transform_node, transform_node_filter_2, transform_node_count, load_node],
#             edges=[edge1, edge2, edge3, edge4, edge5],
#         )
#         self.assertIsInstance(pipeline, AnalyticsPipeline)

#         analytics = DataAndAnalytics(pipeline=pipeline)
#         self.assertIsInstance(analytics, DataAndAnalytics)

#         analytics.process()
#         self.assertTrue(os.path.exists(output_path))

#         audit_trail = analytics.audit_trail
#         self.check_audit_trail(audit_trail)

#         output_resource = TableResource(path=output_path)
#         output_resource.infer()
#         print("Final Output: ", output_resource.to_view(), output_resource.read_rows())
#         self.assertEqual(output_resource.read_rows(), [{'count_unique_D_ACCOUNT_NAME': 3}])
#         os.remove(output_path)