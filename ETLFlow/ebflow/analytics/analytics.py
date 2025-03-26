import copy
import enum
from io import BytesIO
from datetime import datetime
import os
from typing import List, Dict, Any
import networkx as nx
import matplotlib.pyplot as plt
from frictionless import Resource, Pipeline, transform
from frictionless.resources import TableResource
from frictionless_azureblob import AzureBlobControl
from ebflow.analytics.analytics_schema import AnalyticsPipeline, Node, Edge, NodeData
from ebflow.analytics.dna_step_generation import DNATransformStep
from ebflow.utils.utils import get_node_label


FAILED_MESSAGE = "EBFlow Analytics | Data & Analytics pipeline process Failed"
FAILED_ERROR = "Error in processing pipeline"


class PipelineState(enum.Enum):
    initiated = "INITIATED"
    processed = "PROCESSED"
    audit_trail_written = "AUDIT_TRAIL_WRITTEN"
    failed = "FAILED"


class DataAndAnalytics:
    def __init__(self, pipeline: AnalyticsPipeline):
        self.pipeline = pipeline
        self.audit_trail = []
        self.clean_pipeline_map()
        self.state: PipelineState = PipelineState.initiated

    def clean_pipeline_map(self):
        for idx, node in enumerate(self.pipeline.nodes):
            node.steps = None
            node.resource = None
            node.data.label = get_node_label(idx)

    def generate_pipeline_figure(self):
        content = BytesIO()
        G = nx.DiGraph()
        G.add_nodes_from(list(map(lambda node: node.id, self.pipeline.nodes)))
        G.add_edges_from(
            list(map(lambda edge: (edge.source, edge.target), self.pipeline.edges))
        )

        labels = {}
        pos = {}
        colors = []

        for idx, node in enumerate(self.pipeline.nodes):
            labels[node.id] = node.data.label
            pos[node.id] = (
                node.position.x,
                node.position.y,
            )
            if node.data.type == "transform":
                color = "yellow"
            elif node.data.type == "extract":
                color = "grey"
            else:
                color = "lightgreen"
            colors.append(color)
        nx.draw_networkx(
            G,
            pos=pos,
            labels=labels,
            node_color=colors,
            arrows=True,
            node_shape="s",
        )
        plt.savefig(content, format="png", dpi=300)

        content.seek(0)
        content_bytes = content.read()
        content.close()
        return content_bytes

    def log_details(self, message):
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"{timestamp}\t{message}")
        self.audit_trail.append({"timestamp": timestamp, "message": message})

    def write_log_details(self, storage_account_name, folder_path):
        if len(self.audit_trail) == 0:
            return
        analytics_folder = "/".join(folder_path.split("/")[:-1])
        audit_trail_path = f"https://{storage_account_name}.blob.core.windows.net/{analytics_folder}/logs/audit_trail.csv"
        data = [list(self.audit_trail[0].keys())]
        for audit in self.audit_trail:
            data.append(list(audit.values()))
        try:
            resource = Resource(data=data)
            control = AzureBlobControl(overwrite=True)
            target_resource = TableResource(audit_trail_path, control=control)
            resource.write(target_resource)
            return True
        except Exception:
            return False

    def get_node_details(self, node_number):
        for item in self.pipeline.nodes:
            if item.id == node_number:
                return item
        return None

    def get_frictionless_object(self, file_path, file_name):
        if file_path.endswith(".zip"):
            res = TableResource(source=file_path, innerpath=f"cdm/{file_name}")
            return res
        elif file_path.endswith(file_name):
            res = TableResource(file_path)
            return res
        else:
            raise ValueError("Invalid file path")

    def update_visited_node(
        self, id, set_steps=None, resource=None, data_file_name=None
    ):
        for item in self.pipeline.nodes:
            if item.id == id:
                if item.processed is False:
                    item.processed = True
                if set_steps:
                    item.steps = set_steps
                if resource:
                    item.resource = resource
                if data_file_name:
                    item.data_file_name = data_file_name

    def generate_transform_step(self, operation_data: NodeData) -> List[Any]:
        try:
            dna_transform_step = DNATransformStep(
                node_data=operation_data, node=self.pipeline.nodes
            )
            steps = dna_transform_step.generate_step()
            self.log_details(dna_transform_step.message)
            return steps
        except Exception as ex:
            self.state = PipelineState.failed
            self.log_details(f"Error: {str(ex)}")
            self.log_details(FAILED_MESSAGE)
            raise ValueError(FAILED_ERROR)

    def transform_and_write(self, operation, current_resource_node, target_node):
        try:
            dataset = current_resource_node.to_copy()
            if operation is None:
                operation = []

            transformation_steps = (
                DNATransformStep.generate_preprocessing_steps() + operation
            )
            target = transform(dataset, steps=transformation_steps)
            control = AzureBlobControl(overwrite=True)
            target_resource = TableResource(target_node.data.file_path, control=control)
            target = target.write(target_resource)
            return True
        except Exception as ex:
            self.state = PipelineState.failed
            self.log_details(f"Error: {str(ex)}")
            self.log_details(FAILED_MESSAGE)
            raise ValueError(FAILED_ERROR)
        finally:
            for idx, item in enumerate(self.pipeline.edges):
                cleanup_source_node = self.get_node_details(item.source)
                cleanup_target_node = self.get_node_details(item.target)

                if (
                    cleanup_source_node.data_file_name is not None
                    and cleanup_source_node.data_file_name != []
                ):
                    for item in cleanup_source_node.data_file_name:
                        if os.path.exists(item):
                            os.unlink(item)
                if (
                    cleanup_target_node.data_file_name is not None
                    and cleanup_target_node.data_file_name != []
                ):
                    for item in cleanup_target_node.data_file_name:
                        if os.path.exists(item):
                            os.unlink(item)

    def update_load_node(self, id, status):
        for item in self.pipeline.nodes:
            if item.id == id and item.processed is None:
                item.processed = status
                break

    def build_adjacency_list(
        self, nodes: List[Node], edges: List[Edge]
    ) -> Dict[str, List[str]]:
        adjacency_list = {node.id: [] for node in nodes}
        for edge in edges:
            source = edge.source
            target = edge.target
            adjacency_list[source].append(target)
        return adjacency_list

    def dfs(self, node, adjacency_list, visited, stack):
        visited.add(node)
        stack.add(node)
        for neighbor in adjacency_list.get(node, []):
            if neighbor in stack:
                return False  # Cycle detected
            if neighbor not in visited:
                if not self.dfs(neighbor, adjacency_list, visited, stack):
                    return False
        stack.remove(node)
        return True

    def is_acyclic(self, nodes: List[Node], edges: List[Edge]):
        adjacency_list = self.build_adjacency_list(nodes, edges)
        visited = set()
        stack = set()
        for node in nodes:
            if node.id not in visited:
                if not self.dfs(node.id, adjacency_list, visited, stack):
                    return False  # Cycle detected
        return True  # No cycles found, graph is a DAG

    def validate(self):
        # validate that all edges have valid node
        print(00, flush=True)
        nodes_from_edges = set(
            [
                node_id
                for source_targe_tuple in [
                    (item.source, item.target) for item in self.pipeline.edges
                ]
                for node_id in source_targe_tuple
            ]
        )
        nodes_declared = set([node.id for node in self.pipeline.nodes])
        if nodes_from_edges != nodes_declared:
            self.log_details("Invalid pipeline, some nodes are not connected")
            raise ValueError("Invalid pipeline, some nodes are not connected")

        valid = self.is_acyclic(self.pipeline.nodes, self.pipeline.edges)
        if not valid:
            self.log_details(
                "Invalid pipeline, cycle detected.Pipeline should be a Directed Acyclic Graph (DAG)"
            )
            raise ValueError("Invalid pipeline, cycle detected")

    def process(self):
        start_time = datetime.now()
        self.log_details(
            "EBFlow Analytics | Data & Analytics pipeline process initiated"
        )
        self.log_details("Validating Pipeline")
        self.validate()
        self.log_details("Pipeline Validated")
        self.log_details(f"Total edges to process: {len(self.pipeline.edges)}")
        for idx, item in enumerate(self.pipeline.edges):

            source_node = self.get_node_details(item.source)
            target_node = self.get_node_details(item.target)

            self.log_details(
                f"Processing edge, {source_node.data.label} -> {target_node.data.label} ({idx + 1} of {len(self.pipeline.edges)})"
            )

            # processing sources
            if source_node.data.type == "extract" and source_node.processed == False:
                print(f"SOURCE-EXTRACT-{source_node.id}")
                self.log_details(source_node.data.__repr__())
                res = self.get_frictionless_object(
                    source_node.data.file_path, source_node.data.file_name
                )
                self.update_visited_node(source_node.id, resource=res)
                self.log_details("Pointer created for the source file")
                self.log_details("Extract node process complete")

            if source_node.data.type == "transform" and source_node.processed == False:
                print(f"SOURCE-TRANSFORM-{source_node.id}")
                transform_step = self.generate_transform_step(source_node.data)
                # handle condition for compare
                if source_node.data.operation_name in ["compare","overlap"]:
                    generated_data = transform_step["data"]
                    generated_steps = transform_step["steps"]
                    self.update_visited_node(
                        source_node.id,
                        set_steps=generated_steps,
                        resource=generated_data,
                        data_file_name=transform_step["temp_file_name"],
                    )
                else:
                    self.update_visited_node(source_node.id, set_steps=transform_step)

            # processing targets
            if target_node.data.type == "transform" and target_node.processed == False:
                print(f"TARGET-TRANSFORM-{target_node.id}")
                transform_step = self.generate_transform_step(target_node.data)
                # handle condition for compare
                if target_node.data.operation_name in ["compare","overlap"]:
                    generated_data = transform_step["data"]
                    print(
                        f"--------------------------\n{source_node.steps} \n\n {transform_step}"
                    )
                    generated_steps = []
                    self.update_visited_node(
                        target_node.id,
                        set_steps=generated_steps,
                        resource=generated_data,
                        data_file_name=transform_step["temp_file_name"],
                    )
                else:
                    if source_node.data.type == "transform":
                        if source_node.steps != None:
                            transform_step = source_node.steps + transform_step
                    self.update_visited_node(
                        target_node.id,
                        set_steps=transform_step,
                        resource=source_node.resource,
                    )

            # processing load
            if target_node.data.type == "load" and target_node.processed == False:
                print(f"TARGET-LOAD-{target_node.id}")
                self.log_details(target_node.data.__repr__())
                self.log_details(f"Applying {len(source_node.steps or [])} steps")

                status = self.transform_and_write(
                    source_node.steps, source_node.resource, target_node
                )
                self.log_details(
                    f"Output file saved at {target_node.data.file_path} ({target_node.data.file_name})"
                )
                self.update_load_node(target_node.id, status)

        end_time = datetime.now()
        self.log_details(
            "EBFlow Analytics | Data & Analytics pipeline process completed"
        )
        self.log_details(f"Time taken: {end_time - start_time}")
        self.state = PipelineState.processed
        return self.pipeline
