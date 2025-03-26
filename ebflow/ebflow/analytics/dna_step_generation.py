import copy
import os
from frictionless import steps
from ebflow.analytics.analytics_schema import NodeData
from ebflow.analytics.custom_steps import (
    average,
    identify_duplicate,
    non_working_days,
    outside_working_hours,
    backdating,
    multiple_aggregate,
    convert_numeric_none_to_zero,
    eb_filter,
)
import random
from ebflow.utils.custom_steps import custom_row_compare, custom_table_join
from frictionless.resources import TableResource
from frictionless import Resource, Pipeline, transform


class DNATransformStep:
    def __init__(self, node_data: NodeData, node=None):
        self.node_data = node_data
        self.message = None
        self.pipeline_nodes = node
        self.files_to_be_deleted = []

    @classmethod
    def generate_preprocessing_steps(cls):
        return [
            steps.table_normalize(),
            convert_numeric_none_to_zero(),
        ]

    def generate_sum(self):
        return [
            steps.field_add(
                name="sum_group",
                value="SUM",
            ),
            steps.table_aggregate(
                group_name="sum_group",
                aggregation={
                    f"sum_{self.node_data.column_name.value}": (
                        self.node_data.column_name.value,
                        sum,
                    )
                },
            ),
            steps.field_remove(names=["sum_group"]),
        ]

    def generate_average(self):
        return [
            average(field_name=self.node_data.column_name.value),
        ]

    def generate_max(self):
        return [
            steps.field_add(
                name="max_group",
                value="MAX",
            ),
            steps.table_aggregate(
                group_name="max_group",
                aggregation={
                    f"max_{self.node_data.column_name.value}": (
                        self.node_data.column_name.value,
                        max,
                    )
                },
            ),
            steps.field_remove(names=["max_group"]),
        ]

    def generate_min(self):
        return [
            steps.field_add(
                name="min_group",
                value="MIN",
            ),
            steps.table_aggregate(
                group_name="min_group",
                aggregation={
                    f"min_{self.node_data.column_name.value}": (
                        self.node_data.column_name.value,
                        min,
                    )
                },
            ),
            steps.field_remove(names=["min_group"]),
        ]

    def generate_net(self):
        return [
            steps.field_add(
                name=f"net_{self.node_data.column_name.value}_{self.node_data.dci_column.value}",
                function=lambda row, dci_column=self.node_data.dci_column.value, amount_column=self.node_data.column_name.value: (
                    row[amount_column]
                    if row[dci_column] == "D"
                    else -1 * row[amount_column]
                ),
                descriptor={"type": "number"},
            ),
        ]

    def generate_count(self):

        step_data = [
            steps.field_add(
                name="count_group",
                value="COUNT",
            ),
        ]
        if self.node_data.unique:
            step_data.append(
                steps.table_aggregate(
                    group_name="count_group",
                    aggregation={
                        f"count_unique_{self.node_data.column_name.value}": (
                            self.node_data.column_name.value,
                            lambda x: len(set([i for i in x if i is not None])),
                        )
                    },
                )
            )
        else:
            step_data.append(
                steps.table_aggregate(
                    group_name="count_group",
                    aggregation={
                        f"count_{self.node_data.column_name.value}": (
                            self.node_data.column_name.value,
                            lambda x: len(list([i for i in x if i is not None])),
                        )
                    },
                )
            )
        step_data.append(steps.field_remove(names=["count_group"]))
        return step_data

    def generate_multiple_calculation(self):
        expr = self.node_data.data.expression
        col_data = self.node_data.data.column
        expression = expr
        for key, value in col_data.items():
            expression = expression.replace(key, str(value))

        def prepare_formula(row, e=expr, cols=copy.deepcopy(col_data)):
            for key, value in cols.items():
                e = e.replace(key, str(row[value]) if row[value] else "0")

            return eval(e)

        return [
            steps.field_add(
                name=f"maths_{'_'.join(list(self.node_data.data.column.values()))}",
                function=prepare_formula,
            ),
        ]

    def generate_duplicate(self):
        return [
            steps.field_add(name="is_duplicate", function=identify_duplicate()),
        ]

    def generate_remove_column(self):
        return [
            steps.field_remove(names=[col.value for col in self.node_data.column_name]),
        ]

    def generate_filter(self):
        return [
            eb_filter(
                column_name=self.node_data.column_name.value,
                operation=self.node_data.operation_formula.value,
                value=self.node_data.operation_value.value,
            ),
        ]

    def generate_group_by(self):
        aggregation = {}
        if self.node_data.aggregation_type.value == "sum":
            aggregation = {
                f"sum_{self.node_data.column_name.value}": (
                    self.node_data.column_name.value,
                    sum,
                )
            }
        elif self.node_data.aggregation_type.value == "min":
            aggregation = {
                f"min_{self.node_data.column_name.value}": (
                    self.node_data.column_name.value,
                    min,
                )
            }
        elif self.node_data.aggregation_type.value == "max":
            aggregation = {
                f"max_{self.node_data.column_name.value}": (
                    self.node_data.column_name.value,
                    max,
                )
            }
        elif self.node_data.aggregation_type.value == "avg":

            def avg(x):
                count = 0
                total = 0
                for i in x:
                    total += i
                    count += 1
                try:
                    avg = total / count
                except ZeroDivisionError:
                    avg = 0
                return round(avg, 5)

            aggregation = {
                f"avg_{self.node_data.column_name.value}": (
                    self.node_data.column_name.value,
                    lambda x: avg(x),
                )
            }
        elif self.node_data.aggregation_type.value == "count":
            if self.node_data.unique:
                aggregation = {
                    f"count_unique_{self.node_data.column_name.value}": (
                        self.node_data.column_name.value,
                        lambda x: len(set([i for i in x if i is not None])),
                    )
                }
            else:
                aggregation = {
                    f"count_{self.node_data.column_name.value}": (
                        self.node_data.column_name.value,
                        lambda x: len(list([i for i in x if i is not None])),
                    )
                }
        return [
            multiple_aggregate(
                group_names=[col.value for col in self.node_data.group_by],
                aggregation=aggregation,
            ),
        ]

    def generate_nwd(self):
        return [
            steps.field_add(
                name=f"nwd_{self.node_data.column_name.value}",
                function=non_working_days(
                    date_column=self.node_data.column_name.value,
                    custom_dates=(
                        self.node_data.custom_dates
                        if self.node_data.custom_dates
                        else []
                    ),
                    weekdays=self.node_data.weekdays if self.node_data.weekdays else [],
                ),
            ),
        ]

    def generate_owh(self):
        return [
            steps.field_add(
                name=f"owh_{self.node_data.column_name.value}",
                function=outside_working_hours(
                    date_column=self.node_data.column_name.value,
                    start_time=self.node_data.start_time,
                    end_time=self.node_data.end_time,
                ),
            ),
        ]

    def generate_bkd(self):
        return [
            steps.field_add(
                name=f"bkd_{self.node_data.column_name.value}",
                function=backdating(
                    date_column=self.node_data.column_name.value,
                    bkd_column=self.node_data.bkd_column.value,
                    is_bkd_custom=self.node_data.is_bkd_custom,
                ),
            ),
        ]

    # components for compare functionality

    def change_resource_field_name2(self, resource, table_name=""):
        resource.infer()
        field_update_steps = [steps.table_normalize()]
        if table_name:
            table_name = table_name + "_"
        for field in resource.schema.fields:
            standard_name = str(field.name).strip().replace(" ", "_")
            standard_name = standard_name.replace("/", "_")
            standard_name = standard_name.replace(".", "_")
            standard_name = table_name + standard_name
            field_update_steps.append(
                steps.field_update(name=field.name, descriptor={"name": standard_name})
            )
        resource.transform(Pipeline(steps=field_update_steps))
        return resource

    def get_node_details_from_node_id(self, node_id):
        for single_node in self.pipeline_nodes:
            if single_node.id == node_id:
                return single_node

    def get_compare_datasource(self, node_data_path, table_name):
        res = None
        if "/" in node_data_path and "." in node_data_path:
            res = TableResource(node_data_path)
        else:
            # its a node id, find the node and get the data
            nodeData = self.get_node_details_from_node_id(node_data_path)
            res = nodeData.resource
        res = self.change_resource_field_name2(res, table_name)
        return res

    def rename_matching_column(self, collist, table_name):
        for index, col in enumerate(collist):
            collist[index] = table_name + "_" + col
        return collist

    def return_field_add_function_for_compare(self, field_name1, field_name2):
        def compare_fields(row, fn1=field_name1, fn2=field_name2):
            if str(row[fn1]).isnumeric() == str(row[fn2]).isnumeric():
                return str(int(row[fn1]) - int(row[fn2]))
            else:
                return str(str(row[fn1]) == str(row[fn2]))

        return compare_fields
    def get_column_name_from_resource(self, resource):
        return resource.schema.field_names
    
    def collect_previous_steps(self, NodeData,first_datasource_name, second_datasource_name):
        
        random_number = str(random.randint(11111111, 99999999))
        collected_steps = dict()
        if (NodeData is not None and NodeData.type == "transform" and NodeData.operation_name in ["compare","overlap"]):
            # first resource data
            if NodeData.datasource[0] and "/" not in NodeData.datasource[0]:
                first_nodeData = self.get_node_details_from_node_id(NodeData.datasource[0])
                if first_nodeData.data.type == 'extract':
                    # what is  the extract node is not processed
                    if first_nodeData.processed == False:
                        collected_steps['first_node_processed_data'] = self.change_resource_field_name2(TableResource(first_nodeData.data.file_path), first_datasource_name)
                    else:
                        # what  is the extract node is already processed
                        collected_steps['first_node_processed_data'] = self.change_resource_field_name2(first_nodeData.resource, first_datasource_name)
                else:
                    if first_nodeData.steps:
                        process_first_precious_node_steps = transform(
                            first_nodeData.resource,
                            steps=[
                                steps.table_normalize(),
                                first_nodeData.steps[0]
                            ]
                        )
                        process_first_precious_node_steps.write(f"{random_number}_first_node.csv")
                        first_node_resource = TableResource(f"{random_number}_first_node.csv", format="csv")
                        first_res = self.change_resource_field_name2(first_node_resource, first_datasource_name)
                        collected_steps['first_node_processed_data'] = first_res
                        collected_steps['first_node_columns'] = self.get_column_name_from_resource(first_res)
                        self.files_to_be_deleted.append(f"{random_number}_first_node.csv")
            # second resource data
            if (NodeData.datasource[1] and "/" not in NodeData.datasource[1]):
                second_nodeData = self.get_node_details_from_node_id(NodeData.datasource[1])
                if second_nodeData.data.type == 'extract':
                    # what is extract node in data item 2 is not processed
                    if second_nodeData.processed == False:
                        collected_steps['second_node_processed_data'] = self.change_resource_field_name2(TableResource(second_nodeData.data.file_path), second_datasource_name)
                    else:
                        collected_steps['second_node_processed_data'] = self.change_resource_field_name2(second_nodeData.resource, second_datasource_name)
                else:
                    process_second_precious_node_steps = transform(
                        second_nodeData.resource,
                        steps=[
                            steps.table_normalize(),
                            second_nodeData.steps[0]
                        ]
                    )
                    process_second_precious_node_steps.write(f"{random_number}_second_node.csv")
                    second_node_resource = TableResource(f"{random_number}_second_node.csv", format="csv")
                    second_res = self.change_resource_field_name2(second_node_resource, second_datasource_name)
                    collected_steps['second_node_processed_data'] = second_res
                    collected_steps['second_node_columns'] = self.get_column_name_from_resource(second_res)
                    self.files_to_be_deleted.append(f"{random_number}_second_node.csv")
        return collected_steps
    
    def get_node_label(self, node_id):
        label = None
        for single_node in self.pipeline_nodes:
            if single_node.id == node_id:
                label = single_node.data.label
            
        return  label
    
    def generate_compare(self):
        random_number = str(random.randint(11111111, 99999999))
        
        collected_steps = self.collect_previous_steps(
            self.node_data.model_copy(), 
            self.get_node_label(self.node_data.datasource[0]), 
            self.get_node_label(self.node_data.datasource[1])
        )
        

        transformation_step = transform(
            collected_steps['first_node_processed_data'],
            steps=[
                steps.table_normalize(),
                custom_table_join(
                    resource=collected_steps['second_node_processed_data'],
                    mode="outer",
                    left_fields=self.rename_matching_column(
                        self.node_data.matching_columns.copy(),
                        self.get_node_label(self.node_data.datasource[0]),
                    ),
                    right_fields=self.rename_matching_column(
                        self.node_data.matching_columns.copy(),
                        self.get_node_label(self.node_data.datasource[1]),
                    ),
                ),
                steps.field_add(
                    name="matching_columns_match",
                    function=self.return_field_add_function_for_compare(
                        self.rename_matching_column(
                            self.node_data.compare_columns.copy(),
                            self.get_node_label(self.node_data.datasource[0]),
                        )[0],
                        self.rename_matching_column(
                            self.node_data.compare_columns.copy(),
                            self.get_node_label(self.node_data.datasource[1]),
                        )[-1],
                    ),
                ),
            ],
        )
        transformation_step.write(f"{random_number}_temp_gen_compare.csv")
        single_resource = TableResource(
            f"{random_number}_temp_gen_compare.csv", format="csv"
        )
        self.files_to_be_deleted.append(f"{random_number}_temp_gen_compare.csv")
        return {
            "data": single_resource,
            "steps": [],
            "temp_file_name": self.files_to_be_deleted,
        }


    def generate_overlap(self):
        random_number = str(random.randint(11111111, 99999999))
        
        collected_steps = self.collect_previous_steps(
            self.node_data.model_copy(), 
            self.get_node_label(self.node_data.datasource[0]), 
            self.get_node_label(self.node_data.datasource[1])
        )
        transformation_step = transform(
            collected_steps['first_node_processed_data'].to_copy(),
            steps=[
                steps.table_normalize(),
                custom_row_compare(
                    resource=collected_steps['second_node_processed_data'].to_copy()
                )
            ],
        )
        
        transformation_step.write(f"{random_number}_temp_gen_overlap_first.csv")
        first_data_source = TableResource(
            f"{random_number}_temp_gen_overlap_first.csv", format="csv"
        )
        self.files_to_be_deleted.append(f"{random_number}_temp_gen_overlap_first.csv")
        
        return {
            "data": first_data_source,
            "steps": [],
            "temp_file_name": self.files_to_be_deleted,
        }
    
    
    def generate_step(self):
        self.message = self.node_data.__repr__()
        if self.node_data.operation_name == "compare":
            return self.generate_compare()
        if self.node_data.operation_name == 'overlap':
            return self.generate_overlap()
        if self.node_data.operation_name == "sum":
            return self.generate_sum()
        elif self.node_data.operation_name == "avg":
            return self.generate_average()
        elif self.node_data.operation_name == "max":
            return self.generate_max()
        elif self.node_data.operation_name == "min":
            return self.generate_min()
        elif self.node_data.operation_name == "net":
            return self.generate_net()
        elif self.node_data.operation_name == "count":
            return self.generate_count()
        elif self.node_data.operation_name == "multiple_calculation":
            return self.generate_multiple_calculation()
        elif self.node_data.operation_name == "duplicate":
            return self.generate_duplicate()
        elif self.node_data.operation_name == "delete_column":
            return self.generate_remove_column()
        elif self.node_data.operation_name == "filter":
            return self.generate_filter()
        elif self.node_data.operation_name == "group_by":
            return self.generate_group_by()
        elif self.node_data.operation_name == "nwd":
            return self.generate_nwd()
        elif self.node_data.operation_name == "owh":
            return self.generate_owh()
        elif self.node_data.operation_name == "bkd":
            return self.generate_bkd()
        else:
            raise ValueError("Invalid operation name for transform node")
