from datetime import datetime
from typing import Any, List, Dict, Optional
from pydantic import BaseModel, model_validator
import random

PIPELINE_MAP_NAME = "pipeline_map.json"


class Position(BaseModel):
    x: float
    y: float


class MultipleCalculationData(BaseModel):
    expression: str
    column: Dict[str, str]

    @model_validator(mode="before")
    def validate_multi_calculation_data(cls, values):
        if not values["expression"]:
            raise ValueError("Expression is required for Multiple Calculation Data")

        if not values["column"]:
            raise ValueError("Column is required for Multiple Calculation Data")

        values["expression"] = values["expression"].replace("^", "**")

        variables_used = list(values["column"].keys())
        expression = values["expression"].strip().replace(" ", "")

        for variable in variables_used:
            expression = expression.replace(
                variable,
                random.choice(
                    [
                        "1.0",
                        "2.0",
                        "3.0",
                        "4.0",
                        "5.0",
                        "6.0",
                        "7.0",
                        "8.0",
                        "9.0",
                        "10.0",
                    ]
                ),
            )

        try:
            eval(expression)
        except SyntaxError:
            raise ValueError("Expression is syntactically incorrect")
        except ZeroDivisionError:
            raise ValueError("Expression contains division by zero")
        except NameError as e:
            raise ValueError("Expression contains extra variables")
        except Exception as e:
            raise ValueError("Invalid expression")

        return values


class FilterOperations(BaseModel):
    title: str
    value: str


class ColumnOption(FilterOperations):
    data_type: str = "string"

class AggregationType(FilterOperations):
    data_types: List[str]


weekday_names = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday",
}


class NodeData(BaseModel):
    type: str
    label: Optional[str] = None
    file_name: Optional[str] = None
    file_path: Optional[str] = None
    ingestion: Optional[str] = None
    cdm_file: Optional[str] = None
    operation_name: Optional[str] = None
    column_name: Optional[str | ColumnOption | List[str | ColumnOption]] = None
    unique: Optional[bool] = None
    operation_formula: Optional[FilterOperations] = None
    operation_value: Optional[str | ColumnOption] = None
    data: Optional[MultipleCalculationData] = None
    dci_column: Optional[str | ColumnOption] = None
    amount_column: Optional[str] = None
    input_columns: Optional[List[str | ColumnOption]] = []
    output_columns: Optional[List[str | ColumnOption]] = []
    error: Optional[List[str]] = []
    processed: Optional[bool] = False
    weekdays: Optional[List[int]] = []
    custom_dates: Optional[List[str]] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    bkd_column: Optional[str | ColumnOption] = None
    is_bkd_custom: Optional[bool] = None
    group_by: Optional[List[str | ColumnOption]] = None
    aggregation_type: Optional[str | AggregationType] = None
    compare_columns: Optional[List[str | ColumnOption]] = None  # applicable for compare
    matching_columns: Optional[List[str | ColumnOption]] = None  # applicable for compare
    datasource: Optional[List[str]] = None  # applicable for compare

    def __repr__(self):
        node_representation = ""
        if self.type == "extract":
            node_representation = f"Extract: {self.file_path} ({self.file_name})"
        elif self.type == "load":
            node_representation = f"Load: {self.file_path}"
        else:
            node_representation = f"Transform: {self.operation_name}"
            if self.operation_name in [
                "delete_column",
                "sum",
                "avg",
                "min",
                "max",
                "net",
                "nwd",
                "owh",
                "bkd",
                "group_by",
                "count",
                "filter",
            ]:
                if isinstance(self.column_name, list):
                    node_representation += ", ".join([col.value for col in self.column_name])
                else:
                    node_representation += f" on {self.column_name.value}"
            if self.operation_name == "filter":
                node_representation += (
                    f" is {self.operation_formula.title} {self.operation_value.value}"
                )
            elif self.operation_name == "count":
                if self.unique is True:
                    node_representation += " unique"
                else:
                    node_representation += " all"
            elif self.operation_name == "net":
                node_representation += (
                    f" with amount debit/credit Indicator as {self.dci_column.value}"
                )
            elif self.operation_name == "multiple_calculation":
                expression = self.data.expression
                column = self.data.column
                for key, value in column.items():
                    expression = expression.replace(key, value)

                node_representation += f" with formula {expression}"

            elif self.operation_name == "nwd":
                custom_dates = self.custom_dates
                weekdays = self.weekdays

                if custom_dates:
                    node_representation += (
                        f" with custom date(s) as {', '.join(custom_dates)}"
                    )
                if weekdays:
                    node_representation += f" with weekday(s) as {', '.join([weekday_names[dy] for dy in weekdays])}"

            elif self.operation_name == "owh":
                node_representation += (
                    f" with start time {self.start_time} and end time {self.end_time}"
                )

            elif self.operation_name == "bkd":
                node_representation += f" with backdating {'value' if self.is_bkd_custom else 'column'} {self.bkd_column.value}"

            elif self.operation_name == "group_by":
                node_representation += f" with group by {', '.join([col.value for col in self.group_by])} and aggregation type {self.aggregation_type.value}"

                if self.aggregation_type == "count" and self.unique:
                    node_representation += " unique"
            elif self.operation_name == "compare":
                node_representation += f" with compare columns {', '.join(self.compare_columns)} and matching columns {', '.join(self.matching_columns)}"

        return node_representation

    @model_validator(mode="before")
    def validate_dna_node_data(cls, values):
        if values["type"] not in ["extract", "transform", "load"]:
            raise ValueError("Invalid type for DnA Node Data")

        if len(values.get('output_columns', [])) > 0:
            new_output_columns = []
            for column in values['output_columns']:
                if isinstance(column, str):
                    column = ColumnOption(title=column, value=column)
                new_output_columns.append(column)
            values['output_columns'] = new_output_columns

        if values["type"] in ["extract", "load"]:
            if not values["file_name"] or not values["file_path"]:
                raise ValueError(
                    "File name and file path are required for extract node"
                )

            if values["type"] == "extract" and not values["ingestion"]:
                raise ValueError("Ingestion is required for extract node")

            if values["type"] == "extract" and not values["cdm_file"]:
                raise ValueError("CDM File is required for extract node")

            return values

        if not values["input_columns"]:
            raise ValueError("Input columns are required for transform node")
        else:
            new_input_columns = []
            for column in values["input_columns"]:
                if isinstance(column, str):
                    column = ColumnOption(title=column, value=column)
                new_input_columns.append(column)
            values["input_columns"] = new_input_columns

        if not values["operation_name"]:
            raise ValueError("Operation name is required for transform node")

        if values["operation_name"] not in [
            "filter",
            "delete_column",
            "duplicate",
            "sum",
            "avg",
            "min",
            "max",
            "net",
            "count",
            "multiple_calculation",
            "nwd",
            "owh",
            "bkd",
            "group_by",
            "compare",
            "overlap",

        ]:
            raise ValueError("Invalid operation name for transform node")

        if values["operation_name"] in [
            "delete_column",
            "sum",
            "avg",
            "min",
            "max",
            "net",
            "nwd",
            "owh",
            "bkd",
            "group_by",
            "count",
            "filter",
        ]:
            if not values["column_name"]:
                raise ValueError("column_name is required for operation")
            if values["operation_name"] == "delete_column" and not isinstance(
                values["column_name"], list
            ):
                raise ValueError("column_name should be a list for delete operation")

            if values["operation_name"] == "net":
                if not values["dci_column"]:
                    raise ValueError("dci_column is required for operation")

                if isinstance(values["dci_column"], str):
                    values["dci_column"] = ColumnOption(
                        title=values["dci_column"], value=values["dci_column"]
                    )

            if isinstance(values["column_name"], list):
                new_column_names = []
                for column in values["column_name"]:
                    if isinstance(column, str):
                        column = ColumnOption(title=column, value=column)
                    new_column_names.append(column)
                values["column_name"] = new_column_names
            elif isinstance(values["column_name"], str):
                values["column_name"] = ColumnOption(
                    title=values["column_name"], value=values["column_name"]
                )

        if values["operation_name"] == "filter":
            if not values["operation_formula"]:
                raise ValueError("operation_formula is required for operation")

            if not values["operation_value"]:
                raise ValueError("operation_value is required for operation")

            if isinstance(values["operation_value"], str):
                values["operation_value"] = ColumnOption(
                    title=values["operation_value"], value=values["operation_value"]
                )

        elif values["operation_name"] == "count":
            if "unique" not in values.keys() or values["unique"] is None:
                raise ValueError("Unique is required for operation")

        elif values["operation_name"] == "net":
            if not values["dci_column"]:
                raise ValueError("dci_column is required for operation")

        elif values["operation_name"] == "nwd":
            custom_dates = values.get("custom_dates")
            weekdays = values.get("weekdays")

            if not custom_dates and not weekdays:
                raise ValueError("Custom Dates or Weekdays are required for operation")

        elif values["operation_name"] == "owh":
            if not values["start_time"]:
                raise ValueError("Start Time is required for operation")
            if not values["end_time"]:
                raise ValueError("End Time is required for operation")

        elif values["operation_name"] == "bkd":
            if not values["bkd_column"]:
                raise ValueError("Bkd Column is required for operation")
            if "is_bkd_custom" not in values.keys() or values["is_bkd_custom"] is None:
                raise ValueError("Is Bkd Custom is required for operation")

            if isinstance(values["bkd_column"], str):
                values["bkd_column"] = ColumnOption(
                    title=values["bkd_column"], value=values["bkd_column"]
                )

        elif values["operation_name"] == "group_by":
            if not values["group_by"]:
                raise ValueError("Group By is required for operation")
            if not values["aggregation_type"]:
                raise ValueError("Aggregation Type is required for operation")

            if isinstance(values["aggregation_type"], str):
                values["aggregation_type"] = AggregationType(
                    title=values["aggregation_type"],
                    value=values["aggregation_type"],
                    data_types=["string", "number"],
                )

            if values["aggregation_type"].value not in ["sum", "min", "max", "count", "avg"]:
                raise ValueError("Invalid aggregation type for operation")

            if values["aggregation_type"] == "count":
                if "unique" not in values.keys() or values["unique"] is None:
                    raise ValueError("Unique is required for operation")

            new_group_by_value = []
            for column in values["group_by"]:
                if isinstance(column, str):
                    column = ColumnOption(title=column, value=column)
                new_group_by_value.append(column)
            values["group_by"] = new_group_by_value

        elif values["operation_name"] == "compare":
            if not values["compare_columns"]:
                raise ValueError("Compare Columns are required for operation")
            if not values["matching_columns"]:
                raise ValueError("Matching Columns are required for operation")
            if not values["datasource"]:
                raise ValueError("Datasource is required for operation")

            new_compare_columns = []
            for column in values["compare_columns"]:
                if isinstance(column, str):
                    column = ColumnOption(title=column, value=column)
                new_compare_columns.append(column)
            values["compare_columns"] = new_compare_columns

            new_matching_columns = []
            for column in values["matching_columns"]:
                if isinstance(column, str):
                    column = ColumnOption(title=column, value=column)
                new_matching_columns.append(column)
            values["matching_columns"] = new_matching_columns


        return values


class Node(BaseModel):
    id: str
    data: NodeData
    type: str = "N/A"
    height: float = 0
    width: float = 0
    selected: bool = False
    dragging: bool = False
    position: Optional[Position] = None
    positionAbsolute: Optional[Position] = None
    processed: Optional[bool] = False
    steps: Optional[List[Any]] = None
    resource: Optional[Any] = None
    data_file_name: Optional[str] = None  # applicable for compare


class Edge(BaseModel):
    id: str
    source: str
    target: str


class AnalyticsPipeline(BaseModel):
    nodes: List[Node]
    edges: List[Edge]


class PipelineMapDetails(BaseModel):
    name: Optional[str]
    audit_firm: str
    client: str
    engagement: str
    is_saved: bool
    pipeline_id: Optional[str]
    pipeline_map: Optional[AnalyticsPipeline]

    def get_file_path(self):
        initial_directory = (
            f"{self.audit_firm}/analytics/{self.client}/{self.engagement}"
        )
        pipeline_map_path = f"{'saved' if self.is_saved else 'pipelines'}/{self.pipeline_id}/{PIPELINE_MAP_NAME}"

        return f"{initial_directory}/{pipeline_map_path}"


class ListPipeLineMaps(BaseModel):
    audit_firm: str
    client: str
    engagement: str
    is_saved: bool

    def get_lookup_directory(self):
        return f"{self.audit_firm}/analytics/{self.client}/{self.engagement}/{'saved' if self.is_saved else 'pipelines'}"


class PipelineSummaryRequest(BaseModel):
    audit_firm: str
    client: str
    engagement: str
    pipeline_id: Optional[str]
    load_pipeline_map: bool = False

    def get_pipeline_directory(self):
        return f"{self.audit_firm}/analytics/{self.client}/{self.engagement}/pipelines/{self.pipeline_id}"


class FileDetails(BaseModel):
    filename: str
    filepath: str
    created: datetime
    pipeline_id: Optional[str] = None


class PipelineSummaryResponse(BaseModel):
    pipeline_map: Optional[AnalyticsPipeline]
    outputs: List[FileDetails]


class FilePathRequest(BaseModel):
    file_path: str
    num_rows: int = 50
