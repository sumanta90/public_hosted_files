import hashlib
import attrs
import petl as etl
from decimal import Decimal
from datetime import datetime, timedelta
from typing import List, Any, Dict
from frictionless import Resource, Step, fields
from frictionless.resources import TableResource
from frictionless.transformer.transformer import DataWithErrorHandling


@attrs.define(kw_only=True, repr=False)
class eb_filter(Step):

    type = "eb-filter"
    column_name: str
    operation: str
    value: Any
    # Transform

    def get_value_with_data_type(self, field, value):
        try:
            if field.type in ["decimal", "number"]:
                return Decimal(value)
            elif field.type == "integer":
                return int(value)
            elif field.type == "datetime":
                return datetime.fromisoformat(value)
            elif field.type == "date":
                return datetime.fromisoformat(value).date()
            elif field.type == "time":
                return datetime.fromisoformat(value).time()
            elif field.type == "boolean":
                return bool(value)
            else:
                return str(value)
        except ValueError:
            return value

    def get_filter_function(self, value):

        if self.operation == "==":
            return lambda row: row[self.column_name] == value
        elif self.operation == "!=":
            return lambda row: row[self.column_name] != value
        elif self.operation == ">":
            return lambda row: row[self.column_name] > value
        elif self.operation == "<":
            return lambda row: row[self.column_name] < value
        elif self.operation == ">=":
            return lambda row: row[self.column_name] >= value
        elif self.operation == "<=":
            return lambda row: row[self.column_name] <= value
        elif self.operation == "in":
            return lambda row: row[self.column_name] in value

    def get_filter_function_for_column(self, check_field):
        if self.operation == "==":
            return lambda row: row[self.column_name] == row[check_field]
        elif self.operation == "!=":
            return lambda row: row[self.column_name] != row[check_field]
        elif self.operation == ">":
            return lambda row: row[self.column_name] > row[check_field]
        elif self.operation == "<":
            return lambda row: row[self.column_name] < row[check_field]
        elif self.operation == ">=":
            return lambda row: row[self.column_name] >= row[check_field]
        elif self.operation == "<=":
            return lambda row: row[self.column_name] <= row[check_field]

    def transform_resource(self, resource):
        table = resource.to_petl()
        field = resource.schema.get_field(self.column_name)
        value_field = None
        try:
            value_field = resource.schema.get_field(self.value)
        except Exception:
            pass
        try:
            if value_field:
                lambda_func = self.get_filter_function_for_column(value_field.name)
                filtered_data = etl.select(table, lambda_func)
            else:
                value_new = self.get_value_with_data_type(field, self.value)
                lambda_func = self.get_filter_function(value_new)
                filtered_data = etl.select(table, lambda_func)
        except Exception:
            filtered_data = table
        resource.data = filtered_data

    # Metadata

    metadata_profile_patch = {
        "properties": {
            "column_name": {"type": "string"},
            "operation": {"type": "string"},
            "value": {},
        },
    }


class identify_duplicate:
    def __init__(self):
        self.hash_values = []

    def __call__(self, row):
        row_str = ",".join(map(str, row))
        hash_object = hashlib.sha256()
        hash_object.update(row_str.encode("utf-8"))
        hash_value = hash_object.hexdigest()
        is_duplicate = False
        if hash_value in self.hash_values:
            is_duplicate = True
        else:
            self.hash_values.append(hash_value)
        if is_duplicate:
            return "Y"
        else:
            return "N"


class non_working_days:
    custom_dates: List[str]
    weekdays: List[int]
    date_column: str

    def __init__(self, custom_dates: List[str], weekdays: List[int], date_column: str):
        try:
            self.weekdays = weekdays
            self.date_column = date_column
            self.custom_dates = [
                datetime.fromisoformat(dt).strftime("%Y-%m-%d") for dt in custom_dates
            ]
        except Exception as ex:
            raise ValueError(f"Error in Custom Date for 'Non-Working Days': {str(ex)}")

    def __call__(self, row):
        is_non_working_day = "N"
        value_to_check = row[self.date_column]
        date_in_string_format = (
            value_to_check.strftime("%Y-%m-%d")
            if isinstance(value_to_check, datetime)
            else value_to_check
        )
        if (
            value_to_check.weekday() in self.weekdays
            or date_in_string_format in self.custom_dates
        ):
            is_non_working_day = "Y"

        return is_non_working_day


class outside_working_hours:
    start_time: str
    end_time: str
    date_column: str

    def __init__(self, start_time: str, end_time: str, date_column: str):
        self.start_time = start_time
        self.end_time = end_time
        self.date_column = date_column

    def __call__(self, row):
        is_outside_working_hours = "N"
        value_to_check = row[self.date_column]
        if (
            value_to_check.time() < datetime.strptime(self.start_time, "%H:%M").time()
        ) or (value_to_check.time() > datetime.strptime(self.end_time, "%H:%M").time()):
            is_outside_working_hours = "Y"
        return is_outside_working_hours


class backdating:
    date_column: str
    bkd_column: str
    custom_date: datetime
    is_bkd_custom: bool

    def __init__(self, date_column: str, bkd_column: str, is_bkd_custom: bool):

        self.date_column = date_column
        self.is_bkd_custom = is_bkd_custom
        self.bkd_column = bkd_column
        if is_bkd_custom:
            try:
                self.custom_date = datetime.fromisoformat(bkd_column).date()
            except Exception as ex:
                raise ValueError(f"Error in Custom Date for 'Backdating': {str(ex)}")

    def __call__(self, row):
        value = None
        value_to_check = (
            datetime.fromisoformat(row[self.date_column])
            if isinstance(row[self.date_column], str)
            else row[self.date_column]
        )
        value_to_check = (
            value_to_check.date()
            if isinstance(value_to_check, datetime)
            else value_to_check
        )
        if self.is_bkd_custom:
            value = self.custom_date - value_to_check
        else:
            bkd_value = (
                datetime.fromisoformat(row[self.bkd_column])
                if isinstance(row[self.bkd_column], str)
                else row[self.bkd_column]
            )
            bkd_value = (
                bkd_value.date() if isinstance(bkd_value, datetime) else bkd_value
            )
            if value_to_check is None or bkd_value is None:
                value = timedelta(days=0)
            else:
                value = bkd_value - value_to_check

        return value.days


@attrs.define(kw_only=True, repr=False)
class multiple_aggregate(Step):
    type = "multiple_aggregate"

    aggregation: Dict[str, Any]
    group_names: List[str]
    # Transform

    def transform_resource(self, resource: Resource):
        table = resource.to_petl()  # type: ignore
        group_by_fields = [resource.schema.get_field(name) for name in self.group_names]
        resource.schema.fields.clear()
        for field in group_by_fields:
            resource.schema.add_field(field)
        for name in self.aggregation.keys():
            resource.schema.add_field(fields.AnyField(name=name))
        data = etl.aggregate(
            table,
            key=tuple(self.group_names),
            aggregation=list(self.aggregation.values())[0][1],
            value=list(self.aggregation.values())[0][0],
        )  # type: ignore
        resource.data = etl.rename(data, {"value": list(self.aggregation.keys())[0]})  # type: ignore

    # Metadata

    metadata_profile_patch = {
        "type": "object",
        "required": ["groupNames", "aggregation"],
        "properties": {
            "groupNames": {"type": "array"},
            "aggregation": {"type": "object"},
        },
    }


@attrs.define(kw_only=True, repr=False)
class average(Step):
    type = "find_average"

    field_name: str
    # Transform

    def transform_resource(self, resource: Resource):
        table = resource.to_petl()  # type: ignore
        field_stats = table.stats(field=self.field_name)
        if field_stats.errors > 0:
            raise ValueError(
                f"Error in finding average for field {self.field_name}, must be non-numeric"
            )
        mean = field_stats.mean
        new_table_data = [[f"avg_{self.field_name}"], [round(mean, 5)]]
        new_table = etl.head(new_table_data, 1)
        resource.schema.fields.clear()
        resource.schema.add_field(fields.AnyField(name=f"avg_{self.field_name}"))
        resource.data = new_table

    # Metadata

    metadata_profile_patch = {
        "type": "object",
        "required": ["fieldName"],
        "properties": {
            "fieldName": {"type": "string"},
        },
    }


@attrs.define(kw_only=True, repr=False)
class convert_numeric_none_to_zero(Step):
    type = "convert_numeric_none_to_zero"
    # Transform

    def transform_resource(self, resource: Resource):
        table = resource.to_petl()  # type: ignore
        for field in resource.schema.fields:
            if field.type in ["number", "integer", "decimal"]:
                table = table.replace(field.name, None, 0)
        resource.data = table

    # Metadata

    metadata_profile_patch = {
        "type": "object",
        "required": [],
        "properties": {},
    }
