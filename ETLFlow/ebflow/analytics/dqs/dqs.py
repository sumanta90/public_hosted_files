from abc import ABC, abstractmethod
from typing import Any

from frictionless import Pipeline, steps
from frictionless.resources import TableResource

from ebflow.utils.custom_steps import (
    value_counts,
    custom_aggregate,
    custom_sum,
    check_field_gaps,
)


class DQS(ABC):
    def __init__(self, resource: TableResource, entity_type: str, cdm_fields: [dict]):
        self.resource = resource
        self.resource.infer()
        self.entity_type = entity_type
        self.cdm = [
            {"name": cdm_field["cdm_field"], "type": cdm_field["data_type"]}
            for cdm_field in cdm_fields
        ]

    @staticmethod
    def get_percent(count, total):
        if total == 0:
            return 0
        else:
            return round((count / total) * 100)

    def get_unique_value_count(self, fields: [str]):
        count = self.resource.to_copy()
        count.transform(
            Pipeline(
                steps=[
                    steps.row_filter(
                        function=lambda row: all([row[field] for field in fields])
                    ),
                    value_counts(field_names=fields),
                    custom_aggregate(aggregation={"row_count": len}),
                ]
            )
        )
        count = count.read_rows()
        count = int(count[0]["row_count"]) if count else 0
        return count

    def get_field_sum(self, field: str):
        field_sum = self.resource.to_copy()
        field_sum.transform(
            Pipeline(
                steps=[custom_aggregate(aggregation={"field_sum": (field, custom_sum)})]
            )
        )
        field_sum = float(field_sum.read_rows()[0]["field_sum"])
        return field_sum

    def all_rows_blank(self, fields: [str]):
        present = 0
        controls_new_list = list()

        controls = self.resource.to_copy()
        controls.transform(
            Pipeline(
                steps=[
                    custom_aggregate(
                        group_name=None,
                        aggregation={
                            field_name: (field_name, any) for field_name in fields
                        },
                    )
                ]
            )
        )

        for field_name, has_value in controls.read_rows()[0].items():
            controls_new_list.append({field_name: has_value})
            present += 1 if has_value else 0

        control_check_percentage = self.get_percent(present, len(fields))
        return (
            control_check_percentage,
            controls_new_list,
        )

    @abstractmethod
    def statistical(self):
        pass

    @abstractmethod
    def business_rule(self):
        pass

    def profile(self):
        all_fields = list(self.resource.header)

        # Text fields' info
        text_fields = [
            field["name"]
            for field in list(
                filter(
                    lambda cdm_field: cdm_field["type"] in ["string"]
                    and cdm_field["name"] in all_fields,
                    self.cdm,
                )
            )
        ]

        # Numeric fields' info
        numeric_fields = [
            field["name"]
            for field in list(
                filter(
                    lambda cdm_field: cdm_field["type"]
                    in ["decimal", "number", "integer"]
                    and cdm_field["name"] in all_fields,
                    self.cdm,
                )
            )
        ]

        resource = self.resource.to_copy()
        aggregate_dict: dict[str, Any] = {"num_records": len}
        aggregate_dict.update(
            {field: (field, check_field_gaps) for field in all_fields}
        )

        resource.transform(
            Pipeline(steps=[custom_aggregate(aggregation=aggregate_dict)])
        )
        field_gaps = resource.read_rows()[0].to_dict()

        num_records = field_gaps["num_records"]

        filled_fields = [
            field for field in all_fields if field_gaps[field] < num_records
        ]
        empty_fields = [
            field for field in all_fields if field_gaps[field] == num_records
        ]
        null_text_fields = [field for field in text_fields if field_gaps[field] > 0]
        null_numeric_fields = [
            field for field in numeric_fields if field_gaps[field] > 0
        ]

        num_fields = len(all_fields)

        # profile score------ filled rates
        num_filled_fields = len(filled_fields)
        filled_percent = self.get_percent(num_filled_fields, num_fields)

        # profile score------ empty fields
        num_empty_fields = len(empty_fields)
        empty_percent = self.get_percent(num_empty_fields, num_fields)

        # profile score------ Text Fields Empty
        num_text_fields = len(text_fields)
        num_null_text_fields = len(null_text_fields)
        null_text_percent = self.get_percent(num_null_text_fields, num_text_fields)

        # profile score------ number empty fields
        num_numeric_values = len(numeric_fields)
        num_null_numeric_fields = len(null_numeric_fields)
        null_numeric_percent = self.get_percent(
            num_null_numeric_fields, num_numeric_values
        )

        # profile score------ missing fields
        gapped_fields = []
        for field in filled_fields:
            # field_gap[emp_field] will always be >= 0 and < num_records as empty_fields are separate
            if field_gaps[field]:
                gapped_fields.append(field)

        missing_record_details = []
        for field in gapped_fields:
            num_not_null = num_records - field_gaps[field]
            missing_record_details.append(
                {field: self.get_percent(num_not_null, num_records)}
            )
        missing_records_percent = self.get_percent(
            num_filled_fields - len(gapped_fields), num_filled_fields
        )

        return [
            {
                "fieldsFilled": {
                    "percent": filled_percent,
                    "total": num_fields,
                    "filled": len(filled_fields),
                    "fields": filled_fields,
                }
            },
            {
                "fieldsEmpty": {
                    "percent": empty_percent,
                    "total": num_fields,
                    "empty": num_empty_fields,
                    "fields": empty_fields,
                }
            },
            {
                "textFieldsEmpty": {
                    "percent": null_text_percent,
                    "total": num_text_fields,
                    "empty": num_null_text_fields,
                    "fields": null_text_fields,
                }
            },
            {
                "numericFieldsEmpty": {
                    "percent": null_numeric_percent,
                    "total": num_numeric_values,
                    "empty": num_null_numeric_fields,
                    "fields": null_numeric_fields,
                }
            },
            {
                "missingRecords": {
                    "percent": missing_records_percent,
                    "total": num_filled_fields,
                    "empty": len(gapped_fields),
                    "fields": missing_record_details,
                }
            },
        ]
