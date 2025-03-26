from typing import Optional, List, Union, BinaryIO

from frictionless import describe, Dialect, steps, formats, Schema, Pipeline, fields
from frictionless.resources import TableResource

from ebflow.extract.file_checks import FileChecks
from ebflow.utils.constants import Constants
from ebflow.utils.custom_steps import fill_down

from ebflow.utils.schemas import (
    ErpField,
    ExclusionRulesSet,
    ErpFile,
    EBStandardExtractionResponse,
)


class FileManager:
    def __exclusion_rules_legacy(self, row) -> bool:
        exclusion_values = []

        for rule in self.exclusion.rules:
            data = row[rule.column - 1]
            contains_null = (
                str(rule.contains).lower().replace("_", "").replace(" ", "") == "null"
            )
            contains_not_null = (
                str(rule.contains).lower().replace("_", "").replace(" ", "")
                == "notnull"
            )

            if contains_null:
                rule_result = data in ("", None)
            elif contains_not_null:
                rule_result = data not in ("", None)
            else:
                if rule.exact:
                    rule_result = str(data) == rule.contains
                else:
                    rule_result = rule.contains in str(data)
            exclusion_values.append(rule_result)

        result = (
            all(exclusion_values)
            if self.exclusion.aggr == "and"
            else any(exclusion_values)
        )
        return not result

    def __exclusion_rules(self, row) -> bool:
        exclusion = self.exclusion.model_dump()
        expression = str(exclusion["expr"]).lower()

        for index, rule in exclusion["column_expr"].items():
            data = row[rule["column"] - 1]
            contains_null = (
                str(rule["contains"]).lower().replace("_", "").replace(" ", "")
                == "null"
            )
            contains_not_null = (
                str(rule["contains"]).lower().replace("_", "").replace(" ", "")
                == "notnull"
            )

            if contains_null:
                rule_result = data in ("", None)
            elif contains_not_null:
                rule_result = data not in ("", None)
            else:
                if rule["exact"]:
                    rule_result = str(data) == rule["contains"]
                else:
                    rule_result = rule["contains"] in str(data)

            expression = expression.replace(index, str(rule_result))

        result = eval(expression)
        return not result

    def __exclude_rows(self, exclusion_rule_set: ExclusionRulesSet):
        if exclusion_rule_set:
            self.exclusion = exclusion_rule_set
            if exclusion_rule_set.aggr:
                return [steps.row_filter(function=self.__exclusion_rules_legacy)]
            elif exclusion_rule_set.expr:
                return [steps.row_filter(function=self.__exclusion_rules)]
        else:
            return []

    @staticmethod
    def __clean_data():
        clean_steps = [
            steps.cell_replace(pattern="", replace=""),
            steps.cell_replace(pattern=" ", replace=""),
            steps.cell_replace(pattern="  ", replace=""),
            steps.cell_replace(pattern="BLANK", replace=""),
            steps.cell_replace(pattern="blank", replace=""),
            steps.cell_replace(pattern="NULL", replace=""),
            steps.cell_replace(pattern="null", replace=""),
        ]
        return clean_steps

    @staticmethod
    def __get_fill_down(fill_down_options):
        field_names = [
            field["name"] for field in fill_down_options if field["fill_down"]
        ]
        fill_down_steps = [fill_down(field_names=field_names)]
        return fill_down_steps

    @staticmethod
    def __modify_columns(resource: TableResource):
        unnamed_field_count = 0
        for field in resource.schema.fields:
            if "field" in field.name.lower():
                unnamed_field_count += 1
                field.name = f"NO_NAME{unnamed_field_count}"

    @staticmethod
    def __slice_data(n_rows: int = 0):
        return [steps.row_slice(head=max(0, n_rows))]

    @staticmethod
    def __add_data_type_details(header: str, erp_field: ErpField):

        if erp_field.date_format and Constants.DATETIME_FORMATS.get(
            erp_field.date_format
        ):
            date_format = Constants.DATETIME_FORMATS[erp_field.date_format]
        else:
            date_format = "any"

        if erp_field.data_type == Constants.DATATYPE_STRING:
            field = fields.StringField(
                name=header,
                title=erp_field.field_name,
                constraints={"required": not erp_field.nullable},
                description=f'This column {"should" if str(erp_field.required) else "need not"} be present',
            )
        elif erp_field.data_type == Constants.DATATYPE_BOOLEAN:
            field = fields.BooleanField(
                name=header,
                title=erp_field.field_name,
                constraints={"required": not erp_field.nullable},
                description=f'This column {"should" if str(erp_field.required) else "need not"} be present',
            )
        elif erp_field.data_type in [
            Constants.DATATYPE_NUMBER,
            Constants.DATATYPE_DECIMAL,
            Constants.DATATYPE_CURRENCY,
        ]:
            field = fields.NumberField(
                name=header,
                title=erp_field.field_name,
                constraints={"required": not erp_field.nullable},
                description=f'This column {"should" if str(erp_field.required) else "need not"} be present',
                group_char=",",
                float_number=True,
                bare_number=not erp_field.data_type == Constants.DATATYPE_CURRENCY,
            )
        elif erp_field.data_type == Constants.DATATYPE_DATE:
            field = fields.DateField(
                name=header,
                title=erp_field.field_name,
                constraints={"required": not erp_field.nullable},
                description=f'This column {"should" if str(erp_field.required) else "need not"} be present',
                format=date_format,
            )
        elif erp_field.data_type == Constants.DATATYPE_TIME:
            field = fields.TimeField(
                name=header,
                title=erp_field.field_name,
                constraints={"required": not erp_field.nullable},
                description=f'This column {"should" if str(erp_field.required) else "need not"} be present',
                format=date_format,
            )
        elif erp_field.data_type == Constants.DATATYPE_DATETIME:
            field = fields.DatetimeField(
                name=header,
                title=erp_field.field_name,
                constraints={"required": not erp_field.nullable},
                description=f'This column {"should" if str(erp_field.required) else "need not"} be present',
                format=date_format,
            )
        else:
            field = fields.AnyField(
                name=header,
                title=erp_field.field_name,
                constraints={"required": not erp_field.nullable},
                description=f'This column {"should" if str(erp_field.required) else "need not"} be present',
            )
        return field

    def __generate_schema(self, erp_fields: List[ErpField], resource: TableResource):
        schema = Schema(
            fields=[],
            missing_values=["", " ", "None", "BLANK", "blank", "NULL", "null"],
        )
        for header in resource.header:
            erp_field = None
            match_length = 0
            for probable_field in erp_fields:
                if probable_field.field_name.lower() in header.lower():
                    new_match_length = len(probable_field.field_name)
                    if new_match_length > match_length:
                        erp_field = probable_field
                        match_length = new_match_length
            if erp_field:
                schema_field = self.__add_data_type_details(header, erp_field)
            else:
                schema_field = fields.AnyField(name=header, title=header)
            schema.add_field(schema_field)

        return schema

    def read_file(
        self,
        content: Union[str, bytes, BinaryIO],
        content_format: str = None,
        innerpath: str = None,
        n_rows: int = 0,
        clean_data: bool = False,
        fill_down_options: [dict[str, bool]] = [],
        file_config: ErpFile = None,
    ) -> (Optional[TableResource], Optional[Pipeline]):
        """
        Reads a file to return a frictionless resource.

            :param content:
            :param content_format:
            :param innerpath:
            :param n_rows:
            :param clean_data:
            :param fill_down_options:
            :param file_config:

            :returns frictionless resource:
        """

        try:
            table_schema = describe(content)
        except Exception:
            return None, None

        if table_schema.scheme == "stream" and not content_format:
            return None, None

        sheet_name = file_config.template.sheet_name if file_config else None
        start_row = max(1, file_config.template.start_row) if file_config else 1

        if content_format:
            resource_format = content_format
        else:
            resource_format = (
                "csv" if table_schema.format == "txt" else table_schema.format
            )
        resource_encoding = table_schema.encoding

        dialect = Dialect(
            comment_rows=list(range(1, start_row)),
            skip_blank_rows=True,
        )
        control = formats.ExcelControl(sheet=sheet_name) if sheet_name else None

        resource_file = TableResource(
            source=content,
            format=resource_format,
            innerpath=innerpath,
            encoding=resource_encoding,
            dialect=dialect,
            control=control,
        )
        pipeline = Pipeline(steps=[])

        # in case of empty json file
        try:
            resource_file.infer(stats=True)
        except Exception:
            return None, None

        # Check for empty csv
        if not resource_file.schema.fields:
            return None, None

        # normalizing before any changes
        pipeline.steps.append(steps.table_normalize())

        if file_config and file_config.fields:
            resource_file.schema = self.__generate_schema(
                file_config.fields, resource_file
            )

        slice_steps = self.__slice_data(n_rows=n_rows)
        pipeline.steps.extend(slice_steps)

        if clean_data:
            clean_steps = self.__clean_data()
            pipeline.steps.extend(clean_steps)

        if fill_down_options:
            fill_down_steps = self.__get_fill_down(fill_down_options)
            pipeline.steps.extend(fill_down_steps)

        if file_config:
            exclude_steps = self.__exclude_rows(
                exclusion_rule_set=file_config.template.exclusion
            )
            pipeline.steps.extend(exclude_steps)

        self.__modify_columns(resource=resource_file)

        return resource_file, pipeline

    def clean_data(self):
        return self.__clean_data()

    def get_fill_down_steps(self, fill_down_options):
        return self.__get_fill_down(fill_down_options)

    def read_and_validated_resource(
        self,
        content: Union[str, bytes, BinaryIO],
        content_format: str,
        innerpath: str = None,
        n_rows: int = 0,
        clean_data: bool = False,
        fill_down_options: [dict[str, bool]] = [],
        file_config: ErpFile = None,
        full_validation: bool = False,
    ) -> EBStandardExtractionResponse:

        resource_file, pipeline = self.read_file(
            content,
            content_format,
            innerpath,
            n_rows,
            clean_data,
            fill_down_options,
            file_config,
        )

        resource_copy = resource_file.to_copy()
        file_checks = FileChecks()
        view = None
        if full_validation:
            file_check_response = file_checks.validate_resource(
                resource_copy, pipeline, file_config
            )
        else:
            file_check_response, view = file_checks.validate_resource_head(
                resource_copy, pipeline, file_config
            )

        records = resource_copy.read_rows()
        data_types = [
            {"field": field.name, "value": field.type}
            for field in resource_copy.schema.fields
        ]

        extract = EBStandardExtractionResponse(
            resource=resource_file,
            pipeline=pipeline,
            check_response=file_check_response,
            records=records,
            data_type=data_types,
            view=view,
        )

        return extract
