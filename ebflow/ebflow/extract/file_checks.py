from typing import List, Optional

from frictionless.resources import TableResource
from frictionless import steps, Pipeline

from ebflow.utils.schemas import (
    ErpField,
    ErpMandatoryFieldCount,
    ErpFieldValidation,
    MandatoryFieldCheckResponse,
    LogicCheckResponse,
    FieldCheckResponse,
    ImperfectFileCheckResponse,
    ImperfectFileCheckSummary,
    ErpFile,
    FileCheckResponse,
)


class FileChecks:

    def __build_check_summary(
        self, field_validation_result: List[FieldCheckResponse]
    ) -> List[ImperfectFileCheckSummary]:
        missing_column_count = 0
        missing_data_count = 0
        invalid_data_type_count = 0

        for field in field_validation_result:
            if not field.valid:
                if not field.present:
                    missing_column_count += 1
                else:
                    if not field.null_check.valid:
                        missing_data_count += len(field.null_check.data)
                    if not field.type_check.valid:
                        invalid_data_type_count += len(field.type_check.data)

        missing_column = ImperfectFileCheckSummary(
            name="Missing Column",
            valid=missing_column_count == 0,
            message=f"{'No' if missing_column_count == 0 else str(missing_column_count)} Missing column{'' if missing_column_count == 1 else 's'} identified",
        )

        missing_data = ImperfectFileCheckSummary(
            name="Missing Data",
            valid=missing_data_count == 0,
            message=f"{'No' if missing_data_count == 0 else str(missing_data_count)} Missing Data element{'' if missing_data_count == 1 else 's'} identified",
        )

        invalid_data_type = ImperfectFileCheckSummary(
            name="Data Format",
            valid=invalid_data_type_count == 0,
            message=f"{'No' if invalid_data_type_count == 0 else str(invalid_data_type_count)} Data format compatibility issue{'' if invalid_data_type_count == 1 else 's'} identified",
        )

        return [missing_data, missing_column, invalid_data_type]

    def __reformat_report(self, report: list[list], fields: list[dict]):
        errors = {}
        column_names = []

        for field in fields:
            errors[field.title] = {"type-error": [], "constraint-error": []}
            column_names.append(field.title)

        # each e in report = [cell_value, row_number, field_name, error_type]
        # type_error corresponds to cells which do not follow the resource Schema field type and format
        # constraint_error corresponds to the nullable constraint specified in the Schema fields
        for e in report:
            error_type = e[-1]
            if error_type in ["type-error", "constraint-error"]:
                column_name = column_names[int(e[2]) - 1]
                row_number = int(e[1] - 1)
                errors[column_name][error_type].append(str(row_number))

        return errors

    def __flatten_dict(self, data: dict, parent_key: str = ""):
        """Recursively flatten nested dictionaries. Keys will be joined by _"""
        items = {}
        for k, v in data.items():
            new_key = f"{parent_key}_{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self.__flatten_dict(v, new_key))
            else:
                items[new_key] = v
        return items

    def imperfect_file_check(self, resource: TableResource, erp_fields: List[ErpField]):

        report = resource.validate()
        errors = self.__reformat_report(report.flatten(), resource.schema.fields)

        field_check = None
        valid = True
        check_response = []

        for erp_field in erp_fields:

            try:
                if not errors.get(erp_field.field_name):
                    raise KeyError

                if erp_field.nullable:
                    null_check = LogicCheckResponse(
                        valid=True,
                        comment="Null values allowed",
                    )
                else:
                    null_rows = errors[erp_field.field_name]["constraint-error"]
                    null_valid = len(null_rows) == 0
                    null_check = LogicCheckResponse(
                        valid=null_valid,
                        comment=(
                            "No Null Values found"
                            if null_valid
                            else f"Contains {len(null_rows)} null values"
                        ),
                        data=null_rows,
                    )
                invalid_rows = errors[erp_field.field_name]["type-error"]
                dt_invalid_length = len(invalid_rows)
                dt_valid = len(invalid_rows) == 0
                type_check = LogicCheckResponse(
                    valid=dt_valid,
                    comment=(
                        "All rows have valid data."
                        if dt_valid
                        else f"Contains {dt_invalid_length} row{'' if dt_invalid_length == 1 else 's'} with invalid data type"
                    ),
                    data=invalid_rows if not dt_valid else [],
                )

                field_check = FieldCheckResponse(
                    field_name=erp_field.field_name,
                    valid=null_check.valid and type_check.valid,
                    present=True,
                    null_check=null_check,
                    type_check=type_check,
                    data_type=erp_field.data_type,
                )

            except KeyError:
                field_check = FieldCheckResponse(
                    field_name=erp_field.field_name,
                    valid=not erp_field.required,
                    present=False,
                    data_type=erp_field.data_type,
                )
            except Exception:
                field_check = FieldCheckResponse(
                    field_name=erp_field.field_name,
                    valid=False,
                    present=False,
                    data_type=erp_field.data_type,
                )
            finally:
                valid = valid and field_check.valid
                check_response.append(field_check)

        field_dictionaries = [
            self.__flatten_dict(resp.model_dump()) for resp in check_response
        ]
        field_dictionaries = sorted(
            field_dictionaries, key=lambda x: x.get("field_name", "zzzzzzzz")
        )

        summary: list[ImperfectFileCheckSummary] = self.__build_check_summary(
            check_response
        )
        return ImperfectFileCheckResponse(
            valid=valid, summary=summary, fields=field_dictionaries
        )

    def mandatory_field_check(
        self,
        input_headers: List[str],
        field_config: List[ErpField],
        occurrence_count: List[ErpMandatoryFieldCount],
    ) -> MandatoryFieldCheckResponse:

        mfc_valid = True
        check_details = []

        for cdm_field_details in occurrence_count:

            cdm_field = cdm_field_details.cdm_field_name

            fields_required = list(
                filter(lambda field: cdm_field in field.cdm_field_names, field_config)
            )
            if len(fields_required) == 0:
                continue

            for field_required in fields_required:
                cf = field_required.field_name

                present = False
                input_field = cf
                multiple_match = False
                for header in input_headers:
                    if cf.lower() not in header.lower():
                        continue

                    if cf.lower() == header.lower():
                        input_field = header
                        present = True
                        break

                    if present or multiple_match:
                        input_field = cf
                        present = False
                        multiple_match = True
                    else:
                        input_field = header
                        present = True

                check_details.append(
                    ErpFieldValidation(
                        engineb_attribute=cdm_field,
                        input_field=input_field,
                        present=present,
                    )
                )

                mfc_valid = mfc_valid and present

        return MandatoryFieldCheckResponse(valid=mfc_valid, data=check_details)

    def validate_resource(
        self, resource: TableResource, pipeline: Pipeline, file_config: ErpFile
    ) -> FileCheckResponse:

        resource.transform(pipeline)

        header = resource.header
        mandatory_fields = list(
            filter(lambda field: len(field.cdm_field_names) > 0, file_config.fields)
        )
        occurrence_count = file_config.mandatory_field_check

        mfc = self.mandatory_field_check(header, mandatory_fields, occurrence_count)
        ifc = self.imperfect_file_check(resource, file_config.fields)
        file_check_response = FileCheckResponse(ifc=ifc, mfc=mfc)
        return file_check_response

    def validate_resource_head(
        self,
        resource: TableResource,
        pipeline: Optional[Pipeline],
        file_config: ErpFile,
        n_rows: int = 100,
    ) -> [FileCheckResponse, str]:

        if pipeline:
            pipeline_copy = pipeline.to_copy()
            pipeline_copy.steps[1] = steps.row_slice(head=n_rows)
        else:
            pipeline_copy = Pipeline(steps=[steps.row_slice(head=n_rows)])

        file_check_response = self.validate_resource(
            resource, pipeline_copy, file_config
        )

        resource_view = resource.to_view(type="lookall")
        return file_check_response, resource_view
