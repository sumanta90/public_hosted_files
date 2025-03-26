from typing import Dict, List, Literal, Optional, Any

from pydantic import BaseModel, model_validator
from ebflow.utils.constants import Constants


## ------------------
## ERP CONFIG MODELS
## ------------------


class ErpFieldValidation(BaseModel):
    engineb_attribute: str
    input_field: str
    present: bool


class ExclusionRules(BaseModel):
    column: int
    contains: str
    exact: Optional[bool] = False


class ExclusionRulesSet(BaseModel):
    aggr: Optional[Literal["or", "and"]] = None
    rules: Optional[List[ExclusionRules]] = None
    expr: Optional[str] = None
    column_expr: Optional[Dict[str, ExclusionRules]] = None

    @model_validator(mode="before")
    def exclusion_is_present(cls, values):
        if values.get("aggr") and not values.get("rules"):
            raise ValueError("Invalid Exclusion Rule. 'rules' is required for aggr")
        if values.get("rules") and not values.get("aggr"):
            raise ValueError("Invalid Exclusion Rule. 'aggr' is required for rules")

        if values.get("expr") and not values.get("column_expr"):
            raise ValueError(
                "Invalid Exclusion Rule. 'column_expr' is required for expr"
            )
        if values.get("column_expr") and not values.get("expr"):
            raise ValueError(
                "Invalid Exclusion Rule. 'expr' is required for column_expr"
            )
        return values


class FileTemplate(BaseModel):
    start_row: int = 1
    sheet_name: Optional[str]
    exclusion: Optional[ExclusionRulesSet] = None


class ErpField(BaseModel):
    field_name: str
    fill_down: Optional[bool] = False
    data_type: str
    date_format: str = ""
    nullable: bool = True
    required: bool = False
    cdm_field_names: Optional[List[str]] = []

    @model_validator(mode="before")
    def date_format_is_present(cls, values):
        if values.get("data_type") not in Constants.SUPPORTED_DATATYPES:
            raise ValueError("Invalid Data Type")

        if values.get("date_format") not in list(Constants.DATETIME_FORMATS.keys()):
            if values.get("date_format") != "":
                raise ValueError("Invalid Data Format")

        if (
            values.get("data_type") == Constants.DATATYPE_DATETIME
            and values.get("date_format") == ""
        ):
            raise ValueError(
                "Date Format required for datetime datatype", values.get("field_name")
            )
        return values


class ErpMandatoryFieldCount(BaseModel):
    cdm_field_name: str
    occurrence_count: int = 0


class ErpFile(BaseModel):
    file_name: str
    fill_down: Optional[bool] = False
    conform_dates: bool = False
    encoding_format: Optional[str] = "utf-8"
    blank_replacement: bool = False
    template: Optional[FileTemplate] = None
    fields: List[ErpField]
    mandatory_field_check: List[ErpMandatoryFieldCount]


## --------------------------------
## FILE VALIDATION RESPONSE MODELS
## --------------------------------


class MandatoryFieldCheckResponse(BaseModel):
    valid: bool = False
    data: List[ErpFieldValidation] = []


class LogicCheckResponse(BaseModel):
    valid: bool = False
    comment: Optional[str] = ""
    data: Optional[List[str]] = []


class FieldCheckResponse(BaseModel):
    field_name: str = ""
    valid: bool = False
    present: bool = False
    null_check: Optional[LogicCheckResponse] = None
    type_check: Optional[LogicCheckResponse] = None
    data_type: str = ""


class ImperfectFileCheckSummary(BaseModel):
    name: str = ""
    valid: bool = False
    message: str = ""


class ImperfectFileCheckResponse(BaseModel):
    valid: bool
    summary: List[ImperfectFileCheckSummary]
    fields: Optional[List[dict]]


class FileCheckResponse(BaseModel):
    mfc: MandatoryFieldCheckResponse
    ifc: ImperfectFileCheckResponse


class EBStandardExtractionResponse(BaseModel):
    resource: Any
    pipeline: Any
    check_response: FileCheckResponse
    records: List[Dict[str, Any]]
    data_type: List[Dict[str, str]]
    view: Optional[str] = None
