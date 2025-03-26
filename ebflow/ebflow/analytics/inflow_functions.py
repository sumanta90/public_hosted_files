import json
from collections import OrderedDict

from frictionless import steps, Pipeline
from frictionless.resources import TableResource

from ebflow.utils.cdm_conversion_exception import CDMConversionException
from ebflow.utils.utils import adjust_filepath


def generate_inflow_general_ledger(
    cdm_gl_output: TableResource,
    generate_inflow: bool = True,
    temp_path: str = "",
    is_local: bool = True,
):
    try:
        if not generate_inflow:
            return None

        # Preparation
        inflow_gl_columns = OrderedDict(
            {
                "glAccountNumber": "Account Code",
                "journalId": "Transaction Id",
                "amount": "Net",
                "effectiveDate": "Effective Date",
                "enteredDateTime": "Created Date/Time",
                "journalEntryType": "Document Type",
                "enteredBy": "User Id",
                "journalIdLineNumber": "Reference",
                "jeHeaderDescription": "Journal Description",
                "jeLineDescription": "Line Description",
            }
        )

        # frictionless code
        inflow_gl_output = cdm_gl_output.to_copy()

        inflow_gl_output.schema.get_field("effectiveDate").format = "%d/%m/%Y"
        inflow_gl_output.schema.get_field("enteredDateTime").format = (
            "%d/%m/%Y %H:%M:%S"
        )

        inflow_gl_output.transform(
            Pipeline(
                steps=[
                    steps.field_update(
                        name="amount",
                        formula='amount if amountCreditDebitIndicator=="D" else -1 * amount',
                    ),
                    steps.field_filter(names=list(inflow_gl_columns.keys())),
                ]
            )
        )

        for field_name, new_field_name in inflow_gl_columns.items():
            inflow_gl_output.schema.get_field(field_name).name = new_field_name

        # output
        inflow_gl_output_path = adjust_filepath(
            f"{temp_path}Inflow_General_Ledger.csv", is_local
        )
        inflow_gl_output.write(inflow_gl_output_path)

        return inflow_gl_output_path
    except Exception as e:
        error_data = {
            "failed_function": "generate_inflow_general_ledger",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def generate_inflow_trial_balance(
    cdm_tb_output: TableResource,
    generate_inflow: bool = True,
    temp_path: str = "",
    is_local: bool = True,
):
    try:
        if not generate_inflow:
            return None

        # Preparation
        inflow_tb_columns = {
            "glAccountNumber": "AccountCode",
            "glAccountName": "AccountDescription",
            "amountBeginning": "OpeningNet",
            "amountEnding": "ClosingNet",
        }

        inflow_tb_open = cdm_tb_output.to_copy()
        inflow_tb_close = cdm_tb_output.to_copy()

        # filtering only required columns
        inflow_tb_open_fields = [
            field for field in inflow_tb_columns if field != "amountEnding"
        ]
        inflow_tb_open.transform(
            Pipeline(steps=[steps.field_filter(names=inflow_tb_open_fields)])
        )

        inflow_tb_close_fields = [
            field for field in inflow_tb_columns if field != "amountBeginning"
        ]
        inflow_tb_close.transform(
            Pipeline(steps=[steps.field_filter(names=inflow_tb_close_fields)])
        )

        for field_name, new_field_name in inflow_tb_columns.items():
            if field_name != "amountEnding":
                inflow_tb_open.schema.get_field(field_name).name = new_field_name
            if field_name != "amountBeginning":
                inflow_tb_close.schema.get_field(field_name).name = new_field_name

        # output
        inflow_tb_open_path = adjust_filepath(
            f"{temp_path}Inflow_Opening_Trial_Balance.csv", is_local
        )
        inflow_tb_close_path = adjust_filepath(
            f"{temp_path}Inflow_Closing_Trial_Balance.csv", is_local
        )

        inflow_tb_open.write(inflow_tb_open_path)
        inflow_tb_close.write(inflow_tb_close_path)

        return [inflow_tb_open_path, inflow_tb_close_path]

    except Exception as e:
        error_data = {
            "failed_function": "generate_inflow_trial_balance",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
