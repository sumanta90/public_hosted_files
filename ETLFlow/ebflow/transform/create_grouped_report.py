import json

from frictionless import Pipeline, steps
from frictionless.resources import TableResource

from ebflow.analytics.evaluate_groupings import evaluate_groupings
from ebflow.utils.cdm_conversion_exception import CDMConversionException
from ebflow.utils.custom_steps import custom_table_join


def create_grouped_trial_balance(
    cdm_tb_output, fetched_grouping, path_to_zip, grouping_file_path
):
    try:
        grouping_response = (
            fetched_grouping
            if fetched_grouping
            else {"data": {"nominalCodeMappings": None}}
        )
        if not grouping_response["data"]["nominalCodeMappings"]:
            return None

        def trim_account_name(account_name):
            account_name = str(account_name).lower().strip()
            if not account_name:
                return
            return (
                account_name[2:]
                if account_name.startswith("- ")
                else account_name[1:] if account_name.startswith("-") else account_name
            ).lower()

        grouping_data = grouping_response["data"]["nominalCodeMappings"]["grouping"]
        grouping_resource = TableResource(source=grouping_data)
        grouping_resource.infer()

        grouping_resource.schema.set_field_type("nominalCode", "string")
        grouping_resource.transform(
            Pipeline(
                steps=[
                    steps.cell_replace(pattern="", replace=""),
                    steps.cell_replace(pattern="nan", replace=""),
                    steps.cell_replace(pattern="None", replace=""),
                    steps.field_add(
                        name="cleaned_description",
                        function=lambda row: trim_account_name(row["description"]),
                    ),
                ]
            )
        )

        resource: TableResource = cdm_output['resource'].to_copy()
        pipeline = cdm_output['pipeline']
        resource.schema.set_field_type('glAccountNumber', 'string')
        pipeline.steps.extend([
            steps.cell_replace(pattern='', replace=''),
            steps.cell_replace(pattern='nan', replace=''),
            steps.cell_replace(pattern='None', replace=''),
            steps.field_add(name='cleaned_glAccountName', function=lambda row: trim_account_name(row['glAccountName'])),
            custom_table_join(resource=grouping_resource, mode='left',
                              left_fields=['glAccountNumber', 'cleaned_glAccountName'],
                              right_fields=['nominalCode', 'cleaned_description']),
            steps.field_remove(names=['cleaned_glAccountName'])
        ])
        resource.transform(pipeline)

        resource.write(path_to_zip)

        quality_scorecard = evaluate_groupings(resource)

        grouping_response["data"]["quality"] = quality_scorecard
        grouping_response["data"]["grouping"] = list(grouping_resource.read_rows())

        with open(grouping_file_path, "w") as outfile:
            json.dump(grouping_response, outfile)
            print("Grouping created, evaluated, and saved!!!")

    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        error_data = {
            'failed_function': 'create_grouped_report.evaluate_grouping',
            'called_method': error_info['failed_function'],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            'failed_function': 'create_grouped_report',
            'called_method': None,
            'message': str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
