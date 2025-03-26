import json

from frictionless import Pipeline, steps
from frictionless.resources import TableResource

from ebflow.utils.cdm_conversion_exception import CDMConversionException
from ebflow.utils.custom_steps import value_counts, custom_aggregate, custom_sum


def evaluate_groupings(resource: TableResource):
    try:
        grouping_info = {
            "assets": {
                "search_string": ["asset"],
            },
            "liabilities": {
                "search_string": ["liabilities", "liability"],
            },
            "equity": {
                "search_string": ["equity"],
            },
            "income": {
                "search_string": ["income"],
            },
            "expenditure": {
                "search_string": ["expense"],
            },
        }

        resource.infer()

        total_accounts = resource.to_copy().transform(
            Pipeline(
                steps=[
                    steps.row_filter(
                        function=lambda row: all(
                            [row[field] for field in ["glAccountNumber"]]
                        )
                    ),
                    value_counts(field_names=["glAccountNumber"]),
                    custom_aggregate(aggregation={"row_count": len}),
                ]
            )
        )
        data = total_accounts.read_rows()
        total_accounts = int(data[0]["row_count"]) if data else 0

        if "accountType" in resource.header and "account" not in resource.header:
            resource.schema.get_field("accountType").name = "account"

        mapped_account = resource.to_copy().transform(
            Pipeline(
                steps=[
                    steps.row_filter(
                        function=lambda row: all(
                            [
                                row[field]
                                for field in ["glAccountNumber", "glAccountName"]
                            ]
                        )
                    ),
                    value_counts(field_names=["glAccountNumber", "glAccountName"]),
                    custom_aggregate(aggregation={"row_count": len}),
                ]
            )
        )
        data = mapped_account.read_rows()
        mapped_account = int(data[0]["row_count"]) if data else 0

        unique_accounts = resource.to_copy().transform(
            Pipeline(
                steps=[
                    steps.table_normalize(),
                    value_counts(field_names=["account"]),
                ]
            )
        )
        unique_accounts = set(
            [acc["account"] for acc in unique_accounts.read_rows()]
        ) - {None}

        for _, value in grouping_info.items():
            search_strings = value["search_string"]

            accounts_to_search = []
            for account in unique_accounts:
                for to_search in search_strings:
                    if to_search in account.lower():
                        accounts_to_search.append(account)

            account_data = resource.to_copy().transform(
                Pipeline(
                    steps=[
                        steps.table_normalize(),
                        steps.row_filter(
                            function=lambda row: row["account"] in accounts_to_search
                        ),
                    ]
                )
            )

            amount_total = account_data.to_copy().transform(
                Pipeline(
                    steps=[
                        custom_aggregate(
                            group_name=None,
                            aggregation={"amountEnding": ("amountEnding", custom_sum)},
                        )
                    ]
                )
            )
            amount_total = amount_total.read_rows()
            amount_total = float(amount_total[0]["amountEnding"]) if amount_total else 0

            mapped_accounts = account_data.to_copy().transform(
                Pipeline(
                    steps=[
                        steps.row_filter(
                            function=lambda row: all(
                                [
                                    row[field]
                                    for field in ["glAccountNumber", "glAccountName"]
                                ]
                            )
                        ),
                        value_counts(field_names=["glAccountNumber", "glAccountName"]),
                        custom_aggregate(aggregation={"row_count": len}),
                    ]
                )
            )
            data = mapped_accounts.read_rows()
            mapped_accounts = int(data[0]["row_count"]) if data else 0

            value["accounts"] = accounts_to_search
            value["total_value"] = round(amount_total, 3)
            value["mapped"] = mapped_accounts

        quality_scorecard = {
            "total_codes": total_accounts,
            "mapped_codes": mapped_account,
            "account_detail": grouping_info,
        }

        return quality_scorecard
    except Exception as e:
        error_data = {
            "failed_function": "evaluate_grouping",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
