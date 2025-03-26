from frictionless import Pipeline

from ebflow.analytics.dqs.dqs import DQS
from ebflow.utils.custom_steps import custom_aggregate, custom_sum


class TBDQS(DQS):
    def statistical(self):
        resource = self.resource.to_copy()
        resource.transform(
            Pipeline(
                steps=[
                    custom_aggregate(
                        aggregation={
                            "num_records": len,
                            "amount_beginning_sum": ("amountBeginning", custom_sum),
                            "amount_ending_sum": ("amountEnding", custom_sum),
                        }
                    )
                ]
            )
        )

        data = resource.read_rows()[0]

        # TB --statistical score-- record count -------
        total_record_trail_bal = data["num_records"]
        total_fields = len(self.resource.header)

        # TB --statistical score-- Amount Beginning and Ending Sum -------
        amount_beginning_sum = data["amount_beginning_sum"]
        amount_ending_sum = data["amount_ending_sum"]

        # TB --statistical score-- glAccountNumber -------
        gl_account_code_count = self.get_unique_value_count(["glAccountNumber"])

        # TB --statistical score-- Number of Business Units -------
        business_unit_code_count = self.get_unique_value_count(["businessUnitCode"])

        amount_ending_sum = (
            0 if -0.000001 < amount_ending_sum < 0.000001 else amount_ending_sum
        )
        return [
            {"recordCount": float(round(total_record_trail_bal, 5))},
            {"fieldCount": float(total_fields)},
            {"amountBeginningSum": float(round(amount_beginning_sum, 5))},
            {"amountEndingSum": float(round(amount_ending_sum, 5))},
            {"glAccountCodeCount": float(round(gl_account_code_count, 5))},
            {"businessUnitCounts": float(round(business_unit_code_count, 5))},
        ]

    def business_rule(self):
        resource = self.resource.to_copy()
        resource.transform(
            Pipeline(
                steps=[
                    custom_aggregate(
                        aggregation={"amount_ending_sum": ("amountEnding", custom_sum)}
                    )
                ]
            )
        )

        sum_net_amount = resource.read_rows()[0]["amount_ending_sum"]

        # TB --business rule score- netting  -------
        return [{"netting": "pass" if -0.001 <= sum_net_amount <= 0.001 else "fail"}]
