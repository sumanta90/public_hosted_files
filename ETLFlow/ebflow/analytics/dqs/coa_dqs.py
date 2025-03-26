from frictionless import Pipeline

from ebflow.analytics.dqs.dqs import DQS
from ebflow.utils.custom_steps import custom_aggregate


class COADQS(DQS):
    def statistical(self):
        resource = self.resource.to_copy()
        resource.transform(
            Pipeline(steps=[custom_aggregate(aggregation={"num_records": len})])
        )

        data = resource.read_rows()[0]

        coa_total_record = data["num_records"]
        total_fields = len(self.resource.header)
        # COA --- statistical score -- glAccountNumber
        coa_gl_account_number_count = self.get_unique_value_count(["glAccountNumber"])
        # COA --- statistical score -- Number of Business Units
        coa_business_unit_code_count = self.get_unique_value_count(["businessUnitCode"])

        return [
            {"recordCount": float(coa_total_record)},
            {"fieldCount": float(total_fields)},
            {"glAccountCodeCount": float(coa_gl_account_number_count)},
            {"businessUnitCounts": float(coa_business_unit_code_count)},
        ]

    def business_rule(self):
        return []
