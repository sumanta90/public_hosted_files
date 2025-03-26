# import json
# import unittest

# from frictionless import Pipeline, steps
# from frictionless.resources import TableResource

# from ebflow.transform.create_grouped_trial_balance import create_grouped_trial_balance
# from tests.test_transform.data.grouping_data_for_test import grouping_data, joined_data, quality_scorecard


# class TestCreateGroupedTrialBalance(unittest.TestCase):
#     def test_create_grouped_trial_balance(self):
#         resource = TableResource(path='tests/test_transform/data/tb_data.csv')
#         resource.infer()
#         resource.schema.set_field_type('amountEnding', 'number')

#         cdm_resource = {'resource': resource, 'pipeline': Pipeline(steps=[steps.table_normalize()])}
#         grouped_resource_out_path = 'tests/test_transform/data/temp/grouped_trial_balance.csv'
#         grouping_response_out_path = 'tests/test_transform/data/temp/grouping_response.json'

#         create_grouped_trial_balance(cdm_resource, grouping_data, grouped_resource_out_path, grouping_response_out_path)

#         grouped_resource = TableResource(path=grouped_resource_out_path)
#         grouped_resource.infer()

#         required_header = ['glAccountName', 'glAccountNumber', 'amountEnding', 'description', 'accountType',
#                            'accountSubType', 'fsCaption', 'accountName', 'glMapNumber']
#         actual_header = grouped_resource.header
#         self.assertListEqual(actual_header, required_header)

#         # check for left join working or not
#         grouped_resource.transform(Pipeline(steps=[
#             steps.table_normalize(),
#             steps.field_filter(names=['glAccountNumber', 'accountType'])
#         ]))

#         actual_data = grouped_resource.read_rows()
#         for i, row in enumerate(actual_data):
#             self.assertDictEqual(row.to_dict(), joined_data[i])

#         with open(grouping_response_out_path) as f:
#             grouping_response = json.load(f)

#         actual_quality = grouping_response['data']['quality']
#         required_quality = quality_scorecard

#         self.assertDictEqual(actual_quality, required_quality)
