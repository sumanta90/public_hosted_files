table_details_for_test_data = {
    "gl_details_perfect": {
        "validation": {
            "NLNominalAccounts.AccountNumber": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLNominalAccounts.AccountCostCentre": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLNominalAccounts.AccountDepartment": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLNominalAccounts.AccountName": {"type-error": [], "constraint-error": []},
            "NLNominalAccounts.BroughtForwardBalance": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.UniqueReferenceNumber": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.URN": {"type-error": [], "constraint-error": []},
            "NLPostedNominalTrans.TransactionDate": {
                "type-error": [],
                "constraint-error": [],
            },
            "SYSAccountingPeriods.PeriodNumber": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.Reference": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.Narrative": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.GoodsValueInBaseCurrency": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.TransactionAnalysisCode": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLNominalAccounts.Balance": {"type-error": [], "constraint-error": []},
            "SYSCompanies.CompanyName": {"type-error": [], "constraint-error": []},
            "Custom1": {"type-error": [], "constraint-error": []},
            "Custom2": {"type-error": [], "constraint-error": []},
            "Custom3": {"type-error": [], "constraint-error": []},
            "Custom4": {"type-error": [], "constraint-error": []},
        }
    },
    "gl_details_imperfect": {
        "validation": {
            "NLNominalAccounts.AccountCostCentre": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLNominalAccounts.AccountDepartment": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLNominalAccounts.AccountName": {"type-error": [], "constraint-error": []},
            "NLNominalAccounts.BroughtForwardBalance": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.UniqueReferenceNumber": {
                "type-error": [],
                "constraint-error": ["14"],
            },
            "NLPostedNominalTrans.URN": {"type-error": [], "constraint-error": []},
            "NLPostedNominalTrans.TransactionDate": {
                "type-error": ["6"],
                "constraint-error": [],
            },
            "SYSAccountingPeriods.PeriodNumber": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.Reference": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.Narrative": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.GoodsValueInBaseCurrency": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLPostedNominalTrans.TransactionAnalysisCode": {
                "type-error": [],
                "constraint-error": [],
            },
            "NLNominalAccounts.Balance": {"type-error": [], "constraint-error": []},
            "SYSCompanies.CompanyName": {"type-error": [], "constraint-error": []},
            "Custom1": {"type-error": [], "constraint-error": []},
            "Custom2": {"type-error": [], "constraint-error": []},
            "Custom3": {"type-error": [], "constraint-error": []},
            "Custom4": {"type-error": [], "constraint-error": []},
        }
    },
    "CustomImperfect": {
        "validation": {
            "Sr.no": {"type-error": [], "constraint-error": []},
            "AccountName": {"type-error": [], "constraint-error": []},
            "Balance": {"type-error": ["5"], "constraint-error": []},
            "URN": {"type-error": [], "constraint-error": ["5"]},
            "TransactionDate": {"type-error": ["4", "6"], "constraint-error": []},
            "Narrative": {"type-error": [], "constraint-error": []},
            "GoodsValue": {"type-error": ["9"], "constraint-error": []},
            "Amount": {"type-error": ["3", "6"], "constraint-error": []},
            "Completed": {"type-error": ["7"], "constraint-error": []},
        }
    },
}
