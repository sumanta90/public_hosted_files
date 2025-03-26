grouping_data = {
    "data": {
        "nominalCodeMappings": {
            "grouping": [
                {
                    "nominalCode": "2100",
                    "description": "-Freehold Property",
                    "accountType": "A. Assets",
                    "accountSubType": "II. Noncurrent Assets",
                    "fsCaption": "Tangible fixed assets",
                    "accountName": "L/Term L/hold property-depn b/fwd",
                    "glMapNumber": "A01.02.00.51",
                },
                {
                    "nominalCode": "3100",
                    "description": "- Lease Property",
                    "accountType": "A. Assets",
                    "accountSubType": "II. Noncurrent Assets",
                    "fsCaption": "Tangible fixed assets",
                    "accountName": "L/Term L/hold property-depn b/fwd",
                    "glMapNumber": "A01.02.00.51",
                },
                {
                    "nominalCode": "4100",
                    "description": "Plant/Machinery - Cost",
                    "accountType": "E. Expenses",
                    "accountSubType": "I. Cost of Sales",
                    "fsCaption": "Cost of goods sold",
                    "accountName": "Purchases - finished goods",
                    "glMapNumber": "M01.63.00.23",
                },
                {
                    "nominalCode": "4200",
                    "description": "Plant/Machinery - Depr.",
                    "accountType": "A. Assets",
                    "accountSubType": "II. Noncurrent Assets",
                    "fsCaption": "Tangible fixed assets",
                    "accountName": "L/Term L/hold property-depn b/fwd",
                    "glMapNumber": "A01.02.00.51",
                },
                {
                    "nominalCode": "6100",
                    "description": "Office Equipment - Cost",
                    "accountType": "A. Assets",
                    "accountSubType": "II. Noncurrent Assets",
                    "fsCaption": "Tangible fixed assets",
                    "accountName": "Computer equipment-depn b/fwd",
                    "glMapNumber": "A01.08.00.51",
                },
                {
                    "nominalCode": "6200",
                    "description": "Office Equipment - Depr.",
                    "accountType": "A. Assets",
                    "accountSubType": "II. Noncurrent Assets",
                    "fsCaption": "Tangible fixed assets",
                    "accountName": "Computer equipment-depn b/fwd",
                    "glMapNumber": "A01.08.00.51",
                },
                {
                    "nominalCode": "99999",
                    "description": "Accumulated Profit",
                    "accountType": "A. Assets",
                    "accountSubType": "II. Noncurrent Assets",
                    "fsCaption": "Tangible fixed assets",
                    "accountName": "Motor vehicles-depn b/fwd",
                    "glMapNumber": "A01.05.00.51",
                },
            ],
            "metadata": {
                "groupingName": "Test Rudra",
                "version": "Version:1",
                "lastUpdatedBy": "maharudra@engineb.com",
            },
        }
    }
}

quality_scorecard = {
    "total_codes": 6,
    "mapped_codes": 6,
    "account_detail": {
        "assets": {
            "search_string": ["asset"],
            "accounts": ["A. Assets"],
            "total_value": 130,
            "mapped": 4,
        },
        "liabilities": {
            "search_string": ["liabilities", "liability"],
            "accounts": [],
            "total_value": 0,
            "mapped": 0,
        },
        "equity": {
            "search_string": ["equity"],
            "accounts": [],
            "total_value": 0,
            "mapped": 0,
        },
        "income": {
            "search_string": ["income"],
            "accounts": [],
            "total_value": 0,
            "mapped": 0,
        },
        "expenditure": {
            "search_string": ["expense"],
            "accounts": ["E. Expenses"],
            "total_value": 100,
            "mapped": 1,
        },
    },
}

joined_data = [
    {"glAccountNumber": 2100, "accountType": "A. Assets"},
    {"glAccountNumber": 3100, "accountType": "A. Assets"},
    {"glAccountNumber": 4100, "accountType": "E. Expenses"},
    {"glAccountNumber": 4100, "accountType": "E. Expenses"},
    {"glAccountNumber": 4200, "accountType": "A. Assets"},
    {"glAccountNumber": 6100, "accountType": "A. Assets"},
    {"glAccountNumber": 6200, "accountType": None},
]
