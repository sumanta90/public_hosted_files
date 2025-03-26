from ebflow.extract.file_manager import FileManager
from frictionless.resources import TableResource
import json 

jd = {
    "nodes":[

        {
            "id": "1",
            "data": {
                "type": "extract",
                "file_name": "trialBalance.csv",
                "file_path": "https://stengbuksdevuk.blob.core.windows.net/auditfirma/data-ingestions/clteitahi214301nl9smjslw0/share/zip/trialBalance.csv"

            }
        },
        {
            "id": "2",
            "data": {
                "type": "transform",
                "operation_name": "count",
                "column_name": "amountEnding",
                "unique": True,
                "input_columns": ["amountEnding"]
            }
        },
        {
            "id": "6",
            "data": {
                "type": "load",
                "file_name": "op2.csv",
                "file_path": "https://stengbuksdevuk.blob.core.windows.net/auditfirma/data-ingestions/clteitahi214301nl9smjslw0/share/zip/dna/op2.csv"
            }
        }
    ],
    "edges":[
        {
            "id": "1",
            "source": "1",
            "target": "2"
        },
        {
            "id": "2",
            "source": "2",
            "target": "6"
        }
    ]
}

from ebflow.analytics.analytics import AnalyticsNew
from ebflow.analytics.analytics_schema import AnalyticsPipeline

try:
    AnalyticsPipeline.parse_obj(jd)
except Exception as e:
    raise Exception(e)
else:
    an = AnalyticsNew(json.dumps(jd))

    res = an.process()
    print(res)