class Constants:
    DATATYPE_STRING = "string"
    DATATYPE_INTEGER = "integer"
    DATATYPE_DOUBLE = "double"
    DATATYPE_NUMBER = "number"
    DATATYPE_BOOLEAN = "boolean"
    DATATYPE_DATETIME = "datetime"
    DATATYPE_CURRENCY = "currency"
    DATATYPE_DECIMAL = "decimal"
    DATATYPE_DATE = "date"
    DATATYPE_TIME = "time"

    SUPPORTED_DATATYPES = [
        DATATYPE_STRING,
        DATATYPE_INTEGER,
        DATATYPE_NUMBER,
        DATATYPE_DOUBLE,
        DATATYPE_BOOLEAN,
        DATATYPE_DATETIME,
        DATATYPE_CURRENCY,
        DATATYPE_DECIMAL,
        DATATYPE_DATE,
        DATATYPE_TIME,
    ]
    CDM_ERP_ALLOWED_DATA_TYPES = {
        DATATYPE_DATETIME: [DATATYPE_DATETIME, DATATYPE_DATE, DATATYPE_TIME],
        DATATYPE_DECIMAL: [
            DATATYPE_INTEGER,
            DATATYPE_NUMBER,
            DATATYPE_DOUBLE,
            DATATYPE_DECIMAL,
            DATATYPE_CURRENCY,
        ],
        DATATYPE_INTEGER: [DATATYPE_INTEGER, DATATYPE_NUMBER],
        DATATYPE_STRING: SUPPORTED_DATATYPES,
    }

    MMDDYYYY_FORMAT = "mm/dd/yyyy"
    MMDDYYYY_FORMAT_FNS = "MM/dd/yyyy"
    DDMMYYYY_FORMAT = "dd/mm/yyyy"
    DDMMYYYY_FORMAT_FNS = "dd/MM/yyyy"
    DDMMYY_FORMAT = "dd/mm/yy"
    DDMMYY_FORMAT_FNS = "dd/mm/yy"
    MMDDYY_FORMAT = "mm/dd/yy"
    MMDDYY_FORMAT_FNS = "MM/dd/yy"
    MMDDYY_FORMAT_1 = "mm-dd-yy"
    MMDDYY_FORMAT_1_FNS = "MM-dd-yy"
    DDMMYYYY_FORMAT_1 = "dd-mm-yyyy"
    DDMMYYYY_FORMAT_1_FNS = "dd-MM-yyyy"
    MMDDYYYY_FORMAT_1 = "mm-dd-yyyy"
    MMDDYYYY_FORMAT_1_FNS = "MM-dd-yyyy"
    DDMMYYYY_FORMAT_2 = "dd/mm/yyyy HH:MM:SS"
    DDMMYYYY_FORMAT_2_FNS = "dd/MM/yyyy HH:mm:SS"
    DDMMYYYY_FORMAT_3 = "dd-mm-yyyy HH:MM"
    DDMMYYYY_FORMAT_3_FNS = "dd-MM-yyyy HH:mm"
    DDMMYYYY_FORMAT_4 = "dd-mm-yyyy HH:MM:SS"
    DDMMYYYY_FORMAT_4_FNS = "dd-MM-yyyy HH:mm:SS"
    DDMMYYYY_FORMAT_5 = "dd-mm-yyyy HH.MM"
    DDMMYYYY_FORMAT_5_FNS = "dd-MM-yyyy HH.mm"
    DDMMYYYY_FORMAT_6 = "dd-mm-yyyy HH.MM.SS"
    DDMMYYYY_FORMAT_6_FNS = "dd-MM-yyyy HH.mm.SS"
    DDMONYY_FORMAT = "dd-mon-yy"
    DDMONYY_FORMAT_FNS = "dd-MMM-yy"
    YYYYMMDDHHMMSS_FORMAT = "yyyy-mm-dd HH:MM:SS"
    YYYYMMDDHHMMSS_FORMAT_FNS = "yyyy-MM-dd HH:mm:SS"
    DDMMYYYYHHMMSS_FORMAT = "dd/mm/yyyy HH:MM:SS A"
    DDMMYYYYHHMMSS_FORMAT_FNS = "dd/MM/yyyy HH:mm:SS A"
    YYYYMMDDHHMMSSMS_FORMAT = "yyyy-mm-dd HH:MM:SS.ms"
    YYYYMMDDHHMMSSMS_FORMAT_FNS = "yyyy-MM-dd HH:mm:SS.ms"
    YYYYMMDDHHMM_FORMAT = "yyyy-mm-dd HH:MM"
    YYYYMMDDHHMM_FORMAT_FNS = "yyyy-MM-dd HH:mm"
    MMDDYYYYHHMMSS_FORMAT = "mm/dd/yyyy HH:MM:SS A"
    MMDDYYYYHHMMSS_FORMAT_FNS = "MM/dd/yyyy HH:mm:SS A"
    YYYY_FORMAT = "yyyy"
    YYYYMMDD_FORMAT = "yyyymmdd"
    YYYYMMDD_FORMAT_FNS = "yyyyMMdd"
    MM_FORMAT = "mm"
    MM_FORMAT = "MM"
    HHMMSS_FORMAT = "HH:MM:SS"
    HHMMSS_FORMAT = "HH:MM:SS"
    YYYYMMDD_FORMAT_1 = "yyyy-mm-dd"
    YYYYMMDD_FORMAT_1_FNS = "yyyy-MM-dd"
    YYYYMMDDTHHMMSSMS_FORMAT = "yyyy-mm-ddTHH:MM:SS.ms"
    YYYYMMDDTHHMMSSMS_FORMAT_FNS = "yyyy-MM-ddTHH:mm:SS.ms"
    DDMONYY = "dd-mon-yy"
    YYYYMMDD = "yyyy/mm/dd"
    YYYYMMDD_FNS = "yyyy/MM/dd"
    DDMMYY = "dd-mm-yy"
    DDMMYY_FNS = "dd-MM-yy"
    DDMONYYYY = "dd mon yyyy"
    DDMONYYYY_1 = "dd-mon-yyyy"
    YYYYMMDDTHHMMSSMS_FORMAT_TZ = "yyyy-mm-ddTHH:MM:SS.msTZ"
    DDMMYYYY_FORMAT_DOT = "dd.mm.yyyy"
    MMDDYYYY_FORMAT_DOT = "mm.dd.yyyy"
    YYYYMMDD_FORMAT_DOT = "yyyy.mm.dd"
    YYYYMMDDTHHMMSS_FORMAT_TZ = "yyyy-mm-ddTHH:MM:SSTZ"
    DDMMYYYY_FORMAT_DOT = "dd.mm.yyyy"
    MMDDYYYY_FORMAT_DOT = "mm.dd.yyyy"
    YYYYMMDD_FORMAT_DOT = "yyyy.mm.dd"

    DATETIME_FORMATS = {
        MMDDYYYY_FORMAT: "%m/%d/%Y",
        DDMMYYYY_FORMAT: "%d/%m/%Y",
        DDMMYY_FORMAT: "%d/%m/%y",
        MMDDYY_FORMAT: "%m/%d/%y",
        DDMMYYYY_FORMAT_1: "%d-%m-%Y",
        DDMMYYYY_FORMAT_2: "%d/%m/%Y %H:%M:%S",
        DDMMYYYY_FORMAT_3: "%d-%m-%Y %H:%M",
        DDMMYYYY_FORMAT_4: "%d-%m-%Y %H:%M:%S",
        DDMMYYYY_FORMAT_5: "%d-%m-%Y %H.%M",
        DDMMYYYY_FORMAT_6: "%d-%m-%Y %H.%M.%S",
        DDMONYY_FORMAT: "%d-%b-%y",
        YYYY_FORMAT: "%Y",
        MM_FORMAT: "%m",
        YYYYMMDDHHMMSS_FORMAT: "%Y-%m-%d %H:%M:%S",
        DDMMYYYYHHMMSS_FORMAT: "%d/%m/%Y %I:%M:%S %p",
        YYYYMMDDHHMMSSMS_FORMAT: "%Y-%m-%d %H:%M:%S.%f",
        MMDDYYYYHHMMSS_FORMAT: "%m/%d/%Y %I:%M:%S %p",
        HHMMSS_FORMAT: "%H:%M:%S",
        YYYYMMDD_FORMAT: "%Y%m%d",
        YYYYMMDD_FORMAT_1: "%Y-%m-%d",
        MMDDYYYY_FORMAT_1: "%m-%d-%Y",
        MMDDYY_FORMAT_1: "%m-%d-%y",
        YYYYMMDDTHHMMSSMS_FORMAT: "%Y-%m-%dT%H:%M:%S.%f",
        YYYYMMDDHHMM_FORMAT: "%Y-%m-%d %H:%M",
        DDMONYY: "%d-%b-%y",
        YYYYMMDD: "%Y/%m/%d",
        DDMMYY: "%d-%m-%y",
        DDMONYYYY_1: "%d-%b-%Y",
        DDMONYYYY: "%d %b %Y",
        MMDDYYYY_FORMAT_FNS: "%m/%d/%Y",
        DDMMYYYY_FORMAT_FNS: "%d/%m/%Y",
        DDMMYY_FORMAT_FNS: "%d/%m/%y",
        MMDDYY_FORMAT_FNS: "%m/%d/%y",
        DDMMYYYY_FORMAT_1_FNS: "%d-%m-%Y",
        DDMMYYYY_FORMAT_2_FNS: "%d/%m/%Y %H:%M:%S",
        DDMMYYYY_FORMAT_3_FNS: "%d-%m-%Y %H:%M",
        DDMMYYYY_FORMAT_4_FNS: "%d-%m-%Y %H:%M:%S",
        DDMMYYYY_FORMAT_5_FNS: "%d-%m-%Y %H.%M",
        DDMMYYYY_FORMAT_6_FNS: "%d-%m-%Y %H.%M.%S",
        DDMONYY_FORMAT_FNS: "%d-%b-%y",
        YYYYMMDDHHMMSS_FORMAT_FNS: "%Y-%m-%d %H:%M:%S",
        DDMMYYYYHHMMSS_FORMAT_FNS: "%d/%m/%Y %I:%M:%S %p",
        YYYYMMDDHHMMSSMS_FORMAT_FNS: "%Y-%m-%d %H:%M:%S.%f",
        MMDDYYYYHHMMSS_FORMAT_FNS: "%m/%d/%Y %I:%M:%S %p",
        YYYYMMDD_FORMAT_FNS: "%Y%m%d",
        YYYYMMDD_FORMAT_1_FNS: "%Y-%m-%d",
        MMDDYYYY_FORMAT_1_FNS: "%m-%d-%Y",
        MMDDYY_FORMAT_1_FNS: "%m-%d-%y",
        YYYYMMDDTHHMMSSMS_FORMAT_FNS: "%Y-%m-%dT%H:%M:%S.%f",
        YYYYMMDDHHMM_FORMAT_FNS: "%Y-%m-%d %H:%M",
        YYYYMMDD_FNS: "%Y/%m/%d",
        DDMMYY_FNS: "%d-%m-%y",
        YYYYMMDDTHHMMSSMS_FORMAT_TZ: "%Y-%m-%dT%H:%M:%S.%f%z",
        DDMMYYYY_FORMAT_DOT: "%d.%m.%Y",
        MMDDYYYY_FORMAT_DOT: "%m.%d.%Y",
        YYYYMMDD_FORMAT_DOT: "%Y.%m.%d",
        YYYYMMDDTHHMMSS_FORMAT_TZ: "%Y-%m-%dT%H:%M:%S%z"
    }

    DQS_FOLDER_NAME = "dqs"
    DQS_ZIP_FILE_NAME = "scorecard.zip"

    ASSETS_PATH = "assets"

    # Extras from CDM
    CDM_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
    START_CH = "<"
    END_CH = ">"
    SEPERATOR = "_._"
