import copy
import json
from datetime import datetime

from frictionless import steps, Pipeline

from ebflow.utils.conditional_columns import check_for_condition
from ebflow.utils.constants import Constants
from ebflow.utils.cdm_conversion_exception import CDMConversionException
from ebflow.utils.custom_steps import fill_down, update_jid
from ebflow.utils.utils import (
    standardize_field_name,
    get_index_by_occurrence,
    check_ranges_overlapping,
)


def add_data_type_details(data_type, date_format=None):
    field_details = dict()

    # for Field type in [string, boolean, number] the frictionless data_type can be the same. format is not required
    if data_type in [
        Constants.DATATYPE_STRING,
        Constants.DATATYPE_BOOLEAN,
        Constants.DATATYPE_NUMBER,
    ]:
        field_details["type"] = data_type

    # for Field type decimal, frictionless data_type should be number
    elif data_type == Constants.DATATYPE_DECIMAL:
        field_details["type"] = "number"

    # for Field type of datetime, frictionless has same data_type for date, time and datetime
    # the format of the datetime can be 'any' if not specified where frictionless will try to parse the dates
    # if date_format is present, corresponding python datetime format (from constants) is used
    elif data_type in [
        Constants.DATATYPE_DATETIME,
        Constants.DATATYPE_DATE,
        Constants.DATATYPE_TIME,
    ]:
        field_details["type"] = data_type
        field_details["format"] = "any"
        if date_format and Constants.DATETIME_FORMATS.get(date_format):
            field_details["format"] = Constants.DATETIME_FORMATS[date_format]

    # for currency, type should be 'number' but bareNumber to be set False to ignore text before/after the number
    elif data_type == Constants.DATATYPE_CURRENCY:
        field_details["type"] = "number"
        field_details["bareNumber"] = False

    return field_details


def change_erp_field_names(erp_fields):
    for erp_field in erp_fields:
        erp_field["new_field_name"] = standardize_field_name(erp_field["field_name"])


def change_resource_field_names(resource):
    for field in resource.schema.fields:
        field.name = standardize_field_name(field.name)


def get_cdm_map_operation(cdm_map: dict, errors: [dict]):
    original_file_name = cdm_map.get("original_filename", None)
    cdm_file_name = cdm_map.get("file_name", None)
    cdm_field_name = cdm_map.get("cdm_field")
    erp_fields = sorted(cdm_map.get("erp_fields", []), key=lambda i: i.get("order", 0))
    change_erp_field_names(erp_fields)

    def custom_none(row):
        return None

    for field in erp_fields:
        file_name = field.get("file_name", None)
        if file_name != cdm_file_name:
            errors.append(
                {
                    "original_filename": original_file_name,
                    "cdm_field_name": cdm_field_name,
                    "erp_field_name": field.get("field_name"),
                    "data_row": None,
                    "error": "Using fields of different input files",
                }
            )

            return custom_none, None, None

    if cdm_map["map_type"] == "dummy":

        def dummy(row):
            return None

        return dummy, None, None

    if cdm_map["map_type"] == "direct":
        if not len(erp_fields):

            def direct_none(row):
                return None

            return direct_none, None, None

        erp_field_name = erp_fields[0]["new_field_name"]
        cdm_data_type = cdm_map.get("data_type", "string")
        cdm_date_format = cdm_map.get("date_format", None)

        def direct(row, efn=erp_field_name):
            return row[efn]

        return direct, add_data_type_details(cdm_data_type, cdm_date_format), None

    if cdm_map["map_type"] == "date-mask":
        erp_field_name = erp_fields[0]["new_field_name"]
        cdm_data_type = cdm_map.get("data_type", "datetime")

        def date_mask(row, efn=erp_field_name):
            return row[efn]

        return (
            date_mask,
            add_data_type_details(cdm_data_type, Constants.CDM_DATE_FORMAT),
            None,
        )

    if cdm_map["map_type"] == "calculated":
        if cdm_map["operation"] == "DCONCAT":
            date_field = erp_fields[0]["new_field_name"]
            time_field = erp_fields[1]["new_field_name"]

            cdm_data_type = "datetime"

            def d_concat(row, df=date_field, tf=time_field):
                if not row[df] or not row[tf]:
                    errors.append(
                        errors.append(
                            {
                                "original_filename": original_file_name,
                                "cdm_field_name": cdm_field_name,
                                "erp_field_name": f"{df}, {tf}",
                                "data_row": row["serial_number"],
                                "error": "Either date field or time field is not present",
                            }
                        )
                    )
                    return None
                return datetime.combine(row[df], row[tf])

            return (
                d_concat,
                add_data_type_details(cdm_data_type, Constants.CDM_DATE_FORMAT),
                None,
            )

        if cdm_map["operation"] == "FT":
            cdm_data_type = cdm_map.get("data_type", "string")
            value_to_map = cdm_map.get("free_text", None)

            def free_text(row, v=value_to_map):
                return v

            return free_text, add_data_type_details(cdm_data_type), None

        if cdm_map["operation"] == "MC":
            cdm_data_type = cdm_map.get("data_type", "number")
            expression = cdm_map["expression"]

            def manual_calculation(row, ex=expression, ef=copy.deepcopy(erp_fields)):
                ex = ex.replace(" ", "")
                ex = ex.replace("^", "**")
                for erp_field in ef:
                    ex = ex.replace(
                        f'<{erp_field["order"]}>', f'{row[erp_field["new_field_name"]]}'
                    )
                try:
                    return eval(ex)
                except:
                    errors.append(
                        errors.append(
                            {
                                "original_filename": original_file_name,
                                "cdm_field_name": cdm_field_name,
                                "erp_field_name": str([f["field_name"] for f in ef]),
                                "data_row": row["serial_number"],
                                "error": f"Could not evaluate expression: {ex}",
                            }
                        )
                    )
                    return None

            return manual_calculation, add_data_type_details(cdm_data_type), None

        if cdm_map["operation"] == "FILLDOWN":
            cdm_data_type = cdm_map.get("data_type", "string")
            erp_field_name = erp_fields[0]["new_field_name"]

            def fill_down(row, efn=erp_field_name):
                return row[efn]

            return fill_down, add_data_type_details(cdm_data_type), None

        if cdm_map["operation"] == "Journal Line Number":
            cdm_data_type = cdm_map.get("data_type", "number")

            def journal_line_number(row):
                return None

            return journal_line_number, add_data_type_details(cdm_data_type), None

        if cdm_map["operation"] == "SGLAN":
            cdm_data_type = cdm_map.get("data_type", "string")
            erp_field_name = erp_fields[0]["new_field_name"]
            extract_split_fields = cdm_map.get("extract_split_fields")
            if not extract_split_fields:
                return custom_none, None, None

            opening_char = extract_split_fields.get("opening_char")
            opening_char_count = extract_split_fields.get("opening_char_number", 1)

            closing_char = extract_split_fields.get("closing_char")
            closing_char_count = None
            if closing_char:
                closing_char_count = extract_split_fields.get("closing_char_number")
                if not closing_char_count:
                    closing_char_count = 2 if opening_char == closing_char else 1

            inside_outside = extract_split_fields.get("inside_outside", "inside")

            def sglan(
                row,
                efn=erp_field_name,
                oc=opening_char,
                cc=closing_char,
                occ=opening_char_count,
                ccc=closing_char_count,
                io=inside_outside,
            ):

                account = str(row[efn])
                start = get_index_by_occurrence(account, oc, occ)
                end = get_index_by_occurrence(account, cc, ccc)

                if io == "outside":
                    start = 0 if start is None else start
                    end = len(account) if end is None else end + len(cc)
                    if start > end:
                        return None
                    return account[:start] + account[end:]
                else:
                    start = 0 if start is None else start + len(oc)
                    end = len(account) if end is None else end
                    if start > end:
                        return None
                    return account[start:end]

            return sglan, add_data_type_details(cdm_data_type), None

        if cdm_map["operation"] in ["RBA", "ABS", "SPLIT", "DCI1"]:
            if cdm_map["operation"] == "RBA":
                erp_field_name = erp_fields[1]["new_field_name"]

            else:
                erp_field_name = erp_fields[0]["new_field_name"]

            if cdm_map["operation"] in ["ABS", "RBA"]:
                cdm_data_type = cdm_map.get("data_type", "number")

                def custom_abs(row, efn=erp_field_name):
                    return abs(row[efn])

                return custom_abs, add_data_type_details(cdm_data_type), None

            elif cdm_map["operation"] == "DCI1":
                cdm_data_type = cdm_map.get("data_type", "string")

                def debit_credit_indicator1(row, efn=erp_field_name):
                    return "D" if row[efn] > 0 else "C"

                return (
                    debit_credit_indicator1,
                    add_data_type_details(cdm_data_type),
                    None,
                )

            else:
                cdm_data_type = cdm_map.get("data_type", "string")
                extra_required = cdm_map.get("extra", "-1")
                extra_required = extra_required != "-1"
                char_length = int(cdm_map["characters"])

                def custom_split(row, efn=erp_field_name, cl=char_length):
                    return row[efn][:cl]

                def custom_split_extra(row, efn=erp_field_name, cl=char_length):
                    return row[efn][cl:]

                if extra_required:
                    return (
                        custom_split,
                        add_data_type_details(cdm_data_type),
                        custom_split_extra,
                    )
                return custom_split, add_data_type_details(cdm_data_type), None

        if cdm_map["operation"] in ["DIV", "DC", "DCI3", "CS"]:
            first_field = erp_fields[0]["new_field_name"]
            second_field = erp_fields[1]["new_field_name"]

            if cdm_map["operation"] in ["DCI3"]:
                cdm_data_type = cdm_map.get("data_type", "string")
                range_details = cdm_map["rangeDetails"]
                if check_ranges_overlapping(range_details):
                    errors.append(
                        {
                            "original_filename": original_file_name,
                            "cdm_field_name": cdm_field_name,
                            "erp_field_name": None,
                            "data_row": None,
                            "error": f"Ranges are overlapping: {range_details}",
                        }
                    )
                    return custom_none, None, None

                def range_based_debit_credit(
                    row, f=first_field, s=second_field, r=range_details
                ):
                    for range_detail in r:
                        if row[f] is None:
                            return None
                        if row[s] is None:
                            return None
                        if range_detail["start"] < row[f] < range_detail["end"]:
                            if range_detail["negatives"] == "Debit":
                                return "D" if row[s] < 0 else "C"
                            if range_detail["negatives"] == "Credit":
                                return "C" if row[s] < 0 else "D"
                    return None

                return (
                    range_based_debit_credit,
                    add_data_type_details(cdm_data_type),
                    None,
                )

            elif cdm_map["operation"] == "DIV":
                cdm_data_type = cdm_map.get("data_type", "number")

                def custom_division(row, f=first_field, s=second_field):
                    try:
                        return row[f] / row[s]
                    except ZeroDivisionError:
                        errors.append(
                            {
                                "original_filename": original_file_name,
                                "cdm_field_name": cdm_field_name,
                                "erp_field_name": s,
                                "data_row": row["serial_number"],
                                "error": f"Cannot divide value {row[f]} in field {f} by Zero in field {s}",
                            }
                        )
                        return None

                return custom_division, add_data_type_details(cdm_data_type), None

            elif cdm_map["operation"] == "CS":
                cdm_data_type = cdm_map.get("data_type", "string")

                def cs_function(row, f=first_field, s=second_field):
                    return eval(f"{row[f]} if {row[f]} else {row[s]}")

                return cs_function, add_data_type_details(cdm_data_type), None

            else:
                cdm_data_type = cdm_map.get("data_type", "string")

                def dc_function(row, f=first_field, s=second_field):
                    if row[f] and row[s]:
                        errors.append(
                            {
                                "original_filename": original_file_name,
                                "cdm_field_name": cdm_field_name,
                                "erp_field_name": f"{f}, {s}",
                                "data_row": row["serial_number"],
                                "error": f"Both field {f} and field {s} have values, only one should have a value.",
                            }
                        )
                        return None
                    else:
                        return row[s] if row[s] else row[f]

                return dc_function, add_data_type_details(cdm_data_type), None

        if cdm_map["operation"] in ["SUM", "DIFF", "PROD", "CONCAT", "DCI2"]:

            def custom_sum(row, ef=copy.deepcopy(erp_fields)):
                result = 0
                for field in ef:
                    result += (
                        row[field["new_field_name"]]
                        if row[field["new_field_name"]]
                        else 0
                    )
                return result

            def custom_diff(row, ef=copy.deepcopy(erp_fields)):
                result = row[ef[0]["new_field_name"]]
                result = result if result else 0
                for field in ef[1:]:
                    result -= (
                        row[field["new_field_name"]]
                        if row[field["new_field_name"]]
                        else 0
                    )
                return result

            def custom_prod(row, ef=copy.deepcopy(erp_fields)):
                result = 1
                for field in ef:
                    result *= (
                        row[field["new_field_name"]]
                        if row[field["new_field_name"]]
                        else 0
                    )
                return result

            separator = cdm_map.get("separator", "|")

            def custom_concat(row, ef=copy.deepcopy(erp_fields), s=separator):
                values_to_join = []
                for field in ef:
                    str_value = str(row[field["new_field_name"]]).strip()
                    if str_value:
                        values_to_join.append(str_value)
                return s.join(values_to_join)

            def identify_debit_credit(row, ef=copy.deepcopy(erp_fields)):
                debit = row[ef[0]["new_field_name"]]
                credit = row[ef[1]["new_field_name"]]
                if debit and credit:
                    return None
                else:
                    return "C" if credit else "D"

            function_map = {
                "SUM": (custom_sum, "number"),
                "DIFF": (custom_diff, "number"),
                "PROD": (custom_prod, "number"),
                "CONCAT": (custom_concat, "string"),
                "DCI2": (identify_debit_credit, "string"),
            }

            cdm_data_type = cdm_map.get(
                "data_type", function_map[cdm_map["operation"]][1]
            )
            return (
                function_map[cdm_map["operation"]][0],
                add_data_type_details(cdm_data_type),
                None,
            )

        if cdm_map["operation"] == "CC":
            conditions = cdm_map["conditions"]

            def condition_match_column(row, c=conditions, ef=copy.deepcopy(erp_fields)):
                try:
                    for condition in c:
                        if check_for_condition(row, condition):
                            result = condition["result"]
                            variable = result.get("variable", None)
                            value_to_update = (
                                row[variable] if variable else result["value"]
                            )
                            return value_to_update
                    errors.append(
                        {
                            "original_filename": original_file_name,
                            "cdm_field_name": cdm_field_name,
                            "erp_field_name": str([f["field_name"] for f in ef]),
                            "data_row": row["serial_number"],
                            "error": "At least one condition should match from given conditions",
                        }
                    )
                    return None
                except Exception as e:
                    detail = str(e)
                    errors.append(
                        {
                            "original_filename": original_file_name,
                            "cdm_field_name": cdm_field_name,
                            "erp_field_name": str([f["field_name"] for f in ef]),
                            "data_row": row["serial_number"],
                            "error": detail,
                        }
                    )
                    return None

            cdm_data_type = cdm_map.get("data_type", "string")
            return condition_match_column, add_data_type_details(cdm_data_type), None

    return None, None


def generate_cdm_fields(mappings):
    """
    This function creates the intermediate files for the report
    """
    try:

        intermediate_files = list()
        errors = list()
        for mapping in mappings:
            report_name = mapping["report_name"]
            input_files = mapping["file_info"]
            cdm_mapping = mapping["mapping"]

            resource = list(input_files.values())[0].get("resource")
            pipeline = list(input_files.values())[0].get("pipeline")
            if not resource:
                raise CDMConversionException()
            if not pipeline:
                pipeline = Pipeline(steps=[steps.table_normalize()])

            change_resource_field_names(resource)

            report_data = {
                "report_type": report_name,
            }
            cdm_steps = [steps.field_add(name="serial_number", incremental=True)]
            cdm_order_fields = []
            for cdm_map in cdm_mapping:
                cdm_field_name = cdm_map.get("cdm_field")
                cdm_order_fields.append(cdm_map.get("cdm_field"))

                cdm_function, descriptor, extra = get_cdm_map_operation(cdm_map, errors)

                cdm_steps.append(
                    steps.field_add(
                        name=cdm_field_name,
                        function=cdm_function,
                        descriptor=descriptor,
                    )
                )
                if extra:
                    cdm_order_fields.append("extra")
                    cdm_steps.append(
                        steps.field_add(
                            name="extra", function=extra, descriptor=descriptor
                        )
                    )

                # Check for FillDown and JournalLineNumber field
                if cdm_map.get("operation") == "FILLDOWN":
                    cdm_field_name = cdm_map.get("cdm_field")
                    cdm_steps.append(fill_down(field_names=[cdm_field_name]))

                if cdm_map.get("operation") == "Journal Line Number":
                    erp_field_name = standardize_field_name(
                        cdm_map["erp_fields"][0]["field_name"]
                    )
                    cdm_field_name = cdm_map.get("cdm_field")
                    cdm_steps.append(
                        update_jid(
                            field_name=erp_field_name, journal_id_field=cdm_field_name
                        )
                    )

            pipeline.steps.extend(cdm_steps)

            report_data["field_order"] = cdm_order_fields
            report_data["resource"] = resource
            report_data["pipeline"] = pipeline
            report_data["bad_file_data"] = [
                {"file_name": file_name, "file_checks": details["file_checks"]}
                for file_name, details in input_files.items()
            ]

            intermediate_files.append(report_data)

        return intermediate_files, errors

    except CDMConversionException as cdm_e:
        error_info = json.loads(str(cdm_e))
        error_data = {
            "failed_function": "create_report_content",
            "called_method": error_info["failed_function"],
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
    except Exception as e:
        error_data = {
            "failed_function": "create_report_content",
            "called_method": None,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)
