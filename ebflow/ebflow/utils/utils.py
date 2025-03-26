import json
import re
import math

from ebflow.utils.cdm_conversion_exception import CDMConversionException


def get_index_by_occurrence(account_string, char, occurrence):
    count = 1
    idx = -1
    while True:
        idx = account_string.find(char, idx + 1)
        if idx == -1:
            return None
        if count == occurrence:
            return idx
        count += 1


def check_overlap(range1, range2):
    return range1["start"] <= range2["end"] and range2["start"] <= range1["end"]


def check_ranges_overlapping(range_details):
    for i in range(len(range_details)):
        for j in range(i + 1, len(range_details)):
            if check_overlap(range_details[i], range_details[j]):
                return True
    return False


def evaluate_condition_with_different_operators(record, expression, operand):
    symbol = operand.get("symbol", None)
    operand_result = check_for_condition(record, operand)

    expression = expression or ""
    expression = expression + symbol + str(operand_result)

    return eval(expression)


def standardize_field_name(field_name):
    standard_name = str(field_name).strip().replace(" ", "_")
    standard_name = standard_name.replace("/", "_")
    standard_name = standard_name.replace(".", "_")
    return standard_name


def adjust_filepath(filepath, is_local=True):
    try:
        if str(filepath).startswith("/dbfs"):
            return filepath
        return filepath if is_local else f"/dbfs{str(filepath)}"
    except Exception as e:
        error_data = {
            "failed_function": "send_job_status",
            "called_method": None,
            "file_path": filepath,
            "message": str(e),
        }
        error_info = json.dumps(error_data)
        raise CDMConversionException(error_info)


def numeric_validate(input_val):
    # Use regular expression to find all continuous sequences of numbers in the input string
    # matches = re.findall(r"[\d\.\d]+", input_string)  # to get only positive numbers
    input_string = str(input_val).replace(",", "")

    negative = 1
    if input_string[0] == "-":
        negative = -1

    matches = re.findall(
        r"[-+]?\d*\.?\d+|[-+]?\d+", input_string
    )  # to get negative ones as well

    # If there is exactly one match, return that number; otherwise, raise Exception
    if len(matches) == 1:
        return float(matches[0]) * negative
    raise TypeError


def get_node_label(num: int):
    alphabet: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    sequence: str = ""
    while num >= 0:
        sequence = alphabet[num % 26] + sequence
        num = math.floor(num / 26) - 1

    return sequence
