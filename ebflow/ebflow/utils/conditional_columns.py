import copy
from datetime import datetime, date, time
from dateutil.relativedelta import relativedelta

from ebflow.utils.constants import Constants
from ebflow.utils.utils import numeric_validate


def evaluate_condition_with_different_operators(record, operands):
    expression = ""
    for operand in operands:
        symbol = operand.get("symbol", "")
        operand_result = check_for_condition(record, operand)

        expression = expression + symbol + str(operand_result)

    return eval(expression)


def extract_date_info_by_format(value, date_info):
    type_format = date_info.get("type", "date")
    input_date_format = date_info.get("date_format", None)
    date_format = Constants.DATETIME_FORMATS.get(input_date_format)
    date_sub_info = date_info.get("date_sub_info", None)
    delta_args = date_info.get("delta_args", None)

    if date_sub_info == "relativedelta":
        return relativedelta(**delta_args)

    if isinstance(value, (datetime, date, time)):
        date_value = value
    else:
        date_value = datetime.strptime(value, date_format)
        if type_format == "date":
            date_value = date_value.date()
        elif type_format == "time":
            date_value = date_value.time()

    if date_sub_info == "day":
        return date_value.day
    elif date_sub_info == "month":
        return date_value.month
    elif date_sub_info == "year":
        return date_value.year
    else:
        return date_value


def check_for_condition(record, condition):
    try:
        if not isinstance(condition, dict):
            return None  # Check if condition is a dictionary
        condition_type = condition.get("type", None)
        operands = condition.get("operands", [])
        operator = condition.get("operator", None)
        variable = condition.get("variable", None)

        result = True
        if len(operands) > 0:
            if operator is not None:
                for index, operand in enumerate(operands):
                    if index == 0:
                        result = check_for_condition(record, operand)
                    else:
                        operand_result = check_for_condition(record, operand)
                        if operator == "and":
                            if isinstance(result, str) and isinstance(
                                operand_result, str
                            ):
                                result = result == operand_result
                            else:
                                result = eval(
                                    str(result)
                                    + " "
                                    + str(operator)
                                    + " "
                                    + str(operand_result)
                                )
                        elif operator == "in":
                            result = result in operand_result
                        else:
                            if condition_type in [
                                Constants.DATATYPE_DATETIME,
                                Constants.DATATYPE_DATE,
                                Constants.DATATYPE_TIME,
                            ]:
                                if operator == "+":
                                    result = result + operand_result
                                elif operator == "-":
                                    result = result - operand_result
                                elif operator == ">":
                                    result = result > operand_result
                                elif operator == "<":
                                    result = result < operand_result
                                elif operator == "==":
                                    result = result == operand_result
                            else:
                                result = eval(
                                    str(result)
                                    + " "
                                    + str(operator)
                                    + " "
                                    + str(operand_result)
                                )

            else:
                result = evaluate_condition_with_different_operators(record, operands)
            return result

        elif variable:
            result = record[variable]
        elif "value" in condition:
            result = copy.deepcopy(condition["value"])
        else:
            result = "result" in condition

        if condition_type in [
            Constants.DATATYPE_DATETIME,
            Constants.DATATYPE_DATE,
            Constants.DATATYPE_TIME,
        ]:
            result = extract_date_info_by_format(result, condition)
        elif condition_type in [
            Constants.DATATYPE_CURRENCY,
            Constants.DATATYPE_INTEGER,
            Constants.DATATYPE_NUMBER,
            Constants.DATATYPE_DOUBLE,
            Constants.DATATYPE_DECIMAL,
        ]:
            result = numeric_validate(result)

        return result  # Return result as a list

    except Exception as e:
        detail = {
            "error": str(e),
            "id": getattr(condition, "id", None),  # Access "id" attribute if it exists
        }
        raise Exception(str(detail))
