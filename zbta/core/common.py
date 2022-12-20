from typing import Dict, Tuple, Any
from collections import deque
import re
from jsonschema import ValidationError, validate, SchemaError


class APIError(Exception):
    """The error of the API.

    Parameters
    ----------
    Exception : general exception error
        the general exception.
    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class NoValidAccountError(Exception):
    """No Valid account found.

    Parameters
    ----------
    Exception : general exception error
        the general exception.
    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class NoTransactionError(Exception):
    """No transaction error.

    Parameters
    ----------
    Exception : general exception error
        the general exception.
    """
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def _get_validation_bad_key(path: deque, message: str) -> str:
    if path:
        return path[-1]
    # if not possible to retrieve key from path, try to take it from message
    try:
        key = re.search(r"'(.+)' is a required", message).group(1)
    except Exception:
        try:
            key = re.search(r"'(.+)' was unexpected", message).group(1)
        except Exception:
            key = "."
    return key

def validate_schema(obj: Dict, schema: Dict, schema_name: str) -> Tuple[bool, str]:
    """
    Validates `obj` against `schema`. Does not throw any exceptions.
    """
    try:
        validate(obj, schema)
    except ValidationError as err:
        is_valid = False
        message = err.message
        bad_key = _get_validation_bad_key(err.path, message)
        validator = err.validator
        final_error_message = (
            f"There was an error while validating {schema_name} schema. "
            f"Conflicting key: `{bad_key}`. "
            f"Conflicting validator: `{validator}`. "
            f"Error message: {message}"
        )
    except SchemaError as err:
        is_valid = False
        message = err.message
        final_error_message = f"SchemaError in the payload: `{message}`"
    else:  # if valid schema
        is_valid = True
        final_error_message = None
    return is_valid, final_error_message