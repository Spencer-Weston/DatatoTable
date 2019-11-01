"""
Contains type checks and type conversion functions
"""

from datetime import datetime, date
from enum import Enum


def set_type(values, new_type):
    """Convert string values to integers or floats if applicable. Otherwise, return strings.

    If the string value has zero length, none is returned

    Args:
        values: A list of values
        new_type: The type to coerce values to

    Returns:
        The input list of values modified to match their type. String is the default return value. If the values are
        ints or floats, returns the list formatted as a list of ints or floats. Empty values will be replaced with none.
    """
    if new_type == str:
        coerced_values = [str(x) for x in values]
    elif new_type == int or new_type == float:
        float_values = [float(x) for x in values]
        if new_type == int:
            coerced_values = [int(round(x)) for x in float_values]
        else:
            coerced_values = float_values
    else:
        raise ValueError("{} not supported for coercing types".format(new_type.__name__))
    return coerced_values


def _set_type(values, new_type):
    """Transforms a list of values into the specified new type. If the value has zero length, returns none

    Args:
        values: A list of values
        new_type: A type class to modify the list to

    Returns:
        The values list modified to the new_type. If an element is empty, the element is set to None.
    """

    new_vals = []
    for i in values:
        if len(i) > 0:  # Some values may have len(0); we convert them to None to put into sql db
            new_vals.append(new_type(i))
        else:
            new_vals.append(None)
    return new_vals


def get_type(values):
    """Return the type of the values where type is defined as the modal type in the list.

    Args:
        values: A list or value to get the type for.

    Returns:
        The modal type of a list or the type of the element. Can be integer, float, string, datetime, or none
    """
    if hasattr(values, "__len__") and (type(values) != type):  # Checks if the object is iterable
        val_types = []
        for i in values:
            val_types.append(_get_type(i))
        type_set = set(val_types)
        if len(type_set) == 1:
            return type_set.pop()
        elif len(type_set) == 2 and {None}.issubset(type_set):  # None value allowance
            return type_set.difference({None}).pop()
        elif len(type_set) <= 3 and {int, float}.issubset(type_set):
            diff = type_set.difference({int, float})
            if not bool(diff) or diff == {None}:
                return float
            else:
                return str
        else:  # All other possible combinations of value must default to string
            return str

    elif isinstance(values, Enum):  # For enum objects, pass the value to the get_type function (right choice? IDK)
        return _get_type(values.value)
    else:
        return _get_type(values)


def _get_type(val):
    """Return the type of the value if it is a int, float, or datetime. Otherwise, return a string.

    Args:
        val: A value to get the type of
    Returns:
        The type of the value passed into the function if it is an int, float, datetime, or string
    Raise:
        Exception: An exception raised if the val is not int, float, datetime, None, or string.
    """
    if isinstance(val, int):
        return int
    elif isinstance(val, float):
        return float
    elif isinstance(val, datetime):
        return datetime
    elif isinstance(val, date):
        return date
    elif isinstance(val, str):
        return str
    elif isinstance(val, bool):
        return bool
    elif val is None:
        return None
    elif is_python_type(val):  # Handles types that are passed explicitly
        return val
    else:
        raise Exception("Val is not an int, float, datetime, string, Bool, or None")


def is_int(x):
    """Return true if X can be coerced to a integer. Otherwise, return false."""
    try:
        int(x)  # Will raise ValueError if '.2'; will not raise error if .2
        return True
    except ValueError:
        return False


def is_float(x):
    """Return true if X can be coerced to a float. Otherwise, return false."""
    try:
        float(x)
        return True
    except ValueError:
        return False


def is_python_type(x):
    if x in [int, float, datetime, str, bool, None]:
        return True
    else:
        return False
