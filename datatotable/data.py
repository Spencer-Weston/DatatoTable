"""
Data holds the DataOperator class which coerces raw_data into SQLalchemy compatible formats.
"""
from sqlalchemy import Integer, Float, String, DateTime, Boolean
from datatotable import typecheck
from datetime import datetime


class DataOperator:
    """DataOperator takes scraped data in init, and uses its member functions to return manipulations of that data"""

    def __init__(self, data):
        """Stores the data dictionary passed to it

        Args:
            data: A dictionary of data which will, usually, reflect data scraped from a website. Two dictionary
            formats are accepted. First, data may hold column names with data values formatted as:
            data[col1] = [val1, val2, ...]
            data[col2] = [val1, val2, ...]
            Second, data may be a list of rows formatted as:
            data[0] = {col1: val0, col2: val0, colx: val0}
            data[x] = {col1: valx, col2: valx, colx: valx}
        """
        self.data = data

    @property
    def columns(self):
        """Return a dictionary formatted as {key: SQLtype}.

        Returns:
            A dictionary with the same keys as self.data. The dictionary's values are the sql_types of each key:value
            pair in tbl_dict. The columns function with SQLalchemy as column definitions.
        """
        py_types = self._get_py_type()  # py_types is a dict
        sql_types = self._py_type_to_sql_type(py_types)
        return sql_types

    def _get_py_type(self):
        """Take the classes data values and return a dictionary that holds the python type for the values.

        Returns:
            A dictionary formatted as key:py_type where the type can be integer, float, string, datetime, or none
        """
        py_types_dict = {}
        if isinstance(self.data, dict):
            tbl_keys = list(self.data.keys())
            py_types = [typecheck.get_type(self.data[key]) for key in tbl_keys]
            py_types_dict = dict(zip(tbl_keys, py_types))
        elif isinstance(self.data, list):
            if isinstance(self.data[0], dict):
                data = self.data[0]
                tbl_keys = list(data.keys())
                py_types = [typecheck.get_type(data[key]) for key in tbl_keys]
                py_types_dict = dict(zip(tbl_keys, py_types))
            else:
                raise Exception("The data structure ({}) is not handled by _get_py_type".format(type(self.data)))
        return py_types_dict

    @staticmethod
    def _py_type_to_sql_type(py_types):
        """Convert and return a dictionary of python types to a dictionary of sql types.

        Raises:
            An exception if a py_type is not an integer, float, string, datetime, bool, or none
        """

        sql_types = dict()
        for key in py_types:
            py_type = py_types[key]
            if py_type == "integer" or py_type is int:
                sql_types[key] = Integer
            elif py_type == "float" or py_type is float:
                sql_types[key] = Float
            elif py_type == "string" or py_type is str:
                sql_types[key] = String
            elif py_type == "datetime" or py_type is datetime:
                sql_types[key] = DateTime
            elif py_type == "bool" or py_type is bool:
                sql_types[key] = Boolean
            elif py_type is None:
                continue  # We continue here so as to not create a column for null values
                # ToDo: evaluate if this clause should exist. Why was it here in the first place?
            else:
                raise Exception("Error: py_type {} is not an integer, float, datetime,"
                                " none, or string".format(py_types[key]))
        return sql_types

    @property
    def rows(self):
        """Convert and return class data into rows compatible with sqlalchemy's insert function

        Currently presumes each dictionary object is a list of equivalent length. Calls _dict_to_rows() to do primary
        processing. Does not yet function with lists.

        Returns:
            a list of rows compatible with SQLalchemy's

        Raise:
            Exception: If the input is neither a list nor dictionary, an exception is raised
        """
        if isinstance(self.data, dict):
            return self._dict_to_rows()
        elif isinstance(self.data, list):
            if self._list_to_rows():
                return self.data
            else:
                raise ValueError("self.data is not a properly formatted list or dictionary, and cannot be handled")
        else:
            raise ValueError("self.data is not a properly formatted list or dictionary, and cannot be handled")

    def _dict_to_rows(self):
        """Convert and return an input dictionary into rows compatible with SQLalchemy"""

        rows = []
        keys = list(self.data.keys())
        # The length of the data should be checked outside the function to ensure each value is an equal length object
        length = len(self.data[keys[0]])
        for i in range(length):
            row_dict = dict()
            for key in keys:
                row_dict[key] = self.data[key][i]
            rows.append(row_dict)
        return rows

    def _list_to_rows(self):
        """Checks if self.data is in row format and returns the result as a bool"""
        keys = self.data[0].keys()
        for i in self.data:
            if i.keys() == keys:
                continue
            else:
                return False
        return True

    def validate_data_length(self):
        """Given a dictionary where keys references lists, check that all lists are the same length, and return T or F

        Returns:
             True: if all the lists in the dictionary have the same length
             False: if the dictionary's lists are of different lengths
        """
        keys = self.data.keys()
        lengths = []
        for key in keys:
            lengths.append(len(self.data[key]))
        length_set = set(lengths)
        if len(length_set) == 1:
            return True
        else:
            return False


if __name__ == "__main__":
    from datetime import datetime, timedelta
    test_data1 = {"strings": ["hi", "world", "bye", "school"], "ints": [1, 2, 3, 4],
                  "floats": [1.1, 2.2, 3.3, 4.4444], "dates": [datetime(2019, 1, 1)+timedelta(i) for i in range(4)]}

    test_data2 = {"int_float": [1, 2, 1.1, 2.2], "int_float_none": [None, 1, 2, 3.3],
                  "datetime_none": [datetime(2019, 1, 1), None, datetime(2018, 1, 1), None],
                  "datetime_string": [datetime(2019, 1, 1), 'here', 'we', 'go'],
                  "int_float_string": [1, 1.1, "1.1", "2.2cm"],
                  "int_float_string_datetime": [1, 1.1, "1.1cm", datetime(2019, 1, 1)]}

    test_list_data = [{"strings": 'hi', 'ints': 1, 'floats': 1.1, 'dates': datetime(2019, 1, 1)},
                      {"strings": 'world', 'ints': 2, 'floats': 2.2, 'dates': datetime(2019, 1, 2)},
                      {"strings": 'bye', 'ints': 3, 'floats': 3.3, 'dates': datetime(2019, 1, 3)},
                      {"strings": 'school', 'ints': 4, 'floats': 4.4444, 'dates': datetime(2019, 1, 4)}]

    test1 = DataOperator(test_data1)
    columns1 = test1.columns
    test2 = DataOperator(test_data2)
    columns2 = test2.columns
    test_list = DataOperator(test_list_data)
    columns_list = test_list.columns
    rows_list = test_list.rows
    t=2
