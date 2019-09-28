from datatotable.database import Database
from datatotable.data import DataOperator
from datatotable import typecheck
from datetime import datetime, timedelta
import os
import pandas
import pytest
from sqlalchemy import Integer, Float, String, DateTime, Boolean


@pytest.fixture()
def database():
    return Database("test")


@pytest.fixture
def sample_dict_data():
    test_data = {"strings": ["hi", "world", "bye", "school"], "ints": [1, 2, 3, 4],
                 "floats": [1.1, 2.2, 3.3, 4.4444], "dates": [datetime(2019, 1, 1)+timedelta(i) for i in range(4)]}
    return test_data


@pytest.fixture()
def sample_row_data():
    test_data = [{"strings": 'hi', 'ints': 1, 'floats': 1.1, 'dates': datetime(2019, 1, 1)},
                 {"strings": 'world', 'ints': 2, 'floats': 2.2, 'dates': datetime(2019, 1, 2)},
                 {"strings": 'bye', 'ints': 3, 'floats': 3.3, 'dates': datetime(2019, 1, 3)},
                 {"strings": 'school', 'ints': 4, 'floats': 4.4444, 'dates': datetime(2019, 1, 4)}]
    return test_data


class TestTypeCheck:
    """Tests functionality in the typecheck module"""

    def test_get_type(self, sample_dict_data):
        assert str == typecheck.get_type(sample_dict_data['strings'])
        assert int == typecheck.get_type(sample_dict_data['ints'])
        assert float == typecheck.get_type(sample_dict_data['floats'])
        assert datetime == typecheck.get_type(sample_dict_data['dates'])

    def test_set_type(self):
        floats = [1.1, 2.2, 3.3, 4.6]
        ints = [1, 2, 3, 5]
        strings = ['1.1', '2.2', '3.3', '4.6']
        assert ints == typecheck.set_type(floats, int)
        assert strings == typecheck.set_type(floats, str)
        assert [1.0 * x for x in ints] == typecheck.set_type(ints, float)
        assert [str(x) for x in ints] == typecheck.set_type(ints, str)
        assert ints == typecheck.set_type(strings, int)
        assert floats == typecheck.set_type(strings, float)

    def test_set_type_errors(self):
        strings = ['1.1', '2.2', '3.3', '4.6']
        with pytest.raises(ValueError):
            typecheck.set_type(strings, datetime)


class TestDataOperator:

    def test_dict_column_generator(self, sample_dict_data):
        """Assert that columns reflect the expected SQLalchemy column type"""
        data = DataOperator(sample_dict_data)
        columns = data.columns
        assert columns['strings'] == String, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['ints'] == Integer, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['floats'] == Float, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['dates'] == DateTime, "Incorrect SQLalchemy type returned by DataOperator.columns"

    def test_dict_row_generator(self, sample_dict_data):
        """Assert that rows are correctly formatted into a list of dictionaries"""
        data = DataOperator(sample_dict_data)
        rows = data.rows
        assert isinstance(rows, list)
        assert rows[0] == {'strings': 'hi', 'ints': 1, 'floats': 1.1, 'dates': datetime(2019, 1, 1)}
        assert rows[1] == {'strings': 'world', 'ints': 2, 'floats': 2.2, 'dates': datetime(2019, 1, 2)}
        assert rows[2] == {'strings': 'bye', 'ints': 3, 'floats': 3.3, 'dates': datetime(2019, 1, 3)}
        assert rows[3] == {'strings': 'school', 'ints': 4, 'floats': 4.4444, 'dates': datetime(2019, 1, 4)}

    def test_list_column_generator(self, sample_row_data):
        data = DataOperator(sample_row_data)
        columns = data.columns
        assert columns['strings'] == String, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['ints'] == Integer, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['floats'] == Float, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['dates'] == DateTime, "Incorrect SQLalchemy type returned by DataOperator.columns"

    def test_list_row_generator(self, sample_row_data):
        data = DataOperator(sample_row_data)
        rows = data.rows
        assert isinstance(rows, list)
        assert rows[0] == {'strings': 'hi', 'ints': 1, 'floats': 1.1, 'dates': datetime(2019, 1, 1)}
        assert rows[1] == {'strings': 'world', 'ints': 2, 'floats': 2.2, 'dates': datetime(2019, 1, 2)}
        assert rows[2] == {'strings': 'bye', 'ints': 3, 'floats': 3.3, 'dates': datetime(2019, 1, 3)}
        assert rows[3] == {'strings': 'school', 'ints': 4, 'floats': 4.4444, 'dates': datetime(2019, 1, 4)}


class TestDatabase:
    def test_db_exists(self, database):
        """Tests if the database exists.

        This test must be run after a task is executed against the database."""
        database.create_tables()  # Arbitrary blank call to the database creates a connection, and thus, the database
        database.clear_mappers()
        assert os.path.exists(database.path), "Database does not exist"


