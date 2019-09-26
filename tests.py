from datatotable.database import Database
from datatotable.data import DataOperator
from datetime import datetime, timedelta
import os
import pandas
import pytest
from sqlalchemy import Integer, Float, String, DateTime, Boolean


@pytest.fixture
def database():
    return Database("test")


@pytest.fixture
def sample_data_operator():
    test_data = {"strings": ["hi", "world", "bye", "school"], "ints": [1, 2, 3, 4],
                 "floats": [1.1, 2.2, 3.3, 4.4444], "dates": [datetime(2019, 1, 1)+timedelta(i) for i in range(4)]}
    return DataOperator(test_data)


class TestDataOperator:

    def test_column_generator(self, sample_data_operator):
        """Assert that columns reflect the expected SQLalchemy column type"""
        columns = sample_data_operator.columns
        assert columns['strings'] == String, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['ints'] == Integer, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['floats'] == Float, "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['dates'] == DateTime, "Incorrect SQLalchemy type returned by DataOperator.columns"

    def test_row_generator(self, sample_data_operator):
        """Assert that rows are correctly formatted into a list of dictionaries"""
        rows = sample_data_operator.rows
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
        assert os.path.exists(database.location), "Database does not exist"


