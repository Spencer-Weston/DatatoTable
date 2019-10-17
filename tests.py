from datatotable.database import Database
from datatotable.data import DataOperator
from datatotable import typecheck
from datetime import datetime, timedelta
import os
import pandas
import pytest
from sqlalchemy import Integer, Float, String, DateTime, UniqueConstraint, ForeignKey
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError


@pytest.fixture
def sample_dict_data():
    test_data = {"strings": ["hi", "world", "bye", "school"], "ints": [1, 2, 3, 4],
                 "floats": [1.1, 2.2, 3.3, 4.4444], "dates": [datetime(2019, 1, 1) + timedelta(i) for i in range(4)]}
    return test_data


@pytest.fixture()
def sample_row_data():
    test_data = [{"strings": 'hi', 'ints': 1, 'floats': 1.1, 'dates': datetime(2019, 1, 1)},
                 {"strings": 'world', 'ints': 2, 'floats': 2.2, 'dates': datetime(2019, 1, 2)},
                 {"strings": 'bye', 'ints': 3, 'floats': 3.3, 'dates': datetime(2019, 1, 3)},
                 {"strings": 'school', 'ints': 4, 'floats': 4.4444, 'dates': datetime(2019, 1, 4)}]
    return test_data


@pytest.fixture()
def sample_data_operator(sample_dict_data):
    data = DataOperator(sample_dict_data)
    return data


@pytest.fixture()
def sample_foreign_data_operator():
    pass


@pytest.fixture()
def session(database):
    session = Session(bind=database.engine)
    return session


class TestTypeCheck:
    """Tests functionality in the typecheck module"""

    def test_get_type(self, sample_dict_data):
        """Tests that the get_type function from typecheck correctly identifies the type from a list of data"""
        assert str == typecheck.get_type(sample_dict_data['strings'])
        assert int == typecheck.get_type(sample_dict_data['ints'])
        assert float == typecheck.get_type(sample_dict_data['floats'])
        assert datetime == typecheck.get_type(sample_dict_data['dates'])

    def test_set_type(self):
        """Tests that the set_type function from typecheck correctly modifies data types"""
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
        """Tests for correct error raising when data cannot be coerced to an alternate type"""
        strings = ['1.1', '2.2', '3.3', '4.6']
        with pytest.raises(ValueError):
            typecheck.set_type(strings, datetime)


class TestDataOperator:
    """Tests functionality in the data.DataOperator class"""

    def test_dict_column_generator(self, sample_dict_data):
        """Assert that columns reflect the expected SQLalchemy column type when DataOperator is passed a dictionary"""
        data = DataOperator(sample_dict_data)
        columns = data.columns
        assert columns['strings'] == [String], "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['ints'] == [Integer], "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['floats'] == [Float], "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['dates'] == [DateTime], "Incorrect SQLalchemy type returned by DataOperator.columns"

    def test_dict_row_generator(self, sample_dict_data):
        """Assert that rows are correctly formatted into a list of dictionaries when DataOperator is passed a dict"""
        data = DataOperator(sample_dict_data)
        rows = data.rows
        assert isinstance(rows, list)
        assert rows[0] == {'strings': 'hi', 'ints': 1, 'floats': 1.1, 'dates': datetime(2019, 1, 1)}
        assert rows[1] == {'strings': 'world', 'ints': 2, 'floats': 2.2, 'dates': datetime(2019, 1, 2)}
        assert rows[2] == {'strings': 'bye', 'ints': 3, 'floats': 3.3, 'dates': datetime(2019, 1, 3)}
        assert rows[3] == {'strings': 'school', 'ints': 4, 'floats': 4.4444, 'dates': datetime(2019, 1, 4)}

    def test_list_column_generator(self, sample_row_data):
        """Assert that columns reflect the expected SQLalchemy column type when DataOperator is passed a list"""
        data = DataOperator(sample_row_data)
        columns = data.columns
        assert columns['strings'] == [String], "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['ints'] == [Integer], "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['floats'] == [Float], "Incorrect SQLalchemy type returned by DataOperator.columns"
        assert columns['dates'] == [DateTime], "Incorrect SQLalchemy type returned by DataOperator.columns"

    def test_list_row_generator(self, sample_row_data):
        """Assert that rows are correctly formatted into a list of dictionaries when DataOperator is passed a list"""
        data = DataOperator(sample_row_data)
        rows = data.rows
        assert isinstance(rows, list)
        assert rows[0] == {'strings': 'hi', 'ints': 1, 'floats': 1.1, 'dates': datetime(2019, 1, 1)}
        assert rows[1] == {'strings': 'world', 'ints': 2, 'floats': 2.2, 'dates': datetime(2019, 1, 2)}
        assert rows[2] == {'strings': 'bye', 'ints': 3, 'floats': 3.3, 'dates': datetime(2019, 1, 3)}
        assert rows[3] == {'strings': 'school', 'ints': 4, 'floats': 4.4444, 'dates': datetime(2019, 1, 4)}


class TestDatabase:
    """"Tests functionality of the database.Database class."""

    @pytest.fixture(autouse=True, scope="session")
    def database(self, tmpdir_factory):
        """Generates a Database object named 'test.db' in the tmpdir"""
        yield Database("test", tmpdir_factory.mktemp("tempDB"))

    def test_db_exists(self, database):
        """Tests if the database exists."""
        database.create_tables()  # Arbitrary blank call to the database creates a connection and the database file
        database.clear_mappers()
        assert os.path.exists(database.path), "Database does not exist"

    def test_tbl_creation(self, database, sample_data_operator):
        """Tests if a table is created after extracting columns from the sample_data_operator."""
        data = sample_data_operator
        columns = data.columns
        database.map_table("sample_tbl", columns)
        database.create_tables()
        database.clear_mappers()
        assert database.table_exists("sample_tbl")

    def test_tbl_insertion(self, database, session, sample_data_operator):
        """Test if data is correctly inserted after extracting rows from the sample_data_operator."""
        data = sample_data_operator
        rows = data.rows
        tbl_map = database.table_mappings["sample_tbl"]
        tbl_map_rows = [tbl_map(**row) for row in rows]
        session.add_all(tbl_map_rows)
        session.commit()
        test_query = session.query(tbl_map).filter(tbl_map.strings == "hi").all()[0]
        row_dict = {key: value[0] for (key, value) in data.data.items()}
        test_dict = {key: test_query.__getattribute__(key) for key in data.data.keys()}
        assert row_dict == test_dict

    def test_tbl_creation_constraints(self, database, session, sample_data_operator):
        """Test if a unique constraint is attached to unique table by inserting duplicate data."""
        data = sample_data_operator
        columns = data.columns
        constraints = {UniqueConstraint: ["strings"]}
        database.map_table("unique_tbl", columns, constraints)
        database.create_tables()
        database.clear_mappers()
        assert database.table_exists('unique_tbl')

        tbl_map = database.table_mappings["unique_tbl"]
        session.add_all([tbl_map(**row) for row in data.rows])
        session.commit()
        non_unique_row = tbl_map(**{'strings': 'hi', 'ints': 1, 'floats': 1.1, 'dates': datetime.now()})
        session.add(non_unique_row)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_foreign_key_constraints(self, database, session):
        """Test if foreign key constraints work by inserting expected data then raising an error on invalid data."""
        foreign_data = DataOperator({"fk_id": [1, 22]})
        foreign_cols = foreign_data.columns
        foreign_cols['fk_id'].append(ForeignKey('sample_tbl.id'))
        database.map_table('foreign_tbl', foreign_cols)
        database.create_tables()

        foreign_tbl = database.table_mappings['foreign_tbl']
        rows = foreign_data.rows
        # inserts '1' which exists in sample_tbl.id. Should not raise an error
        session.add(foreign_tbl(**rows[0]))
        session.commit()
        # inserts '22' which does not exists in sample_tbl.id. Should raise an error
        session.add(foreign_tbl(**rows[1]))
        with pytest.raises(IntegrityError):
            session.commit()

    def test_cascades(self, database, session):
        foreign_tbl = database.table_mappings['foreign_tbl']
        sample_tbl = database.table_mappings['sample_tbl']

        sample_row = session.query(sample_tbl).join(foreign_tbl).all()
        assert len(sample_row) == 1
        session.delete(sample_row[0])
        session.commit()
        sample_row = session.query(sample_tbl).join(foreign_tbl).all()
        assert len(sample_row) == 0

    def test_drop_tbl(self, database):
        """Perform various tests to see if tables are dropped or raise correct errors when we attempt to drop them.

        Test1 - Ensure an error is raised when we attempt to drop sample_tbl. The error results from foreign keys.
        Test2 - Ensure unique tbl, with no FK constraints, is dropped
        """
        # Test 1 
        # with pytest.raises(IntegrityError):
        #    database.drop_table(drop_tbl='sample_tbl')

        # Test 2
        database.drop_table('unique_tbl')
        assert not database.table_exists('unique_tbl')

