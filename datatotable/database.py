"""
This file contains a Database class which dictates table creation, deletion, and access.
"""

from pathlib import Path
import os
from sqlalchemy import Column, Integer, Table
from sqlalchemy import create_engine, MetaData, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, mapper, clear_mappers
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base

# Local Imports
from datatotable import data


class Database:
    """Database provides table creation and deletion, table access, and database information.

    Attributes:
         location: The location of the database in relation to the working directory with the database prefix (e.g. SQLite:///)
         path: The absolute path to the database
         engine: SQLalchemy engine for accessing the database
         metadata: Metadata for the engine, used mostly for table access / reflection
         Base: SQLalchemy declarative_base() used for table creation
    """

    class Template(object):
        """Blank template to map tables to with the sqlalchemy mapper function

        Note:
            Template can only be mapped to one table at a time. Use clear_mappers to free the template for new tables
        """
        pass

    def __init__(self, name, directory=None):
        """Initialize macro-level SQLalchemy objects as class attributes (engine, metadata, base).

        A session will allow interaction with the DB.

        Args:
            directory: The directory where the database is stored or will be created
            name: The name of the database
        """
        if directory:
            prefix = r"sqlite:///"
            self.path = Path(directory).joinpath("{}.db".format(name))
            self.location = "{}{}".format(prefix, self.path)
        else:
            self.location = r"sqlite:///{}.db".format(name)
            self.path = os.path.join(os.getcwd(), "{}.db".format(name))
        self.engine = create_engine(self.location)
        self.metadata = MetaData(self.engine)
        self.Base = declarative_base()
        # self.Base = automap_base()
        # self.Base.prepare()

    @property
    def tables(self):
        """Return a dictionary of tables from the database"""
        meta = MetaData(bind=self.engine)
        meta.reflect(bind=self.engine)
        return meta.tables

    @property
    def table_mappings(self):
        """Find and return the specified table mappings or return all table mappings"""
        self.metadata.reflect(self.engine)
        Base = automap_base(metadata=self.metadata)
        Base.prepare()

        return Base.classes

    def table_exists(self, tbl_name):
        """Check if a table exists in the database; Return True if it exists and False otherwise."""
        if tbl_name in self.tables:
            return True
        else:
            return False

    def create_tables(self):
        """Creates all tables which have been made or modified with the Base class of the Database

        Note that existing tables which have been modified, such as by adding a relationship, will be updated when
        create_tables() is called. """
        self.metadata.create_all(self.engine)

    def map_table(self, tbl_name, column_types, constraints=None):
        """Map a table named tbl_name and with column_types to Template, add constraints if specified.

        Note: Foreign key constraints should likely be added to the mapped table explicitly rather than in this function.

        A primary key named 'id' is automatically inserted as the first column of every table. This is a design decision
        to simplify table creation as the use of other columns as primary keys will only be the correct decision in a
        minority of circumstances.

        Args:
            tbl_name: The name of the table to be mapped
            column_types: A dictionary with column names as keys and sql types as values
            constraints: A dictionary of desired constraints where the constraints (Such as UniqueConstraint) are keys
            and the columns to be constrained is a list of string column names
        """
        columns = self._generate_columns(column_types)
        if constraints:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *columns,
                      *(constraint(*columns) for constraint, columns in constraints.items()),
                      )
        else:
            t = Table(tbl_name, self.metadata, Column('id', Integer, primary_key=True),
                      *columns
                      )

        mapper(self.Template, t)

    @staticmethod
    def _generate_columns(columns):
        """Take columns where key is the column name and value is the column type into SQLalchemy columns.

        To use additional arguments, such as constraints, specify column values as a list where the constraints are
        elements of the list"""
        column_list = []
        for col_name, args in columns.items():
            try:
                kwargs = []
                for idx in range(len(args)):
                    if type(args[idx]) is dict:
                        kwargs.append(args[idx])
                        args.pop(idx)
                col = Column(col_name, *args)
                for kwarg in kwargs:
                    keys = [*kwarg]
                    if len(keys) > 1:
                        raise Exception('Expected 1 key, {} were given'.format(len(keys)))
                    else:
                        key = keys[0]
                    col.__setattr__(key, kwarg[key])
                column_list.append(col)  # Unpacks additional column arguments
            except TypeError:  # if no additional arguments, just make a standard name and type column
                column_list.append(Column(col_name, args))
        return column_list

    @staticmethod
    def clear_mappers():
        clear_mappers()

    def drop_table(self, drop_tbl):
        """Drops the specified table from the database.

        Note: If the database uses SQLite, tables with foreign key constraints cannot be dropped. """
        self.metadata.reflect(bind=self.engine)
        drop_tbls = self.metadata.tables[drop_tbl]
        drop_tbls.drop()
        self.metadata = MetaData(bind=self.engine)  # Updates the metadata to reflect changes


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """SQLalchemy listener function to allow foreign keys in SQLite"""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


if __name__ == "__main__":
    import tempfile
    from datetime import datetime, timedelta
    from datatotable.data import DataOperator
    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import Session, relationship

    # Setup temporary file
    temp_dir = tempfile.TemporaryDirectory()
    db = Database(name="test", directory=temp_dir.name)
    session = Session(db.engine)

    # Setup test parent table
    test_data = {"strings": ["hi", "world", "bye", "school"], "ints": [1, 2, 3, 4],
                 "floats": [1.1, 2.2, 3.3, 4.4444], "dates": [datetime(2019, 1, 1) + timedelta(i) for i in range(4)]}
    data = DataOperator(test_data)
    columns = data.columns
    db.map_table('parent', columns)
    db.create_tables()
    db.clear_mappers()
    parent_tbl = db.table_mappings["parent"]
    session.add_all([parent_tbl(**row) for row in data.rows])
    session.commit()
    del parent_tbl  # Delete b/c this object will not be aware of relationships added later

    # Setup child table
    foreign_data = DataOperator({"parent_id": [1, 2]})
    foreign_cols = foreign_data.columns
    foreign_cols['parent_id'].append(ForeignKey('parent.id', ondelete="CASCADE"))
    foreign_cols['parent_id'].append({"nullable": False})

    db.map_table('child', foreign_cols)
    db.Template.parent = relationship("parent", backref=backref("children", passive_deletes=True))
    db.create_tables()

    # # Test if parent relationship cascades
    # parent_tbl = db.table_mappings['parent']
    # parent_tbl.children = relationship('child', cascade="all, delete", backref="parents")
    # db.create_tables()

    # Add data to child with a foreign key
    child_tbl = db.table_mappings["child"]
    rows = foreign_data.rows
    session.add(child_tbl(**rows[0]))
    session.commit()
    session.add(child_tbl(**rows[1]))
    session.commit()

    # Check cascade
    parent_tbl = db.table_mappings["parent"]
    c = session.query(child_tbl).first()
    p1 = session.query(parent_tbl).filter(parent_tbl.id == 1).all()[0]
    p2 = session.query(parent_tbl).filter(parent_tbl.id == 2).all()[0]
    # p2.id = 99
    # session.delete(p1)  # Delete parent.id = 1, should set child.parent_id to Null
    #session.add(p2)  # raises error b/c foreign key constraint failed
    session.commit()

    # Check that relationships are established
    parent_tbl = db.table_mappings["parent"]
    p = parent_tbl()
    c = child_tbl()
    print(p.child_collection)
    temp_dir.cleanup()

