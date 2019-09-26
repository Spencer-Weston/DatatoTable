"""
This file contains a DBInterface class which dictates table creation, deletion, and access.
"""

import os
from sqlalchemy import Column, Integer, Table
from sqlalchemy import create_engine, MetaData, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, clear_mappers
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base

# Local Imports
from datatotable import manipulator


class DBInterface:
    """DBInterface provides table creation and deletion, table access, and database information.

    Attributes:
         location: The path to the database
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
            # ToDo: This clause will not work
            self.location = os.path.realpath(os.path.join(directory, name))
        else:
            self.location = r"sqlite:///{}.db".format(name)
        self.engine = create_engine(self.location, pool_pre_ping=True)
        self.metadata = MetaData(self.engine)
        self.Base = declarative_base()

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

        # ToDo: Used if not a property DELETE
        # mapped_tables = [Base.classes[name] for name in table_names]
        # if len(mapped_tables) == 1:
        #    return mapped_tables[0]
        # else:
        #    return mapped_tables

    def table_exists(self, tbl_name):
        """Check if a table exists in the database; Return True if it exists and False otherwise."""
        if tbl_name in self.tables:
            return True
        else:
            return False

    def create_tables(self):
        """Creates all tables which have been made or modified with the Base class of the DBInterface

        Note that existing tables which have been modified, such as by adding a relationship, will be updated when
        create_tables() is called. """
        self.metadata.create_all(self.engine)

    def map_table(self, tbl_name, column_types, constraints=None):
        """Map a table named tbl_name and with column_types to Template, add constraints if specified.

        Note: Foreign key constraints should likely be added to the mapped table explicitly rather than in this function

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
        """Take columns where key is the column name and value is the column type into SQLlachemy columns.

        To use additional arguments, such as constraints, specify column values as a list where the constraints are
        elements of the list"""
        column_list = []
        for key, value in columns.items():
            try:
                column_list.append(Column(key, *value))  # Unpacks additional column arguments
            except TypeError:  # if no additional arguments, just make a standard name and type column
                column_list.append(Column(key, value))
        return column_list

    @staticmethod
    def clear_mappers():
        clear_mappers()

    # def insert_row(self, table, row):
    #     """Insert a single row into the specified table in the engine
    #
    #     ToDo: No row access in DBinterface"""
    #     conn = self.engine.connect()
    #     table = self.get_tables(table)
    #     conn.execute(table.insert(), row)
    #     conn.close()
    #     # Rows formatted as
    #     #   [{'l_name': 'Jones', 'f_name': 'bob'},
    #     #   {'l_name': 'Welker', 'f_name': 'alice'}])
    #
    # def insert_rows(self, table, rows):
    #     """Insert rows into the specified table.
    #
    #     Uses sqlalchemy's "Classic" method. ORM database interactions are mediated by sessions.
    #
    #     ToDo: No row access in DBinterface
    #     """
    #     table = self.get_tables(table)
    #     conn = self.engine.connect()
    #     for row in rows:
    #         conn.execute(table.insert(), row)
    #     conn.close()

    def drop_table(self, drop_tbl):
        """Drops the specified table from the database"""
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


# def database_file(calling_file_path):
#     """Return the database file path with the path modified in relation to the path the function is called from.
#
#     The base path is r"sqlite:///outputs//nba_db.db". This function modifies that path in relation to the calling file
#     path by inserting ..// to the front of the base path. So a file nested one level below the root directory becomes
#     r"sqlite:///..//outputs//nba_db.db"
#     """
#     head_path = project_directory()
#     head_folder = os.path.split(head_path)[1]
#
#     if os.path.realpath(calling_file_path) in head_path:
#         # If NBApredict is imported from outside the project, replace calling_file_path with head_path
#         #
#         calling_file_path = head_path
#
#     calling_file_path = calling_file_path.replace("\\", "/")
#     print("Calling_file_path:", calling_file_path)
#     sub_dirs = []
#     split_path = os.path.split(calling_file_path)
#     path = split_path[0]
#     folder = split_path[1]
#     while folder != head_folder:
#         sub_dirs.append(folder)
#         split_path = os.path.split(path)
#         path = split_path[0]
#         folder = split_path[1]
#
#     if len(sub_dirs) > 0:
#         modified_path = calling_file_path
#         for folder in sub_dirs:
#             modified_path = rreplace(modified_path, folder, "..", 1)
#
#         path_addin = modified_path.split(head_folder)[1]
#         path_addin = path_addin.replace("/", "//")
#         while path_addin[0] == "/":
#             path_addin = path_addin[1:]
#         db_path = r"sqlite:///{}//outputs//nba_db.db".format(path_addin)
#         return db_path
#     else:
#         return r"sqlite:///outputs//nba_db.db"