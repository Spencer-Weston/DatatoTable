import datatotable.dbinterface
import os
import unittest

test = datatotable.dbinterface.DBInterface(os.getcwd())


class DBCreate(unittest.TestCase):
    def test_create_db(self):
        directory = os.getcwd()
        self.assertTrue(os.path.exists(directory), "directory does not exist")


# class TblCreate(unittest.TestCase):
