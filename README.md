# DatatoTable
Datatotable converts data into SQLites tables automatically. The package is most useful when the name and number of columns in a table  are large or unknown ahead of time as SQLalchemy's Object Relational mapper (ORM) requires manual generation of tables and their columns. Datatotable automates this process. Yet, it still allows manual additions of constraints, foreign keys, and other properties as desired. 

## Quick Start
The minimal use case of datatotable creates a table from a data source, and then inserts the data. This example walks through this process. Datatotable functions through two classes Database, which provides access to the database, and DataOperator, which interprets data for use with SQLalchemy. First, import these classes. Next, import a SQLalchemy Session class for inserts and tempfile to create a temporary directory.

```python
from datatotable.database import Database
from datatotable.data import DataOperator
from sqlalchemy.orm import Session
import tempfile
```
To connect to or create a database, specify the name and directory of the database on Database's instantiation. Then, connect a session to the database via Database's engine property.
```python
temp_dir = tempfile.TemporaryDirectory()
db = database.Database(name="sample", directory=temp_dir.name)
session = Session(db.engine)
```

Next, pass data to DataOperator. The DataOperator's column property reads the data and returns a dictionary with column names as keys and lists of column attributes as values. 
```python
raw_data = {"col1": [1,2,3,4], "col2": ["hello", "world", "from", "computer"], "col3": [10.1, 13.5, 23.2, 98.4]}
data = datatotable.data.DataOperator(raw_data)
columns = data.columns
print(columns)
>>> {'col1': [<class 'sqlalchemy.sql.sqltypes.Integer'>], 'col2': [<class 'sqlalchemy.sql.sqltypes.String'>], 'col3': [<class 'sqlalchemy.sql.sqltypes.Float'>]}
```

Pass the columns to Database's map_table() function to create a mapped SQLalchemy Base class. After the table is mapped, call the create_tables() function to create the table in the database.
```python
db.map_table(tbl_name="example_tbl", column_types=columns)
db.create_tables()
```

With the table created, access the table via the Database.table_mappings property. The table mapping can be used for queries or mapping data for inserts. The row property of DataOperator returns a list of dictionaries where each dictionary is a row in the database. Use ** notation to unpack each dictionary into a table object. A list comprehension, as used here, can create a list of table objects. Add these objects using Session.add_all().
```python
example_tbl = db.table_mappings["example_tbl"]
example_rows = [example_tbl(**row) for row in data.rows]
session.add_all(example_rows)
session.commit()
```

Now, check if the data is in the database.
```python
print(session.query(example_tbl).count())
>>> 4
```

## Version 0.3
This version is the first valid version uploaded to PyPi. It provides access to a database, automatically creates tables, and coerces data into a format that can be inserted into a table. It passes all tests in the test suite, but the test suite will need to be fleshed out as users come across problems not anticipated currently. 

## Future Development
### Documentation
1. Use Sphinx to create API documentation on Read the Docs
2. A thorough tutorial to explain how constraints and relationships are established
### Functionality
1. Add functionality to relate SQLalchemy ORM objects when tables are created with foreign keys. This is a manual process currently.
2. Allow simultaneous creation of multiple tables. Tables must be created one by one currently. 

## Author
Spencer Weston

spencerweston3214@gmail.com

## License
MIT
