# DatatoTable
Datatotable converts data into SQLites tables automatically. The package is most useful when the name and number of columns in a table  are large or unknown ahead of time as SQLalchemy's Object Relational mapper (ORM) requires manual generation of tables and their columns. Datatotable automates this process. Yet, it still allows manual additions of constraints, foreign keys, and other properties as desired. 

## Quick Start
We first create a Database object which takes a name and location as input. The Database creates an engine we can pass to sqlalchemy.orm.Session to interact with the database. We'll use a temporary directory for this example.
```python
import datatotable
import tempfile
from sqlalchemy.orm import Session
temp_dir = tempfile.TemporaryDirectory()
db = Database(name="sample", directory=temp_dir.name)
session = Session(db.engine)
```

Next, we create a DataOperator object with the data we want to store. The DataOperator's column property reads the data and returns a dictionary with column names as keys and lists of column attributes as values. 
```python
raw_data = {"col1": [1,2,3,4], "col2": ["hello", "world", "from", "computer"], "col3": [10.1, 13.5, 23.2, 98.4]}
data = datatotable.DataOperator(raw_data)
columns = data.columns
print(columns)
{"col1": [Integer], "col2": [String], "col3": [Float]}
```

When passed to the Database.map_table() function, these columns are mapped to an empty object. After the table is mapped, call the create_tables() function to create the table in the database.
```python
db.map_table(tbl_name="example_tbl", column_types=columns)
db.create_tables()
```

After a table is created, we can access the table via the Database.table_mappings property. The table mapping can be used for queries or mapping data for inserts. The DataOperator's rows property returns the data formatted to create new row objects we can insert with SQLalchemy syntax via session. We can use a list comprehension to create the new row objects. 

```python
example_tbl = db.table_mappings["example_tbl"]
example_rows = [example_tbl(**row) for row in data.rows]
session.add_all(example_rows)
session.commit()
```
