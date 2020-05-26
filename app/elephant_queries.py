#app/elephant_queries.py
import psycopg2
from psycopg2.extras import execute_values # to insert multiple values at a time 
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from sqlalchemy import create_engine 
import pandas as pd
import os
import json

load_dotenv() # reads the contents of the .env file and adds them to the environment

DB_NAME = os.getenv("DB_NAME", default="OOPS.")
DB_USER = os.getenv("DB_USER", default="OOPS.")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS.")
DB_HOST = os.getenv("DB_HOST", default="OOPS.")

### Connect to ElephantSQL-hosted PostgreSQL
connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

print("CONNECTION", type(connection))

### A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor(cursor_factory=DictCursor)
print("CURSOR", type(cursor))

### An example query
cursor.execute('SELECT * from test_table;')
### type(result) <class 'tuple'>
# result = cursor.fetchone()
# (1, 'A row name', None)

result = cursor.fetchall()

for row in result:
    print("------")
    print(type(row))
    print(row)


#
# CREATE THE TABLE
#

print("-------------------")
query = f"""
CREATE TABLE IF NOT EXISTS test_table2 (
  id SERIAL PRIMARY KEY,
  name varchar(40) NOT NULL,
  data JSONB
);
"""
print("SQL:", query)
cursor.execute(query)


#
# INSERT SOME DATA
#

my_dict = { "a": 1, "b": ["dog", "cat", 42], "c": 'true' }
 
## ---------------FIRST APPROACH OF INSERTING DATA INTO A TABLE (one at a time)----------------

# insertion_query = f"INSERT INTO test_table2 (name, data) VALUES (%s, %s)"
# cursor.execute(insertion_query,
#  ('A rowwwww', 'null')
# )
# cursor.execute(insertion_query,
#  ('Another row, with JSONNNNN', json.dumps(my_dict))
# )


## ---------------SECOND APPROACH OF INSERTING DATA INTO A TABLE (multiple at a time)------

# h/t: https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
insertion_query = f"INSERT INTO test_table2 (name, data) VALUES %s"
execute_values(cursor, insertion_query, [
 ('A rowwwww', 'null'),
 ('Another row, with JSONNNNN', json.dumps(my_dict)),
 ('Third row', "3")
])

## ---------------THIRD APPROACH OF INSERTING DATA INTO A TABLE (DataFrame)------


df = pd.DataFrame([
  ['A rowwwww', 'null'],
  ['Another row, with JSONNNNN', json.dumps(my_dict)],
  ['Third row', "null"],
  ["Pandas Row", "null"]
])

records = df.to_dict("records") #> [{0: 'A rowwwww', 1: 'null'}, {0: 'Another row, with JSONNNNN', 1: '{"a": 1, "b": ["dog", "cat", 42], "c": "true"}'}, {0: 'Third row', 1: '3'}, {0: 'Pandas Row', 1: 'YOOO!'}]
list_of_tuples = [(r[0], r[1]) for r in records]

execute_values(cursor, insertion_query, list_of_tuples)


#
# QUERY THE TABLE
#

print("-------------------")
query = f"SELECT * FROM test_table2;"
print("SQL:", query)
cursor.execute(query)
for row in cursor.fetchall():
    breakpoint()
    print(row)

# ACTUALLY SAVE THE TRANSACTIONS
connection.commit()

cursor.close()
connection.close()
