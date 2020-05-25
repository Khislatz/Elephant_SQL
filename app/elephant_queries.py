#app/elephant_queries.py

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
import os

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
    breakpoint()
    print("------")
    print(type(row))
    print(row)

