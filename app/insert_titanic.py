#python app/insert_titanic.py
import psycopg2
from psycopg2.extras import execute_values # to insert multiple values at a time 
from psycopg2.extras import DictCursor
from dotenv import load_dotenv 
import pandas as pd
import os
import json
# from sqlalchemy import create_engine

load_dotenv() # reads the contents of the .env file and adds them to the environment

DB_NAME = os.getenv("DB_NAME", default="OOPS.")
DB_USER = os.getenv("DB_USER", default="OOPS.")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS.")
DB_HOST = os.getenv("DB_HOST", default="OOPS.")

### Connect to ElephantSQL-hosted PostgreSQL
connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

titanic_df = pd.read_csv(r'C:\Users\Khisl\Desktop\Elephant_SQL\app_data\titanic.csv') 

# titanic_df.to_sql('titanic_table', con=engine, if_exists='replace')
# connection = engine.raw_connection()
print("CONNECTION", type(connection))
### A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor(cursor_factory=DictCursor)
print("CURSOR", type(cursor))
insertion_query = f"INSERT INTO titanic_table (Survived, Pclass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Aboard, Fare) VALUES %s"
records = titanic_df.to_dict("records")
list_of_tuples = [(r['Survived'], r['Pclass'], r['Name'], r['Sex'], r['Age'], r['Siblings/Spouses Aboard'], r['Parents/Children Aboard'], r['Fare']) for r in records]
execute_values(cursor, insertion_query, list_of_tuples)

# ACTUALLY SAVE THE TRANSACTIONS
connection.commit()
cursor.close()
connection.close()

