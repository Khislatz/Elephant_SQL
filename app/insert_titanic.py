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

#Connect to ElephantSQL-hosted PostgreSQL
connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

titanic_df = pd.read_csv(r'C:\Users\Khisl\Desktop\Elephant_SQL\app_data\titanic.csv') 

print("CONNECTION", type(connection))

# A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor(cursor_factory=DictCursor)
print("CURSOR", type(cursor))

insertion_query = f"INSERT INTO titanic_table (Survived, Pclass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Aboard, Fare) VALUES %s"
# Convert DataFrame into a list of tuples and inserting data into titanic table   
records = titanic_df.to_dict("records")
list_of_tuples = [(r['Survived'], r['Pclass'], r['Name'], r['Sex'], r['Age'], r['Siblings/Spouses Aboard'], r['Parents/Children Aboard'], r['Fare']) for r in records]
# Invoking a function execute_values 
execute_values(cursor, insertion_query, list_of_tuples)

# ACTUALLY SAVE THE TRANSACTIONS
connection.commit()
cursor.close()
connection.close()


print("-------------------")


# How many people survived and how many didn't

query = """
SELECT COUNT(*)
FROM titanic_table
WHERE Survived=1;"""
result = cursor.execute(query).fetchone()
print("There are :", list(query), " people survived")


query1 = """
SELECT COUNT(*)
FROM titanic_table
WHERE Survived=0;"""
result1 = cursor.execute(query1).fetchone()
print("There are :", list(result1), " people survived")



# Which class survived ordered by pclass 

query2 = """
SELECT 
	survived
	,pclass
FROM titanic_table
WHERE survived=1
ORDER BY pclass
LIMIT 20;
"""
result2 = cursor.execute(query2).fetchone()
print("Poeple sruvived were mostly from the first class :", list(result2)")


# What was the fare depending on whether there were siblings or spouses aboard 

query3 = """
SELECT 
	siblings_spouses_aboard
	,fare
FROM titanic_table
ORDER BY siblings_spouses_aboard ASC, fare DESC;
"""
result3 = cursor.execute(query3).fetchone()
print("The fare depending on whether there were siblings or spouses aboard, list(result3)")


# Who out of surived people in the second class had parents/children aboard

query4 = """
        SELECT 
	survived
	,name
	,pclass
	,parents_children_aboard
FROM titanic_table
WHERE survived=1 AND pclass=2;
        """
print(query4)
cursor.execute(query4)
for row in cursor.fetchall():
    print(f"Survived: {row['survived']}, Pclass: {row['pclass']}, Name: {row['name']}, Parents_Children_Aboard: {row['parents_children_aboard']}")