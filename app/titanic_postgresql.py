#python app/insert_titanic.py
import psycopg2
from psycopg2.extras import execute_values # to insert multiple values at a time 
from psycopg2.extras import DictCursor
from dotenv import load_dotenv 
import pandas as pd
import os
import json

load_dotenv()  # reads contents of the .env file and adds them to the environment

DB_NAME = os.getenv("DB_NAME", default="OOPS.")
DB_USER = os.getenv("DB_USER", default="OOPS.")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS.")
DB_HOST = os.getenv("DB_HOST", default="OOPS.")

#Connect to ElephantSQL-hosted PostgreSQL
connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

print("CONNECTION", type(connection))

# A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor(cursor_factory=DictCursor)
print("CURSOR", type(cursor))

# How many passengers survived, and how many died?
query = "SELECT count(survived) as survived_passengers FROM titanic_table WHERE survived = 1;"
result = cursor.execute(query)
result1 = cursor.fetchall()
print(result1, " passengers survived.")

query1 = "SELECT count(survived) as not_survived_pass FROM titanic_table WHERE survived = 0;"

result2 = cursor.execute(query1)
result3 = cursor.fetchall()
print(result3, " passengers didn't survive.")

# How many passengers were in each class?

query2 = "SELECT pclass, count(survived) as num_people FROM titanic_table GROUP BY pclass;"

result4 = cursor.execute(query2)
result5 = cursor.fetchall()
print(result5, " passengers are in each class.")

# How many passengers survived/died within each class?
query3 = "SELECT pclass, count(survived) as survived_people FROM titanic_table WHERE survived = 1 GROUP BY pclass;"

result6 = cursor.execute(query3)
result7 = cursor.fetchall()
print(result7, " passengers survived within each class.")

query4 = "SELECT pclass, count(survived) as not_survived_people FROM titanic_table WHERE survived = 0 GROUP BY pclass;"

result8 = cursor.execute(query4)
result9 = cursor.fetchall()
print(result9, " passengers died within each class")

# What was the average age of survivors vs nonsurvivors?

query5 = "SELECT survived, avg(age) as avg_age FROM titanic_table GROUP BY survived;"

result10 = cursor.execute(query5)
result11= cursor.fetchall()
print("The average age of survivors vs nonsurvivors: ", result11)


# What was the average age of each passenger class?

query6 = "SELECT pclass, avg(age) as avg_age FROM titanic_table GROUP BY pclass;"

result12 = cursor.execute(query6)
result13= cursor.fetchall()
print("The average age of each passenger class: ", result13)

# What was the average fare by passenger class? 
query7 = "SELECT pclass, avg(fare) as avg_fare FROM titanic_table GROUP BY pclass"

result14 = cursor.execute(query7)
result15= cursor.fetchall()
print("The average fare by passenger class ", result15)

# By survival?

query8 = "SELECT survived, avg(fare) as avg_fare FROM titanic_table GROUP BY survived;"

result16 = cursor.execute(query8)
result17= cursor.fetchall()
print("The average fare by survival ", result17)

# How many siblings/spouses aboard on average, by passenger class? 

query9 = "SELECT pclass, avg(siblings_spouses_aboard) as avg_siblings_spouses FROM titanic_table GROUP BY pclass;"
result18 = cursor.execute(query9)
result19= cursor.fetchall()
print("The number of siblings/spouses aboard on average, by passenger class: ", result19)

# By survival?

query10 = "SELECT survived, avg(siblings_spouses_aboard) as avg_siblings_spouses FROM titanic_table GROUP BY survived;"
result20 = cursor.execute(query10)
result21= cursor.fetchall()
print("The number of siblings/spouses aboard on average, by passenger survival: ", result21)

# How many parents/children aboard on average, by passenger class? 

query11 = "SELECT pclass, avg(parents_children_aboard) as avg_siblings_spouses FROM titanic_table GROUP BY pclass;"
result22 = cursor.execute(query11)
result23= cursor.fetchall()
print("Parents/children aboard on average, by passenger class: ", result23)

# By survival?

query12 = "SELECT survived, avg(parents_children_aboard) as avg_parents_children FROM titanic_table GROUP BY survived;",

result24 = cursor.execute(query11)
result25= cursor.fetchall()
print("Parents/children aboard on average, by passenger survival: ",result25)

# Do any passengers have the same name?
query13 = "SELECT name, count(*) as duplicate_count FROM titanic_table GROUP BY name HAVING count(*) > 1;"
result26 = cursor.execute(query13)
result27= cursor.fetchall()
print("Passengers that have the same name: ", result27)

