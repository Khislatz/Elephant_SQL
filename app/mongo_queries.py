# app/mongo_queries.py

import pymongo
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values # to insert multiple values at a time 
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine 
import pandas as pd
import os, sys
import json
import sqlite3

# construct a path to wherever your database exists
#DB_FILEPATH = "rpg_db.db"

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "app_data", "rpg_db.sqlite3")
connect_sqlite = sqlite3.connect(DB_FILEPATH)
connect_sqlite.row_factory = sqlite3.Row
print("CONNECTION:", connect_sqlite)
cursor_sqlite = connect_sqlite.cursor()
print("CURSOR", cursor_sqlite)

load_dotenv()

MONGO_USER = os.getenv("MONGO_USER", default="OOPS")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
MONGO_CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")

connection_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)

client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)

#  db = client.test_database -- "test_database" is the name that we give to our new database
db = client.rpg_db 
print("----------------")
print("DB:", type(db), db)

# collection = db.pokemon_test # "pokemon_test is a name of the collection or table 

collection = db.rpg_collection 
print("----------------")
print("COLLECTION:", type(collection), collection)

tabnames = ['charactercreator_character', 'charactercreator_character_inventory', 'charactercreator_cleric',
'charactercreator_fighter', 'charactercreator_mage', 'charactercreator_necromancer',
'charactercreator_thief', 'armory_item', 'armory_weapon','auth_group', 'auth_group_permissions', 
'auth_permission', 'auth_user', 'auth_user_groups', 'auth_user_user_permissions', 
'django_admin_log', 'django_content_type', 'django_migrations', 'django_session'] 

store_tables = {}
for table in tabnames:
    query = """
            SELECT * 
            FROM %s
            """ %table
    store_tables[table] = pd.read_sql_query(query, engine)
collection.insert_many(tabnames)

tabnames1 = []

for table in tabnames:
    cursor_sqlite.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%s';" % table)
    get_tab = cursor_sqlite.fetchall()
    for item in get_tab:
        tabnames1.append(item[0])

for table in tabnames1:
    cursor_sqlite.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?;", (table,))
    create = cursor_sqlite.fetchone()[0]
    cursor_sqlite.execute("SELECT * FROM %s;" %table)
    rows=cursor_sqlite.fetchall()
    if len(rows) > 0:
        colcount=len(rows[0])
        pholder='%s,'*colcount
        newholder=pholder[:-1]

    try:
        connect_postgre
        # # cursor_postgre.execute("SET search_path TO %s;" %pgschema)
        cursor_postgre
        cursor_postgre.execute("DROP TABLE IF EXISTS %s CASCADE;" %table)
        create = create.replace("AUTOINCREMENT", "").replace("unsigned", "").replace("bool", "integer").replace("datetime", "timestamp")
        cursor_postgre.execute(create)
        cursor_postgre.executemany("INSERT INTO %s VALUES (%s);" % (table, newholder),rows)
        connect_postgre.commit()
        print('Created', table)A2vv 
 
        # if connect_postgre:
        #     connect_postgre.close()


    except psycopg2.DatabaseError as e:
        print ('Error %s' % e) 
        client = pymongo.MongoClient(connection_uri)
        db = client.rpg_db 
        # sys.exit(1)

    finally: 
        print("Complete")

db.rpg_db.insert_many("rpg_db.sqlite3")

# ACTUALLY SAVE THE TRANSACTIONS
# connect_sqlite.commit()
connect_sqlite.close()
    

