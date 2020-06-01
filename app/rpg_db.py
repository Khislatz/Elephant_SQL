## SQLITE INTO POSTGRESQL

import psycopg2
from psycopg2.extras import execute_values # to insert multiple values at a time 
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
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

load_dotenv() # reads the contents of the .env file and adds them to the environment

# connect to postreSQL credentials 
NAME = os.getenv("NAME", default="OOPS.")
USER = os.getenv("USER", default="OOPS.")
PASSWORD = os.getenv("PASSWORD", default="OOPS.")
HOST = os.getenv("HOST", default="OOPS.")


#Connect to ElephantSQL-hosted PostgreSQL
connect_postgre = psycopg2.connect(dbname=NAME, user=USER, password=PASSWORD, host=HOST)
print("CONNECTION", type(connect_postgre))

### A "cursor", a structure to iterate over db records to perform queries
cursor_postgre = connect_postgre.cursor()
print("CURSOR", type(cursor_postgre))


tabnames = ['charactercreator_character', 'charactercreator_character_inventory', 'charactercreator_cleric',
'charactercreator_fighter', 'charactercreator_mage', 'charactercreator_necromancer',
'charactercreator_thief', 'armory_item', 'armory_weapon','auth_group', 'auth_group_permissions', 
'auth_permission', 'auth_user', 'auth_user_groups', 'auth_user_user_permissions', 
'django_admin_log', 'django_content_type', 'django_migrations', 'django_session', 'sqlite_sequence'] 

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
        print('Created', table)
 
        # if connect_postgre:
        #     connect_postgre.close()


    except psycopg2.DatabaseError as e:
        print ('Error %s' % e) 
        connect_postgre = psycopg2.connect(dbname=NAME, user=USER, password=PASSWORD, host=HOST)
        cursor_postgre = connect_postgre.cursor()
        # sys.exit(1)

    finally: 
        print("Complete")

# ACTUALLY SAVE THE TRANSACTIONS
# connect_sqlite.commit()
connect_sqlite.close()
connect_postgre.close()





