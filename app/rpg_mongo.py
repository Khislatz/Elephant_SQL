# app/mongo_queries.py

import pymongo
import os
from dotenv import load_dotenv
import sqlite3

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "app_data", "rpg_db.sqlite3")
connect_sqlite = sqlite3.connect(DB_FILEPATH)
connect_sqlite.row_factory = sqlite3.Row
print("CONNECTION:", connect_sqlite)
cursor_sqlite = connect_sqlite.cursor()
print("CURSOR", cursor_sqlite)

load_dotenv()

DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)

client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)

print(dir(client))
print("DB_NAMES:", client.database_names)


#  db = client.test_database -- "test_database" is the name that we give to our new database
db = client.rpg_db
print("----------------")
print("DB:", type(db), db)

# collection = db.pokemon_test # "pokemon_test is a name of the collection or table 

collection = db.rpg_collection 
print("----------------")
print("COLLECTION:", type(collection), collection)

print("----------------")
print("COLLECTIONS:")
print(db.list_collection_names())

collection.insert_one({
    "name": "Pikachu",
    "level": 30,
    "exp": 76000000000,
    "hp": 400,
    "fav_ice_cream_flavors":["vanilla_bean", "choc"],
    "stats": {"a":1, "b":2, "c":[1,2,3]}
})
print("DOCS:", collection.count_documents({})) # same as SELECT count(distinct_id) FROM pokemon
print(collection.count_documents({"name": "Pikachu"})) # same as SELECT count(distinct_id) FROM pokemon WHERE name = "Pikachu"


mewtwo = {
    "name": "Mewtwo",
    "level": 100,
    "exp": 76000000000,
    "hp": 450,
    "strength": 550,
    "intelligence": 450,
    "dexterity": 300,
    "wisdom": 575
}

cubone = {
    "name": "Cubone",
    "level": 20,
    "exp": 35000,
    "hp": 80,
    "strength": 600, 
    "intelligence": 60,
    "dexterity": 200,
    "wisdom": 200
}

blastoise = {
    "name": "Blastoise",
    "lvl": 70,
}

charmander = {
    "nameee": "Charmander", #misspelled name attribute
    "level": 70,
    "random_stat": {"a":2}
}

pokemon_team = [mewtwo, blastoise, cubone, charmander]
collection.insert_many(pokemon_team)

print("DOCS:", collection.count_documents({})) # same as SELECT count(distinct_id) FROM pokemon


pikas = list(collection.find({"name": "Pikachu"})) # SELECT * FROM pokemon WHERE name = "Pikachu"
print(len(pikas), "PIKAS")

# len(pikas)
# output: 3  
print(pikas[0])
#output {'_id': ObjectId('5ed06599b4ccc30db96d9b88'), 'name': 'Pikachu', 'level': 30, 'exp': 76000000000, 'hp': 400, 'fav_ice_cream_flavors': ['vanilla_bean', 'choc'], 'stats': {'a': 1, 'b': 2, 'c': [1, 2, 3]}}
# a unique id is assigned to each Pickachu created in a memory.

print(pikas[0]['_id']) # prints the first _id
# output: ObjectId('5ed06599b4ccc30db96d9b88')
# pikas[1]['_id'] -  prints the seconds _id
# output: ObjectId('5ed065feb12bae77037decb8')
print(pikas[0]['name']) # we can also access "name" attribute
# output: 'Pikachu'

# collection.insert_one({"_id": "MY_ID", "name": "MY_NAME"})
# can ovewrite the "_id" and "name" attributes but cannot insert duplicate _id and name values

strong = list(collection.find({'lvl': {"$gte": 70}})) 
# $gte = Matches values that are greater than or equal to a specified value.

#TODO: also try to account for our mistakes "lvl" vs "level"
breakpoint()