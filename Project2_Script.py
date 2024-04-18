#!/usr/bin/env python3

from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)

db = client.project2db

collection = db.ProjectCollection


# assuming you have defined a connection to your db and collection already:



# so basically I need to go through all of the json files and say for each of these files open them 
# and import them into the collection
for (root, dirs, file) in os.walk('/workspace/ds2002-dp2-fork/data/'):
    for f in file:
        with open('generated01.json') as file:
            file_data = json.load(file)
            if isinstance(file_data, list):
                collection.insert_many(file_data)  
            else:
                collection.insert_one(file_data)