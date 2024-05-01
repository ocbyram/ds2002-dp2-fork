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

import os

directory = '/workspace/ds2002-dp2-fork/data/'


for filename in os.listdir(directory):
    with open(os.path.join(directory, filename)) as f:
        try:
            file_data = json.load(f)
            if isinstance(file_data, list):
                try:
                    collection.insert_many(file_data)
                except:
                    pass
            else:
                try:
                    collection.insert_one(file_data)
                except:
                    pass
        except:
            print(filename, 'could not be imported. Check JSON for corruption.')

print(collection.count_documents({}), 'documents were successfully imported into my collection.')

print('Closer look at generated39.json - there are six documents in this JSON, 1 is corrupted.')

print('The corrupted file keeps the other five from being imported, despite all five being complete.')

