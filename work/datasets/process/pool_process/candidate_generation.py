import os
import sys
import time
from pymongo import MongoClient
from tqdm import tqdm
from collections import Counter
from literal_recognizer import LiteralRecognizer

import requests

headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
}

def lookup(cell):
    current_lookup_result = {}
    params = {
        'token': "insideslab-lamapi-2022",
        'name': cell,
        'kg': "wikidata",
        'limit': 10
    }
    result = requests.get("https://lamapi.inside.disco.unimib.it/lookup/entity-retrieval", headers=headers, params=params).json()
    for key, value in result.items():
        current_lookup_result = {"name": key, "entities": []}
        for entity in value:
            current_lookup_result["entities"].append({'id': entity['id'], 'label': entity['name']})
    return current_lookup_result

MONGO_ENDPOINT = os.environ['MONGO_ENDPOINT']
MONGO_PORT = os.environ['MONGO_PORT']
MONGO_ENDPOINT_USERNAME = os.environ['MONGO_INITDB_ROOT_USERNAME']
MONGO_ENDPOINT_PASSWORD = os.environ['MONGO_INITDB_ROOT_PASSWORD']
MONGO_DBNAME = os.environ['MONGO_DBNAME']
mongo_client = MongoClient(
                            MONGO_ENDPOINT, 
                            int(MONGO_PORT), 
                            username=MONGO_ENDPOINT_USERNAME, 
                            password=MONGO_ENDPOINT_PASSWORD, 
                            authSource='admin'
                        )
candidate_c = mongo_client[MONGO_DBNAME].candidate
cell_set_c = mongo_client[MONGO_DBNAME].cell_set
log_c = mongo_client[MONGO_DBNAME].log
missing_c = mongo_client[MONGO_DBNAME].missing

data = cell_set_c.find_one_and_update({"status": "TODO"}, {"$set": {"status": "DOING"}})

if data is None:
    sys.exit()

cell = data["cell"]
_id =  data["_id"]

attempts = 0
fail = True
while fail and attempts < 5:
    try:
        result = lookup(cell)
        candidate_c.insert_one({
            "cell": cell,
            "candidates": result
        })
        cell_set_c.update_one({"_id": _id}, {"$set": {"status": "DONE"}})  
        fail = False
    except Exception as e:
        attempts += 1
        fail = True
        print(e)
        log_c.insert_one({
            "cell": cell,
            "attempts": attempts
        })
        
        if attempts == 5:
            missing_c.insert_one({
                "cell": cell
            })
            continue
            
        time.sleep(attempts*30)