import os
import sys
from pymongo import MongoClient
from tqdm import tqdm
from collections import Counter
from literal_recognizer import LiteralRecognizer

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
columns_c = mongo_client[MONGO_DBNAME].columns
cell_set_c = mongo_client[MONGO_DBNAME].cell_set

all_columns = columns_c.find({})
total = columns_c.count_documents({})

if all_columns is None:
    sys.exit(0)

cell_set = set()
for column in tqdm(all_columns, total = total):
    tag = column['tag']
    if tag == 'NE':
        for cell in column['column']['data']:
            current_cell = cell['cell']
            if current_cell not in cell_set:
                cell_set.add(current_cell)
                cell_set_c.insert_one({"cell": current_cell, "status": "TODO"})