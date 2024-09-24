from pymongo import MongoClient

import os

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

def get_collection(collection_name):
    return mongo_client[MONGO_DBNAME][collection_name]

def index_field(collection_name, field_name):
    mongo_client[MONGO_DBNAME][collection_name].create_index([ (field_name, 1) ], unique=True)

def get_entity(collection_name, entity):
    return mongo_client[MONGO_DBNAME][collection_name].find_one({ 'entity': entity })