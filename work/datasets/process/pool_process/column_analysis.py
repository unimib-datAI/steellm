# CREATE COLUMNS COLLECTION
import os
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
table_c = mongo_client[MONGO_DBNAME].tables
columns = mongo_client[MONGO_DBNAME].columns

# UTILS
def clean_str(s):
    s = s.lower()
    return " ".join(s.split())

mappings_regex = {
    'DATE': {'datatype':'DATETIME', 'tag': 'LIT'},
    'DATETIME': {'datatype':'DATETIME', 'tag': 'LIT'},
    'TIME': {'datatype':'DATETIME', 'tag': 'LIT'},
    'URL': {'datatype':'STRING', 'tag': 'LIT'},
    'EMAIL': {'datatype':'STRING', 'tag': 'LIT'},
    'INTEGER': {'datatype':'NUMBER', 'tag': 'LIT'},
    'FLOAT': {'datatype':'NUMBER', 'tag': 'LIT'},
    'ENTITY': {'datatype': 'ENTITY', 'tag': 'NE'},
}

tables = table_c.find({})
total = table_c.count_documents({})
lr = LiteralRecognizer()
index = 0
for table in tqdm(tables, total = total):
    number_of_rows = table["nRows"]
    table_name = table["name"]
    data = table["data"]
    column_names = data[0].keys()
    for index, col in enumerate(column_names):
        column = {'data': []}
        column_eval = []
        for row in range(0, number_of_rows):
            cell = clean_str(data[row][col])
            column['data'].append({'cell': cell})
            column_eval.append(lr.check_literal(cell))
        column_regex_eval = Counter(column_eval).most_common(1)[0][0]
        columns.insert_one({
            'id': index,
            'tag': mappings_regex[column_regex_eval]['tag'],
            'datatype': mappings_regex[column_regex_eval]['datatype'],
            'col':col,
            'index_col': index + 1,
            'table_name': table_name,
            'column': column,
            "status": "TODO"
        })
        index += 1