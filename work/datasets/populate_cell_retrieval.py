import csv
import requests
from tqdm import tqdm
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

def insert_many_entity(collection_name, entities):
    mongo_client[MONGO_DBNAME][collection_name].insert_many(entities)

cea_datasets: list[dict[str, str]] = [
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round1_gt.csv", "tables": "./wikidata/SemTab2020_Table_GT_Target/Round1/tables", "dataset": "semtab_2020_r1"}, # SemTab2020_Table_GT_Target R1
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round2_gt.csv", "tables": "./wikidata/SemTab2020_Table_GT_Target/Round2/tables", "dataset": "semtab_2020_r2"}, # SemTab2020_Table_GT_Target R2
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round3_gt.csv", "tables": "./wikidata/SemTab2020_Table_GT_Target/Round3/tables", "dataset": "semtab_2020_r3"}, # SemTab2020_Table_GT_Target R3
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round4_gt.csv", "tables": "./wikidata/SemTab2020_Table_GT_Target/Round4/tables", "dataset": "semtab_2020_r4"}, # SemTab2020_Table_GT_Target R4
    {"gt": "./wikidata/HardTablesR1/DataSets/HardTablesR1/Valid/gt/cea_gt.csv", "tables": "./wikidata/HardTablesR1/DataSets/HardTablesR1/Valid/tables", "dataset": "hardtables_2022_r1"}, # HardTablesR1 2022
    {"gt": "./wikidata/HardTablesR2/DataSets/HardTablesR2/Valid/gt/cea_gt.csv", "tables": "./wikidata/HardTablesR2/DataSets/HardTablesR2/Valid/tables", "dataset": "hardtables_2022_r2"}, # HardTablesR2 2022
    {"gt": "./wikidata/WikidataTables2023R1/DataSets/Valid/gt/cea_gt.csv", "tables": "./wikidata/WikidataTables2023R1/DataSets/Valid/tables", "dataset": "wikidata_tables_2023"}, # Wikidata Tables 2023
]

headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
}

def column_analysis(columns):
    params = {
        'token': "insideslab-lamapi-2022",
    }
    result = requests.post("https://lamapi.inside.disco.unimib.it/sti/column-analysis", headers=headers, params=params, json={"json": columns}).json()
    return result

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

cell_set = set()
cell_buffer = []
for dataset in cea_datasets:
    all_csv_tables = os.listdir(dataset['tables'])
    for csv_table in tqdm(all_csv_tables, total=len(all_csv_tables)):
            table_name: str = csv_table.split('.')[0]
            with open(os.path.join(dataset['tables'], csv_table), 'r') as f:
                reader = csv.reader(f)
                next(reader, None)
                columns = {}
                for row in reader:
                    for index, cell in enumerate(row):
                        if index not in columns:
                            columns[index] = [cell]
                        else:
                            columns[index].append(cell)
                columns_to_annotate = []
                for _, column in columns.items():
                    columns_to_annotate.append(column)
                annotated_columns = column_analysis(columns_to_annotate)
                for key, val in annotated_columns.items():
                    if val['tag'] == "NE":
                        for cell in val['column_rows']:
                            if cell.lower() not in cell_set:
                                cell_set.add(cell.lower())
                                result = lookup(cell.lower())
                                cell_buffer.append({"name": cell.lower(), "entities": result})
                if len(cell_buffer) > 500:
                    insert_many_entity("cell_retrieval", list(cell_buffer))
                    cell_buffer = []

if len(cell_buffer) > 0:
    insert_many_entity("cell_retrieval", list(cell_buffer))
    cell_buffer = []