#### LOAD TABLES
import csv
import os
from tqdm import tqdm
from pymongo import MongoClient

# UTILS
def clean_str(s):
    s = s.lower()
    return " ".join(s.split())

DEBUG=False
SAMPLE_DEBUG = 3

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

cea_datasets: list[dict[str, str]] = [
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round1_gt.csv", "tables": "./../../wikidata/SemTab2020_Table_GT_Target/Round1/tables", "dataset": "semtab_2020_r1"}, # SemTab2020_Table_GT_Target R1
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round2_gt.csv", "tables": "./../../wikidata/SemTab2020_Table_GT_Target/Round2/tables", "dataset": "semtab_2020_r2"}, # SemTab2020_Table_GT_Target R2
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round3_gt.csv", "tables": "./../../wikidata/SemTab2020_Table_GT_Target/Round3/tables", "dataset": "semtab_2020_r3"}, # SemTab2020_Table_GT_Target R3
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round4_gt.csv", "tables": "./../../wikidata/SemTab2020_Table_GT_Target/Round4/tables", "dataset": "semtab_2020_r4"}, # SemTab2020_Table_GT_Target R4
    {"gt": "./wikidata/HardTablesR1/DataSets/HardTablesR1/Valid/gt/cea_gt.csv", "tables": "./../../wikidata/HardTablesR1/DataSets/HardTablesR1/Valid/tables", "dataset": "hardtables_2022_r1"}, # HardTablesR1 2022
    {"gt": "./wikidata/HardTablesR2/DataSets/HardTablesR2/Valid/gt/cea_gt.csv", "tables": "./../../wikidata/HardTablesR2/DataSets/HardTablesR2/Valid/tables", "dataset": "hardtables_2022_r2"}, # HardTablesR2 2022
    {"gt": "./wikidata/WikidataTables2023R1/DataSets/Valid/gt/cea_gt.csv", "tables": "./../../wikidata/WikidataTables2023R1/DataSets/Valid/tables", "dataset": "wikidata_tables_2023"}, # Wikidata Tables 2023
]

i = 0

for path in tqdm(cea_datasets):
    for table in tqdm(os.listdir(path['tables'])):
        if table.startswith('.'):
            continue
        id_table = table[:-4]
        buffer = []

        with open(f"{path['tables']}/{table}", 'r') as file:
            reader = csv.reader(file)
            # skip the header
            header = next(reader, None)
            for row in reader:
                item = {h:'' for h in header}
                for j, cell in enumerate(row):
                    item[header[j]] = clean_str(cell)
                buffer.append(item)  
        table_c.insert_one({ "id": i, "idDataset": 5, "name": id_table, "data": buffer, "nCols": len(header), "nRows": len(buffer), "status": "TODO"})
        i += 1
        
        if DEBUG and i == SAMPLE_DEBUG:
            break
        
col = mongo_client[MONGO_DBNAME].tables
resp = col.create_index([ ("name", 1) ], unique=True)