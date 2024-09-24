import os
import csv
import json
import random
from tqdm import tqdm
from pymongo import MongoClient
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

def get_collection(collection_name):
    return mongo_client[MONGO_DBNAME][collection_name]

def get_entity(collection_name, entity):
    return mongo_client[MONGO_DBNAME][collection_name].find_one({ 'entity': entity })

def get_cell_retrieval(collection_name, cell):
    return mongo_client[MONGO_DBNAME][collection_name].find_one({"cell": cell})

class GroundTruthItemCEA:
    def __init__(self, table, row, column, value):
        self.table: str = table
        self.row: str = row
        self.column: str = column
        self.value: str = value

    def get_item(self) -> dict[str, str]:
        return {
            'table': self.table,
            'row': self.row,
            'column': self.column,
        }
    
    @property
    def get_identifier(self) -> str:
        return f'{self.row}_{self.column}'
    
    @property
    def get_output(self) -> str:
        return f'({self.row},{self.column})={self.value}'

    def __str__(self) -> str:
        return f'{self.table} {self.row} {self.column} {self.value}'
    
class GroundTruthCEA:
    def __init__(self, filename):
        self.filename: str = filename
        self.ground_truth: dict[str, dict[str, GroundTruthItemCEA]] = {}

    @property
    def total(self) -> int:
        return len(self.ground_truth)
    
    def number_of_items_in_csv(self) -> int:
        with open(self.filename, 'r') as f:
            reader = csv.reader(f)
            total: int = 0
            for _ in reader:
                total += 1
            return total

    def load(self):
        with open(self.filename, 'r') as f:
            print('Loading ground truth...')
            total_lines: int = self.number_of_items_in_csv()
            print(f'Total lines: {total_lines}')
            reader = csv.reader(f)
            for row in tqdm(reader, total=total_lines):
                current_gt: GroundTruthItemCEA = GroundTruthItemCEA(row[0], row[1], row[2], row[3])
                if current_gt.table in self.ground_truth:
                    self.ground_truth[current_gt.table][current_gt.get_identifier] = current_gt
                else:
                    self.ground_truth[current_gt.table] = {
                        current_gt.get_identifier: current_gt
                    }

pool_cache = set()
class Dataset():

    def __init__(self, gt_dataset: GroundTruthCEA, dataset_name: str, filename: str):
        self.filename: str = filename
        self.dataset_name: str = dataset_name
        self.gt_dataset = gt_dataset
        self.llm_dataset: list[dict[str, str]] = []
        self.instruction = "perform the cell entity annotation (cea) task on this table where each row is separated by '|' and each cell is separated by ';' and the first row in position 0 is the header of the table:"
        self.literal_recognizer = LiteralRecognizer()
    
    def get_output(self, table_name: str, pool: set) -> str | None:
        outputCEA: str = ""
        if table_name not in self.gt_dataset.ground_truth:
            return None
        for _, gtTable in self.gt_dataset.ground_truth[table_name].items():
            if gtTable.value != "UNKNOWN":
                current_entity_label = get_entity("entities", gtTable.value.replace('http://www.wikidata.org/entity/', ''))
                if current_entity_label is not None and 'label' in current_entity_label:
                    outputCEA += gtTable.get_output.replace('http://www.wikidata.org/entity/', '') + f" [{current_entity_label['label'].lower()}]" + "|"
                    # add correct entity to pool
                    pool.add(f"{gtTable.value.replace('http://www.wikidata.org/entity/', '')} [{current_entity_label['label'].lower()}]")
                else:
                    outputCEA += gtTable.get_output.replace('http://www.wikidata.org/entity/', '') + "|"
            else:
                return None
        
        return outputCEA, pool
    
    def get_pool(self, filename: str, csv_table: str) -> list[dict[str, str]]:
        pool = set()
        with open(os.path.join(filename, csv_table), 'r') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    for cell in row:
                        if self.literal_recognizer.check_literal(cell) == "ENTITY":
                            cell_retrieval = get_cell_retrieval("candidate", cell.lower())
                            if cell_retrieval is not None:
                                for entity in cell_retrieval['candidates']['entities'][0:3]:
                                    pool.add(f"{entity['id']} [{entity['label'].lower()}]")
                                    pool_cache.add(f"{entity['id']} [{entity['label'].lower()}]")
                            else:
                                # pescare qualche candidato a caso
                                random_entities = random.sample(list(pool_cache), 3)
                                pool.update(random_entities)
        return pool
    
    def format_pool(self, pool: set) -> str:
        string_pool = ""
        for entity in pool:
            string_pool += entity + ";"
        return string_pool
    
    def load_tables(self):
        all_csv_tables = os.listdir(self.filename)
        print("LOAD TABLES...")
        for csv_table in tqdm(all_csv_tables, total=len(all_csv_tables)):
            table_name: str = csv_table.split('.')[0]
            table_representation: str = ""
            with open(os.path.join(self.filename, csv_table), 'r') as f:
                reader = csv.reader(f)
                #next(reader, None)
                for row in reader:
                    table_representation += ";".join(row) + "|"
                table_representation = table_representation[:-1]
                pool = self.get_pool(self.filename, csv_table)
                current_output, pool_updated = self.get_output(table_name, pool)
                if current_output is None:
                    continue
                self.llm_dataset.append({
                    'dataset': self.dataset_name,
                    'table': table_name,
                    'instruction': self.instruction,
                    'input': table_representation,
                    "output": current_output[:-1],
                    "pool_instruction": "use the following pool of entities to annotate the table:",
                    "pool": self.format_pool(pool_updated)
                })

cea_datasets: list[dict[str, str]] = [
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round1_gt.csv", "tables": "./wikidata/SemTab2020_Table_GT_Target/Round1/tables", "dataset": "semtab_2020_r1"}, # SemTab2020_Table_GT_Target R1
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round2_gt.csv", "tables": "./wikidata/SemTab2020_Table_GT_Target/Round2/tables", "dataset": "semtab_2020_r2"}, # SemTab2020_Table_GT_Target R2
    {"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round3_gt.csv", "tables": "./wikidata/SemTab2020_Table_GT_Target/Round3/tables", "dataset": "semtab_2020_r3"}, # SemTab2020_Table_GT_Target R3
    #{"gt": "./wikidata/SemTab2020_Table_GT_Target/GT/CEA/CEA_Round4_gt.csv", "tables": "./wikidata/SemTab2020_Table_GT_Target/Round4/tables", "dataset": "semtab_2020_r4"}, # SemTab2020_Table_GT_Target R4
    {"gt": "./wikidata/HardTablesR1/DataSets/HardTablesR1/Valid/gt/cea_gt.csv", "tables": "./wikidata/HardTablesR1/DataSets/HardTablesR1/Valid/tables", "dataset": "hardtables_2022_r1"}, # HardTablesR1 2022
    #{"gt": "./wikidata/HardTablesR2/DataSets/HardTablesR2/Valid/gt/cea_gt.csv", "tables": "./wikidata/HardTablesR2/DataSets/HardTablesR2/Valid/tables", "dataset": "hardtables_2022_r2"}, # HardTablesR2 2022
    {"gt": "./wikidata/WikidataTables2023R1/DataSets/Valid/gt/cea_gt.csv", "tables": "./wikidata/WikidataTables2023R1/DataSets/Valid/tables", "dataset": "wikidata_tables_2023"}, # Wikidata Tables 2023
]

all_datasets: list[dict[str, str]] = []
for dataset in tqdm(cea_datasets):
    print(f"{dataset['dataset']}...")
    gt_dataset = GroundTruthCEA(dataset['gt'])
    gt_dataset.load()

    dataset = Dataset(gt_dataset, dataset['dataset'], dataset['tables'])
    dataset.load_tables()
    all_datasets.extend(dataset.llm_dataset)

def to_jsonl(llm_dataset_list: list, datasetName: str):
    with open(datasetName, 'w') as outfile:
        for entry in llm_dataset_list:
            json.dump(entry, outfile)
            outfile.write('\n')

to_jsonl(all_datasets, './output/pool/cea_pool_2.jsonl')

print("DONE!")