import re
import os
import csv
from dataclasses import dataclass
from tqdm import tqdm
from typing import Dict
from wrappers.mongodb import get_entity

@dataclass
class GroundTruthItemInterface:

    def __init__(self, table):
        self.table: str = table

    def __str__(self) -> str:
        pass
    
    @property
    def get_item(self) -> Dict[str, str]:
        pass
    
    @property
    def get_identifier(self) -> str:
        pass
    
    @property
    def get_output(self) -> str:
        pass

class GroundTruthAbstract:

    def __init__(self, filename):
        self.filename: str = filename
        self.ground_truth: dict[str, dict[str, GroundTruthItemInterface]] = {}

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
        pass

class Dataset():

    def __init__(self, gt_dataset: GroundTruthAbstract, dataset_name: str, filename: str, instruction: str = ""):
        self.filename: str = filename
        self.dataset_name: str = dataset_name
        self.gt_dataset = gt_dataset
        self.llm_dataset: list[dict[str, str]] = []
        self.instruction = instruction

    def get_output(self, table_name: str) -> str:
        output: str = ""
        if table_name not in self.gt_dataset.ground_truth:
            return None
        for index, (_, gtTable) in enumerate(self.gt_dataset.ground_truth[table_name].items()):
            if gtTable.value != "UNKNOWN":
                current_entity = gtTable.get_output.replace('http://www.wikidata.org/entity/', '').replace('http://www.wikidata.org/prop/direct/', '').replace('https://www.wikidata.org/wiki/', '').replace('https://www.wikidata.org/wiki/Property:', '')
                current_entity_id = current_entity.split(" ")[0].split("=")[1]
                current_entity_label = get_entity("entities", current_entity_id)
                if current_entity_label is None:
                    current_entity = f"{current_entity}"
                else:
                    current_entity = f"{current_entity} [{current_entity_label['label']}]"
                if index == len(self.gt_dataset.ground_truth[table_name]) - 1:
                    output += current_entity
                else:
                    output += current_entity + ";"
            else:
                return None

        return output
    
    def clean_str(self, s: str) -> str:
        s = s.lower()
        return " ".join(s.split())

    def load_tables(self):
        all_csv_tables = os.listdir(self.filename)
        for csv_table in tqdm(all_csv_tables, total=len(all_csv_tables)):
            table_name: str = csv_table.split('.')[0]
            table_representation: str = ""
            with open(os.path.join(self.filename, csv_table), 'r') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    table_representation += ";".join(row) + "|"
                current_output = self.get_output(table_name)
                if current_output is None:
                    continue
                table_encode = table_representation.encode("ascii", "ignore")
                table_decode = table_encode.decode()
                self.llm_dataset.append({
                    'dataset': self.dataset_name,
                    'table': table_name,
                    'instruction': self.instruction,
                    'input': re.sub(' +', ' ', table_decode.strip())[:-1],
                    "output": current_output
                })