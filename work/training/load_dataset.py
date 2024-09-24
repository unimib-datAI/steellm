import json
from tqdm import tqdm
from typing import List
from datasets import Dataset

class TabularData:

    def __init__(self, paths: List, tokenizer, max_tokens, test_size = 0.05, validation_size = 0.05):
        self.paths = paths
        self.validation_size = validation_size
        self.test_size = test_size
        self.tokenizer = tokenizer
        self.max_tokens = max_tokens

    def get_current_size(self, dataset):
        count = 0
        with open(dataset) as f:
            for _ in f:
                count += 1
        return count
    
    def to_jsonl(self, dataset, path):
        with open(path, "w") as f:
            for data in dataset:
                f.write(json.dumps(data) + "\n")

    def find_all_entities(self):
        all_entities_set: set = set()
        for path in tqdm(self.paths):
            with open(path) as f:
                for _, data in enumerate(f):
                    table = json.loads(data)
                    prompt = f"<s>[INST] {table['instruction']} {table['input']}. {table['pool_instruction']} {table['pool']}[/INST] {table['output']}</s>"
                    encoded_input = self.tokenizer(prompt, add_special_tokens=False)
                    if len(encoded_input["input_ids"]) < self.max_tokens:
                        for output_table in table["output"].split(";"):
                            try:
                                all_entities_set.add(output_table.split("=")[1].split(" ")[0])
                            except:
                                print(output_table)
        return all_entities_set
    
    def load_wikidata_training(self, path):
        training_data = []
        with open(path) as f:
            for data in f:
                prompt_wikidata = json.loads(data)
                training_data.append(prompt_wikidata["prompt"])
        
        return Dataset.from_dict({"prompt": training_data})

    def load_tables(self):
        training_data = []
        validation_data = []
        test_data = []
        test_results = []
        train_tables = []; val_tables = []; test_tables = []
        train_dataset = []; val_dataset = []; test_dataset = []
        for path in tqdm(self.paths):
            current_size = self.get_current_size(path)
            validation_size = int(current_size * self.validation_size)
            test_size = int(current_size * self.test_size)
            with open(path) as f:
                for _, data in enumerate(f):
                    table = json.loads(data)
                    prompt = f"<s>[INST] {table['instruction']} {table['input']}. {table['pool_instruction']} {table['pool']}[/INST] {table['output']}</s>"
                    encoded_input = self.tokenizer(prompt, add_special_tokens=False)
                    if len(encoded_input["input_ids"]) < self.max_tokens:
                        if len(validation_data) < validation_size:
                            validation_data.append(f"<s>[INST] {table['instruction']} {table['input']} [/INST] {table['output']}</s>")
                            val_tables.append(table["table"])
                            val_dataset.append(table["dataset"])
                        elif len(test_data) < test_size:
                            test_data.append(f"<s>[INST] {table['instruction']} {table['input']} [/INST]")
                            test_results.append({table['output']})
                            test_tables.append(table["table"])
                            test_dataset.append(table["dataset"])
                        else:
                            train_tables.append(table["table"])
                            train_dataset.append(table["dataset"])
                            training_data.append(f"<s>[INST] {table['instruction']} {table['input']} [/INST] {table['output']}</s>")

        return (
            Dataset.from_dict({"table": train_tables, "dataset": train_dataset, "prompt": training_data}),
            Dataset.from_dict({"table": val_tables, "dataset": val_dataset, "prompt": validation_data}),
            Dataset.from_dict({"table": test_tables, "dataset": test_dataset, "prompt": test_data}),
            Dataset.from_dict({"table": test_tables, "dataset": test_dataset, "prompt": test_results}),
        )