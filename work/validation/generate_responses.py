import json
import os
from pymongo import MongoClient
from transformers import (
    AutoModelForCausalLM,
    BitsAndBytesConfig,
    BitsAndBytesConfig,
    AutoTokenizer,
)
import json
from tqdm import tqdm

model_name = "mistralai/Mixtral-8x7B-Instruct-v0.1"

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
labels_c = mongo_client[MONGO_DBNAME].wikidata_qids_labels

# BitsAndBytesConfig
bnb_config = BitsAndBytesConfig(
   load_in_4bit=True,
   bnb_4bit_quant_type="nf4",
   bnb_4bit_use_double_quant=True,
   bnb_4bit_compute_dtype="float16"
)

batch_size = 32
model_path = "./../training/wikidata_training/mistralai-sti-instruct_QIDs&Labels_2"
path = "./../training/datasets_new/QID&Labels/test_data.jsonl"

prompts = []
with open(path) as f:
    for index, data in enumerate(f):
        loaded_data = json.loads(data)
        prompts.append(loaded_data)

trained_model = AutoModelForCausalLM.from_pretrained(model_path, quantization_config=bnb_config, device_map='auto', use_cache=False)
tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
tokenizer.pad_token = tokenizer.eos_token

batch = []; batch_prompts = []
for prompt in tqdm(prompts):
    batch.append(prompt)
    batch_prompts.append(f"{prompt['prompt']}")
    if len(batch) == batch_size:
        encoded_input = tokenizer(batch_prompts, return_tensors="pt", padding=True, add_special_tokens=False)
        model_inputs = encoded_input.to('cuda')
        generated_ids = trained_model.generate(**model_inputs,
                                    max_new_tokens=256,
                                    pad_token_id=tokenizer.eos_token_id)
        decoded_output = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)
        for index, prompt in enumerate(batch):
            labels_c.insert_one({
                "table": prompt['table'],
                "dataset": prompt['dataset'],
                "prompt": prompt['prompt'],
                "response": decoded_output[index]
            })
        batch = []
        batch_prompts = []

if len(batch) > 0:
    prompts = prompts_input = [f"{p['prompt']}" for p in batch]
    encoded_input = tokenizer(prompts, return_tensors="pt", padding=True, add_special_tokens=False)
    model_inputs = encoded_input.to('cuda')
    generated_ids = trained_model.generate(**model_inputs,
                                max_new_tokens=256,
                                pad_token_id=tokenizer.eos_token_id)
    decoded_output = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)
    for index, prompt in enumerate(batch):
        labels_c.insert_one({
            "table": prompt['table'],
            "dataset": prompt['dataset'],
            "prompt": prompt['prompt'],
            "response": decoded_output[index]
        })
    batch = []
    batch_prompts = []