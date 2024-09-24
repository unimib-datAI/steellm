import os
import csv
from tqdm import tqdm
from wrappers.mongodb import get_collection
from wrappers.lamapi import LamAPI
from process.config import cea_datasets, cta_datasets, cpa_datasets

LAMAPI_HOST = os.environ.get('LAMAPI_ENDPOINT', 'localhost')
LAMAPI_PORT = os.environ.get('LAMAPI_PORT', 8080)
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]

lamapi = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)

def insertEntities(entities):
    collection = get_collection("entities")
    collection.insert_many(entities)

wikidata_entities = set()
# CEA
for dataset in tqdm(cea_datasets):
    print(f'{dataset["dataset"]} ...')
    with open(dataset["gt"], "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in tqdm(reader):
            if row[3] != "UNKNOWN" and row[3] != "NIL":
                wikidata_entities.add(row[3].replace("http://www.wikidata.org/entity/", ""))
# CTA
for dataset in tqdm(cta_datasets):
    print(f'{dataset["dataset"]} ...')
    with open(dataset["gt"], "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in tqdm(reader):
            if row[2] != "UNKNOWN" and row[2] != "NIL":
                wikidata_entities.add(row[2].replace("http://www.wikidata.org/entity/", ""))
# CPA
for dataset in tqdm(cpa_datasets):
    print(f'{dataset["dataset"]} ...')
    with open(dataset["gt"], "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in tqdm(reader):
            if row[3] != "UNKNOWN" and row[3] != "NIL":
                wikidata_entities.add(row[3].replace("http://www.wikidata.org/prop/direct/", ""))

print("Wikidata entities: ", len(wikidata_entities))

total = 0
buffer = []; insert_buffer = []
for entity in tqdm(wikidata_entities):
    buffer.append(entity)
    if len(buffer) == 1000:
        result = lamapi.labels(buffer)
        for entity in result:
            if 'labels' in result[entity]:
                if 'en' in result[entity]['labels']:
                    insert_buffer.append(({ 'entity': entity, 'label': result[entity]['labels']['en'] }))
        insertEntities(insert_buffer)
        total += len(insert_buffer)
        insert_buffer = []
        buffer = []

if len(buffer) > 0:
    result = lamapi.labels(buffer)
    for entity in result:
        if 'labels' in result[entity]:
            if 'en' in result[entity]['labels']:
                insert_buffer.append(({ 'entity': entity, 'label': result[entity]['labels']['en'] }))
    insertEntities(insert_buffer)
    total += len(insert_buffer)
    insert_buffer = []
    buffer = []

print("Data inserted!")
print("Total: ", total)