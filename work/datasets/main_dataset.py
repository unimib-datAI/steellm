import json
from tqdm import tqdm
from process.dataset import Dataset
from process.dataset_cea import GroundTruthCEA
from process.dataset_cta import GroundTruthCTA
from process.dataset_cpa import GroundTruthCPA
from process.config import cea_datasets, cta_datasets, cpa_datasets

def to_jsonl(llm_dataset_list: list, datasetName: str):
    with open(datasetName, 'w') as outfile:
        for entry in llm_dataset_list:
            json.dump(entry, outfile)
            outfile.write('\n')

# CEA
print("GENERATING CEA DATASET ...")
for current_dataset in tqdm(cea_datasets):
    all_datasets_cea: list[dict[str, str]] = []
    print(current_dataset)
    print(f"{current_dataset['dataset']}... loading")
    gt_dataset = GroundTruthCEA(current_dataset['gt'])
    gt_dataset.load()

    dataset = Dataset(gt_dataset, current_dataset['dataset'], current_dataset['tables'], instruction="conduct the cell entity annotation (cea) task for wikidata on this table:")
    dataset.load_tables()
    len(dataset.llm_dataset)
    all_datasets_cea.extend(dataset.llm_dataset)

    print("Dataset length:", len(all_datasets_cea))
    print("Dataset output at 'output/cea/cea_all.jsonl'", len(all_datasets_cea))
    to_jsonl(all_datasets_cea, f'./output/cea/cea_{current_dataset["dataset"]}.jsonl')


# CTA
print("GENERATING CTA DATASET ...")
for current_dataset in tqdm(cta_datasets):
    all_datasets_cta: list[dict[str, str]] = []
    print(f"{current_dataset['dataset']}...")
    gt_dataset = GroundTruthCTA(current_dataset['gt'])
    gt_dataset.load()

    dataset = Dataset(gt_dataset, current_dataset['dataset'], current_dataset['tables'], instruction="conduct the column type annotation (cta) task for wikidata on this table:")
    dataset.load_tables()
    len(dataset.llm_dataset)
    all_datasets_cta.extend(dataset.llm_dataset)

    print("Dataset length:", len(all_datasets_cta))
    print("Dataset output at 'output/cta/cta_all.jsonl'", len(all_datasets_cta))
    to_jsonl(all_datasets_cta, f"output/cta/cta_{current_dataset['dataset']}.jsonl")


# CPA
print("GENERATING CPA DATASET ...")
for current_dataset in tqdm(cpa_datasets):
    all_datasets_cpa: list[dict[str, str]] = []
    print(f"{current_dataset['dataset']}...")
    gt_dataset = GroundTruthCPA(current_dataset['gt'])
    gt_dataset.load()

    dataset = Dataset(gt_dataset, current_dataset['dataset'], current_dataset['tables'], instruction="conduct the column property annotation (cpa) task for wikidata on this table:")
    dataset.load_tables()
    len(dataset.llm_dataset)
    all_datasets_cpa.extend(dataset.llm_dataset)

    print("Dataset length:", len(all_datasets_cpa))
    print("Dataset output at 'output/cpa/cpa_all.jsonl'", len(all_datasets_cpa))
    to_jsonl(all_datasets_cpa, f"output/cpa/cpa_{current_dataset['dataset']}.jsonl")
