from peft import LoraConfig, prepare_model_for_kbit_training
from transformers import (
    AutoModelForCausalLM,
    BitsAndBytesConfig,
    TrainingArguments
)
from trl import SFTTrainer
from tokenizer import Tokenizer
from load_dataset import TabularData
from training_config import general_config, lora_config, bitsandbytes, training_args
from datasets_paths import relative_paths

print("DEFINE MODEL")
model_name = "mistralai/Mixtral-8x7B-Instruct-v0.1"

# import dataset
print("IMPORT TOKENIZER")
tokenizer = Tokenizer(model_name).get_tokenizer

# import dataset
print("IMPORT DATASET")
#base_path = "../datasets/output/"
#all_paths = [base_path + path for path in relative_paths]
all_paths = ["../datasets/output/pool/cea_pool_2.jsonl"]

tabular_data = TabularData(all_paths, tokenizer=tokenizer, max_tokens=550)
training_data, validation_data, test_data, test_results = tabular_data.load_tables()

print(len(training_data))
print(len(validation_data))
print(len(test_data))

print("BITS AND BYTES CONFIG")
# BitsAndBytesConfig
bnb_config = BitsAndBytesConfig(
   load_in_4bit=bitsandbytes["use_4bit"],
   bnb_4bit_quant_type=bitsandbytes["bnb_4bit_quant_type"],
   bnb_4bit_use_double_quant=bitsandbytes["use_nested_quant"],
   bnb_4bit_compute_dtype=bitsandbytes["bnb_4bit_compute_dtype"],
)

print("IMPORT MODEL")
# load model
model = AutoModelForCausalLM.from_pretrained("./mistralai-sti-instruct_Wikidata_Pretraining", quantization_config=bnb_config, device_map='auto', use_cache=False)
model = prepare_model_for_kbit_training(model)
model.config.pad_token_id = tokenizer.pad_token_id
model.config.use_cache = False

print("LORA CONFIG")
peft_config = LoraConfig(
        lora_alpha=lora_config["lora_alpha"],
        lora_dropout=lora_config["lora_dropout"],
        r=lora_config["lora_r"],
        bias="none",
        task_type="CAUSAL_LM",
        target_modules= [
                "q_proj",
                "k_proj",
                "v_proj",
                "o_proj",
                "gate_proj",
                "up_proj",
                "down_proj",
                "lm_head",
        ]
)

print("DEFINE TRAINING ARGUMENTS")
training_arguments = TrainingArguments(
        output_dir=training_args["output_dir"],
        evaluation_strategy=training_args["evaluation_strategy"],
        do_eval=training_args["do_eval"],
        optim=training_args["optim"],
        fp16=training_args["fp16"],
        bf16=training_args["bf16"],
        per_device_train_batch_size=training_args["per_device_train_batch_size"],
        per_device_eval_batch_size=training_args["per_device_eval_batch_size"],
        log_level=training_args["log_level"],
        save_strategy=training_args["save_strategy"],
        save_steps=training_args["save_steps"],
        num_train_epochs=training_args["num_train_epochs"],
        learning_rate=training_args["learning_rate"],
        lr_scheduler_type=training_args["lr_scheduler_type"],
)

trainer = SFTTrainer(
        model=model,
        train_dataset=training_data,
        eval_dataset=validation_data,
        peft_config=peft_config,
        dataset_text_field="prompt",
        max_seq_length=550,
        tokenizer=tokenizer,
        args=training_arguments,
)

trainer.train()

trainer.model.save_pretrained(general_config["new_model"])