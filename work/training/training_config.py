################################################################################
# General parameters
################################################################################

general_config = {
    "new_model": "mistralai-sti-instruct",
}

################################################################################
# QLoRA parameters
################################################################################

lora_config = {
    "lora_r": 64,
    "lora_alpha": 16,
    "lora_dropout": 0.1,
}

################################################################################
# bitsandbytes parameters
################################################################################

bitsandbytes = {
    "use_4bit": True,
    "bnb_4bit_compute_dtype": "float16",
    "bnb_4bit_quant_type": "nf4",
    "use_nested_quant": True,
}

################################################################################
# TrainingArguments parameters
################################################################################

training_args = {
    "output_dir": "./results_mixtral_sft",
    "evaluation_strategy": "epoch",
    "num_train_epochs": 20,
    "do_eval": True,
    "fp16": True,
    "bf16": False,
    "per_device_train_batch_size": 32,
    "per_device_eval_batch_size": 32,
    "gradient_checkpointing": True,
    "save_strategy": "epoch",
    "learning_rate": 2e-5,
    "optim": "paged_adamw_8bit",
    "lr_scheduler_type": "linear",
    "save_steps": 25,
    "log_level": "debug",
}