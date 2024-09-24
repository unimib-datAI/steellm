from transformers import AutoTokenizer

model_name = "mistralai/Mixtral-8x7B-v0.1"

class Tokenizer:

    def __init__(self, model_name):
        self.model_name = model_name
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, add_eos_token=True, use_fast=True)
        self.tokenizer.pad_token = self.tokenizer.eos_token
        self.tokenizer.pad_token_id =  self.tokenizer.eos_token_id
        self.tokenizer.padding_side = 'right'

    @property
    def get_tokenizer(self):
        return self.tokenizer