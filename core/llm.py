import logging

import streamlit as st
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from config.settings import MODEL_NAME, MAX_INPUT_TOKENS, GENERATION_CONFIG

logger = logging.getLogger(__name__)


@st.cache_resource
def load_model(model_name: str = MODEL_NAME):
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info("Loading model %s on %s", model_name, device)

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype=torch.float16 if device == "cuda" else torch.float32,
    ).to(device)
    model.eval()

    return tokenizer, model, device


def run_inference(prompt: str, task: str = "summarization") -> str:
    if task not in GENERATION_CONFIG:
        raise ValueError(f"Unknown task '{task}'. Valid tasks: {list(GENERATION_CONFIG.keys())}")

    tokenizer, model, device = load_model()
    gen_config = GENERATION_CONFIG[task]

    inputs = tokenizer(
        prompt,
        return_tensors="pt",
        truncation=True,
        max_length=MAX_INPUT_TOKENS,
    ).to(device)

    input_len = inputs["input_ids"].shape[-1]

    with torch.no_grad():
        output = model.generate(**inputs, **gen_config)

    generated_tokens = output[0][input_len:]
    decoded = tokenizer.decode(generated_tokens, skip_special_tokens=True).strip()
    return decoded