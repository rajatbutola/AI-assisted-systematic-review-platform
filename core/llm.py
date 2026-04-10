#core/llm.py 25th March v1, 2026
# core/llm.py  — v3  31st March
#
# ROOT CAUSE FIXES:
#
# FIX 1 — Transformers backend returns prompt + completion (return_full_text=True default)
#   The HuggingFace text-generation pipeline includes the input prompt in its
#   output by default. When displayed, the user sees the entire system prompt
#   ("You are an expert systematic review methodologist...") followed by the
#   actual summary. This was the cause of the "prompt leaking into summary" bug.
#   Fix: pass return_full_text=False so the pipeline returns ONLY the generated
#   text, not the prompt.
#
# FIX 2 — Extraction task prepended "{" causing double-brace JSON
#   The original code had:
#       if task == "extraction":
#           text = "{" + text   # because prompt used to end with "{"
#   The updated prompts (config/prompts.py) end with "JSON:" not "JSON:\n{".
#   So the model starts its response with "{", and prepending another "{"
#   creates "{{..." which is invalid JSON that json.loads() rejects.
#   Fix: remove the prepend entirely. The JSON parser already searches for
#   the first "{" in the text, so it handles any leading whitespace cleanly.
#
# FIX 3 — "scoring" task max_tokens=200 was set in GENERATION_CONFIG
#   200 tokens is enough for a 7-field JSON object (~150 tokens). This is fine.
#   But the stop tokens for "scoring" were ["\n\n"] which terminated the
#   response after the first line of JSON. Fix: use no stop tokens for
#   scoring/extraction so the full JSON object is generated.
#
# FIX 4 — Llama-cpp echo=False is correct, but transformers needs explicit handling
#   Both backends now strip any accidental prompt echo from the output.

import logging
import os
import streamlit as st

from config.settings import (
    MODEL_PATH, N_CTX, N_THREADS, N_GPU_LAYERS,
    MAX_INPUT_TOKENS, GENERATION_CONFIG, LLM_BACKEND,
    OLLAMA_BASE_URL, OLLAMA_MODEL,
)

logger = logging.getLogger(__name__)


# ── Model loader ──────────────────────────────────────────────────────────────

@st.cache_resource
def load_model():
    backend = LLM_BACKEND.lower()
    logger.info("Loading LLM backend: %s", backend)

    if backend == "gguf":
        # llama-cpp-python — local GGUF file
        try:
            from llama_cpp import Llama
        except ImportError:
            raise ImportError(
                "llama-cpp-python not installed. "
                "Run: pip install llama-cpp-python"
            )
        llm = Llama(
            model_path=MODEL_PATH,
            n_ctx=N_CTX,
            n_threads=N_THREADS,
            n_gpu_layers=N_GPU_LAYERS,
            verbose=False,
        )
        logger.info("GGUF model loaded from %s", MODEL_PATH)
        return ("gguf", llm)

    elif backend == "transformers":
        # HuggingFace transformers pipeline
        try:
            from transformers import pipeline as hf_pipeline
        except ImportError:
            raise ImportError(
                "transformers not installed. "
                "Run: pip install transformers torch"
            )
        pipe = hf_pipeline(
            "text-generation",
            model=MODEL_PATH,
            device_map="auto",
            # FIX 1: return ONLY the generated text, NOT the prompt
            return_full_text=False,
        )
        logger.info("Transformers model loaded from %s", MODEL_PATH)
        return ("transformers", pipe)

    elif backend == "ollama":
        # Ollama REST API — no local model object needed
        logger.info("Ollama backend configured at %s", OLLAMA_BASE_URL)
        return ("ollama", None)

    else:
        raise ValueError(
            f"Unknown LLM_BACKEND: '{backend}'. "
            "Valid options: 'gguf', 'transformers', 'ollama'"
        )


# ── Inference ─────────────────────────────────────────────────────────────────

def run_inference(prompt: str, task: str = "summarization") -> str:
    """
    Run the LLM on a prompt and return the generated text.

    Parameters
    ----------
    prompt : str
        Full prompt string including system instructions and the abstract.
    task : str
        One of the keys in GENERATION_CONFIG: "summarization", "extraction",
        "scoring".

    Returns
    -------
    str
        The model's generated text, with the prompt stripped if echoed.
    """
    if task not in GENERATION_CONFIG:
        raise ValueError(
            f"Unknown task '{task}'. Valid: {list(GENERATION_CONFIG.keys())}"
        )

    gen_cfg = GENERATION_CONFIG[task]

    # Truncate prompt if too long
    max_chars = MAX_INPUT_TOKENS * 4  # rough chars-per-token estimate
    if len(prompt) > max_chars:
        prompt = prompt[:max_chars]
        logger.warning("Prompt truncated to %d chars for task '%s'", max_chars, task)

    backend_type, model = load_model()

    if backend_type == "gguf":
        text = _run_gguf(model, prompt, gen_cfg)
    elif backend_type == "transformers":
        text = _run_transformers(model, prompt, gen_cfg)
    elif backend_type == "ollama":
        text = _run_ollama(prompt, gen_cfg)
    else:
        raise RuntimeError(f"Unexpected backend type: {backend_type}")

    # Strip any accidental echo of the prompt from the beginning of the output
    text = _strip_prompt_echo(text, prompt)

    logger.debug("run_inference [%s] → %d chars output", task, len(text))
    return text.strip()


# ── Backend implementations ───────────────────────────────────────────────────

def _run_gguf(llm, prompt: str, gen_cfg: dict) -> str:
    """Run inference using llama-cpp-python (GGUF model)."""
    response = llm(
        prompt,
        max_tokens=gen_cfg["max_tokens"],
        temperature=gen_cfg["temperature"],
        # FIX 3: no stop tokens for extraction/scoring so full JSON is returned
        stop=gen_cfg.get("stop", []),
        echo=False,   # NEVER echo the prompt
    )
    return response["choices"][0]["text"]


def _run_transformers(pipe, prompt: str, gen_cfg: dict) -> str:
    """
    Run inference using HuggingFace transformers pipeline.
    return_full_text=False is set at pipeline creation time (load_model),
    so this returns ONLY the generated tokens.
    """
    outputs = pipe(
        prompt,
        max_new_tokens=gen_cfg["max_tokens"],
        temperature=gen_cfg["temperature"],
        do_sample=(gen_cfg["temperature"] > 0),
        # FIX 3: pad_token to suppress the common "no padding token" warning
        pad_token_id=pipe.tokenizer.eos_token_id,
    )
    # outputs is a list of dicts; with return_full_text=False the key is
    # "generated_text" and it contains ONLY the new tokens.
    if isinstance(outputs, list) and outputs:
        return outputs[0].get("generated_text", "")
    return ""


def _run_ollama(prompt: str, gen_cfg: dict) -> str:
    """Run inference via Ollama REST API."""
    import requests
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict":  gen_cfg["max_tokens"],
            "temperature":  gen_cfg["temperature"],
        },
    }
    resp = requests.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json().get("response", "")


# ── Prompt-echo stripper ──────────────────────────────────────────────────────

def _strip_prompt_echo(text: str, prompt: str) -> str:
    """
    If the model accidentally echoed the prompt at the start of its output,
    strip it.  This happens with some backends / model configurations.
    We check the first 200 chars to avoid false positives.
    """
    text_stripped = text.lstrip()

    # Check if the output starts with a substantial portion of the prompt
    # (use first 80 chars of prompt as fingerprint, ignoring leading whitespace)
    prompt_fingerprint = prompt.strip()[:80]
    if prompt_fingerprint and text_stripped.startswith(prompt_fingerprint[:40]):
        # Strip the entire prompt from the beginning
        if text_stripped.startswith(prompt.strip()[:len(prompt.strip()) // 2]):
            # Find where the prompt ends in the output
            idx = text.find(prompt[-50:])
            if idx != -1:
                text = text[idx + 50:]

    return text












