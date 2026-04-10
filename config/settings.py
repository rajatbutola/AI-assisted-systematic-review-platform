# config/settings.py


# config/settings.py  — v2 27th March
#
# Key change: GENERATION_CONFIG stop tokens for extraction/scoring removed.
# Stop tokens like ["\n\n"] caused the model to terminate after the first
# line of a multi-line JSON object, producing truncated/invalid JSON.

# config/settings.py  — v2
#
# Key change: GENERATION_CONFIG stop tokens for extraction/scoring removed.
# Stop tokens like ["\n\n"] caused the model to terminate after the first
# line of a multi-line JSON object, producing truncated/invalid JSON.


# config/settings.py — v3 (FINAL) 30th March

from dotenv import load_dotenv
import os

load_dotenv()

NCBI_EMAIL   = os.getenv("NCBI_EMAIL")
NCBI_API_KEY = os.getenv("NCBI_API_KEY")
CORE_API_KEY = os.getenv("CORE_API_KEY", "")

MODEL_PATH = os.getenv(
    "MODEL_PATH",
    "models/meta-llama-Llama-3.1-8B-Q8_0.gguf"
)

N_CTX            = 4096
N_THREADS        = 12
N_GPU_LAYERS     = 0
MAX_INPUT_TOKENS = 3000   # slightly lower to leave room for completion

GENERATION_CONFIG = {
    "summarization": {
        "max_tokens": 400,
        "temperature": 0.1,
        "stop": ["\n\n\n"],
    },
    "extraction": {
        # Raised from 350 → 800 to prevent truncation of 14-field JSON.
        # A single JSON object with realistic clinical values can be 500-700 tokens.
        # 800 gives enough headroom without hitting the context limit.
        "max_tokens": 800,
        "temperature": 0.0,
        "stop": [],
    },
    "scoring": {
        # Plain integer lines are very short — 150 tokens is plenty.
        # (5 lines of "key: N" = ~50 tokens)
        "max_tokens": 150,
        "temperature": 0.0,
        "stop": [],
    },
}

SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "")
OPENALEX_EMAIL           = os.getenv("OPENALEX_EMAIL", NCBI_EMAIL or "")

def _detect_backend(model_path: str) -> str:
    return "gguf" if model_path.lower().endswith(".gguf") else "transformers"

_env_backend = os.getenv("LLM_BACKEND", "").strip().lower()
LLM_BACKEND  = _env_backend if _env_backend in ("gguf", "transformers", "ollama") \
               else _detect_backend(MODEL_PATH)

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",    "llama3.2:3b-instruct-q4_K_M")
