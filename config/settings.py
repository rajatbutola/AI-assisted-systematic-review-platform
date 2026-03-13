MODEL_NAME = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"
MAX_INPUT_TOKENS = 1800

GENERATION_CONFIG = {
    "summarization": {
        "max_new_tokens": 250,
        #"temperature": 0.1,
        "do_sample": False,
    },
    "extraction": {
        "max_new_tokens": 150,
        "temperature": 0.0,
        "do_sample": False,
    },
    "scoring": {
        "max_new_tokens": 10,
        "temperature": 0.0,
        "do_sample": False,
    },
}
