# pipeline/relevance_scorer.py 

import re
from typing import Optional

from config.prompts import RELEVANCE_SCORING_PROMPT
from core.llm import run_inference

def score_relevance(abstract: str) -> Optional[float]:
    if not abstract.strip():
        return None

    prompt = RELEVANCE_SCORING_PROMPT.format(abstract=abstract)
    raw_output = run_inference(prompt, task="scoring")
    print(f"RAW SCORER OUTPUT: '{raw_output}'")  # Add this line
    return parse_score(raw_output)

def parse_score(text: str) -> Optional[float]:
    text = text.strip().upper()

    # Handle YES/NO responses (primary path with LLaMA 3.1)
    if text.startswith("YES"):
        return 1.0
    if text.startswith("NO"):
        return 0.0

    # Fallback: numeric patterns (kept for safety)
    import re
    patterns = [
        r"^\s*(\d+(?:\.\d+)?)\s*$",
        r"(\d+(?:\.\d+)?)\s*/\s*10",
        r"score[:\s]+(\d+(?:\.\d+)?)",
        r"(\d+(?:\.\d+)?)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            return min(val / 10.0, 1.0) if val > 1.0 else val

    return None
