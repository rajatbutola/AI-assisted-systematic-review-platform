import re
from typing import Optional

from config.prompts import RELEVANCE_SCORING_PROMPT
from core.llm import run_inference


def score_relevance(abstract: str) -> Optional[float]:
    if not abstract.strip():
        return None

    prompt = RELEVANCE_SCORING_PROMPT.format(abstract=abstract)
    raw_output = run_inference(prompt, task="scoring")
    return parse_score(raw_output)


def parse_score(text: str) -> Optional[float]:
    text = text.strip()

    patterns = [
        r"^\s*(\d+(?:\.\d+)?)\s*$",
        r"(\d+(?:\.\d+)?)\s*/\s*10",
        r"score[:\s]+(\d+(?:\.\d+)?)",
        r"(\d+(?:\.\d+)?)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            score = float(match.group(1))
            return max(0.0, min(score, 10.0))

    return None