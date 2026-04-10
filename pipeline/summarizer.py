# pipeline/summarizer.py v1 25th March, 2026

# pipeline/summarizer.py  — v2 27th March


# pipeline/summarizer.py — v3
#
# Changes from v2:
# - The few-shot prompt now ends with "•" so the model continues from
#   the bullet. The _clean_summary strips and rebuilds properly.
# - Handles the case where the model outputs "Study Design: X" without
#   the leading "•" (common with instruction-tuned models).
# - Cleaner deduplication: only keep the FIRST 5 valid bullet lines.


# pipeline/summarizer.py — v4 31st March



import re
import logging
from config.prompts import SUMMARIZATION_PROMPT
from core.llm import run_inference

logger = logging.getLogger(__name__)

_BULLET_KEYS = [
    "Study Design",
    "Population",
    "Intervention",
    "Key Findings",
    "Conclusion",
]

_PROMPT_SENTINELS = [
    "Summarize the following",
    "You are a systematic review",
    "You are an expert systematic",
    "Now summarize this abstract",
    "Example of correct output",
    "Return a single JSON",
]


def summarize_with_llm(abstract: str) -> str:
    if not abstract or not abstract.strip():
        return "No abstract available to summarize."

    prompt = SUMMARIZATION_PROMPT.format(abstract=abstract)
    raw    = run_inference(prompt, task="summarization")
    logger.debug("Summary raw: %r", raw[:400])
    return _clean_summary(raw)


def _clean_summary(text: str) -> str:
    """
    Post-process LLM output to extract exactly 5 clean bullet lines.
    Handles:
    - Prompt echo at start of output
    - Template placeholder text like "[type of study]"
    - Missing "•" prefix
    - Repeated bullet sets
    - Bullets all on one line (separated by "•")
    """
    # Step 1: strip prompt contamination
    for sentinel in _PROMPT_SENTINELS:
        if sentinel.lower() in text.lower():
            # Find the LAST occurrence of "Summary:" or the first bullet key
            idx = text.lower().rfind("summary:")
            if idx != -1:
                text = text[idx + len("summary:"):].strip()
                break
            # Otherwise find the first bullet key
            for key in _BULLET_KEYS:
                idx = text.find(key + ":")
                if idx != -1:
                    text = text[idx:].strip()
                    break
            break

    # Step 2: if bullets are on one line separated by "•", split them
    if text.count("•") >= 4 and "\n" not in text[:200]:
        text = text.replace("•", "\n•")

    # Step 3: collect valid bullet lines
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Accept "• Key: value" or "Key: value" formats
        is_bullet_key = any(
            key.lower() in line.lower() and ":" in line
            for key in _BULLET_KEYS
        )
        if is_bullet_key:
            lines.append(line)

    # Step 4: deduplicate — keep only FIRST occurrence of each key
    seen = set()
    deduped = []
    for line in lines:
        for key in _BULLET_KEYS:
            if key.lower() in line.lower():
                if key not in seen:
                    seen.add(key)
                    deduped.append(line)
                break

    # Step 5: ensure "•" prefix on every line
    cleaned = []
    for line in deduped:
        # Remove template placeholders like [type of study]
        line = re.sub(r'\[.*?\]', '', line).strip()
        if not line or line in ("•", "•  :", "• :"):
            continue
        if not line.startswith("•"):
            line = "• " + line.lstrip("-* ")
        cleaned.append(line)

    if not cleaned:
        # Last resort: return the raw text stripped
        stripped = text.strip()
        if len(stripped) > 20:
            return stripped
        return "Summary could not be generated. Please check that your LLM model is loaded and responding."

    return "\n".join(cleaned[:5])   # cap at 5 bullets















