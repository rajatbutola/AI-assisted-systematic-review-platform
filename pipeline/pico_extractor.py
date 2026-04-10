# pipeline/pico_extractor.py 25th March, 2026
# Enhanced PICO extractor — now also captures study_design and sample_size.


# pipeline/pico_extractor.py  — v2 27th March
# Hardened parser identical in structure to quality_assessor v2.



# pipeline/pico_extractor.py  — v3 31st March
# Hardened parser identical in structure to quality_assessor v2.

import json
import logging
import re
from typing import Optional

from config.prompts import PICO_EXTRACTION_PROMPT
from core.llm import run_inference
from models.schemas import PICOExtraction

logger = logging.getLogger(__name__)


def extract_pico(abstract: str) -> PICOExtraction:
    if not abstract or not abstract.strip():
        return PICOExtraction()
    try:
        prompt = PICO_EXTRACTION_PROMPT.format(abstract=abstract)
        raw    = run_inference(prompt, task="extraction")
        logger.debug("PICO raw output: %r", raw[:300])
        return _parse_pico(raw)
    except Exception as e:
        logger.error("PICO extraction failed: %s", e)
        return PICOExtraction()


def _parse_pico(text: str) -> PICOExtraction:
    """
    Parse PICO JSON from LLM output.
    The new prompt provides a filled template so the model knows the exact
    schema. We still do robust extraction in case of leading text.
    """
    start = text.find("{")
    if start == -1:
        logger.warning("No JSON in PICO output: %r", text[:200])
        return _line_fallback_pico(text)

    end = text.rfind("}") + 1
    if end == 0:
        text = text + "}"
        end  = len(text)

    json_str = text[start:end]

    # Fix doubled braces
    if json_str.startswith("{{"):
        json_str = json_str[1:]
    if json_str.endswith("}}"):
        json_str = json_str[:-1]

    try:
        data = json.loads(json_str)
        return PICOExtraction(
            population=   str(data.get("population",   "") or "").strip(),
            intervention= str(data.get("intervention", "") or "").strip(),
            comparison=   str(data.get("comparison",   "") or "").strip(),
            outcome=      str(data.get("outcome",      "") or "").strip(),
        )
    except json.JSONDecodeError as e:
        logger.warning("PICO JSON parse failed (%s): %r", e, json_str[:300])
        return _line_fallback_pico(text)


def _line_fallback_pico(text: str) -> PICOExtraction:
    """Extract PICO from plain-text output when JSON parsing fails."""
    result = {"population": "", "intervention": "", "comparison": "", "outcome": ""}
    for line in text.splitlines():
        line = line.strip()
        for key in result:
            if re.match(rf"^{key}\s*:", line, re.IGNORECASE):
                result[key] = line.split(":", 1)[1].strip()
    return PICOExtraction(**result)











