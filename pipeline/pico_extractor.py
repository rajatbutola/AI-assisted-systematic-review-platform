import json
import logging
import re
from typing import Optional

from config.prompts import PICO_EXTRACTION_PROMPT
from core.llm import run_inference
from models.schemas import PICOExtraction

logger = logging.getLogger(__name__)


def extract_pico(abstract: str) -> PICOExtraction:
    """Extract PICO elements from an abstract using the LLM."""

    if not abstract or not abstract.strip():
        return PICOExtraction()

    try:
        prompt = PICO_EXTRACTION_PROMPT.format(abstract=abstract)

        raw_output = run_inference(prompt, task="extraction")

        # Try JSON parsing first
        parsed = _try_parse_json(raw_output)
        if parsed:
            return parsed

        logger.warning("JSON parsing failed, falling back to line parsing.")

        # Fallback parser
        return _parse_lines(raw_output)

    except Exception as e:
        logger.error(f"PICO extraction failed: {e}")

        return PICOExtraction()


def _try_parse_json(text: str) -> Optional[PICOExtraction]:
    """Attempt to parse JSON output from the LLM."""

    try:
        start = text.find("{")
        end = text.rfind("}") + 1

        if start == -1 or end == 0:
            return None

        json_text = text[start:end]

        data = json.loads(json_text)

        return PICOExtraction(
            population=str(data.get("population", "")),
            intervention=str(data.get("intervention", "")),
            comparison=str(data.get("comparison", "")),
            outcome=str(data.get("outcome", "")),
        )

    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _parse_lines(text: str) -> PICOExtraction:
    """Fallback parser when JSON is not returned."""

    result = {
        "population": "",
        "intervention": "",
        "comparison": "",
        "outcome": "",
    }

    for line in text.splitlines():
        line = line.strip()

        for key in result.keys():
            if re.match(rf"^{key}\s*:", line, re.IGNORECASE):
                result[key] = line.split(":", 1)[1].strip()

    return PICOExtraction(**result)