# pipeline/quality_assessor.py v1 25th March, 2026
#
# Replaces the old pipeline/relevance_scorer.py.
#
# Rationale for change:
#   The original scorer asked "Is this a clinical study? YES/NO" which provides
#   minimal value after articles have already been retrieved via a clinical query.
#   By the time AI Analysis runs, all screened-in articles are assumed to be
#   clinically relevant.
#
#   This module instead performs a lightweight methodological quality assessment
#   aligned with established SR frameworks (GRADE, Cochrane RoB2, AMSTAR-2).
#   It scores 5 domains (1-3 each) → max 15 → maps to low/moderate/high quality.
#
#   This quality score is:
#   (a) Displayed per study in the AI Analysis tab
#   (b) Included in the data pooling table for context
#   (c) Useful for sensitivity analyses (e.g., "high-quality studies only")


# pipeline/quality_assessor.py — v3 27th March
#
# Changes:
# - Parser now handles chain-of-thought output: the model may write
#   reasoning text before the JSON. We find the LAST { } block,
#   not the first, since the model may use {} in its reasoning.
# - Regex fallback improved to find integers next to score keys.
# - Clamping ensures scores are always 1-3, never 0.



# pipeline/quality_assessor.py — v4 30th March
#
# Completely new parser that reads plain integer lines instead of JSON.
# The prompt now asks for output like:
#   randomisation: 2
#   sample_size: 2
#   outcomes: 3
#   followup: 2
#   comparator: 1
#
# This is trivial to parse with a simple line scan and eliminates ALL
# JSON-related failures (invalid keys, truncated objects, "?" literals).



# pipeline/quality_assessor.py — v5 (FINAL) 31st March
#
# Completely new parser that reads plain integer lines instead of JSON.
# The prompt now asks for output like:
#   randomisation: 2
#   sample_size: 2
#   outcomes: 3
#   followup: 2
#   comparator: 1
#
# This is trivial to parse with a simple line scan and eliminates ALL
# JSON-related failures (invalid keys, truncated objects, "?" literals).

import re
import logging
from dataclasses import dataclass
from typing import Optional

from config.prompts import QUALITY_ASSESSMENT_PROMPT
from core.llm import run_inference

logger = logging.getLogger(__name__)


@dataclass
class QualityAssessment:
    randomisation_score:     int = 0
    sample_size_score:       int = 0
    outcome_reporting_score: int = 0
    followup_score:          int = 0
    comparator_score:        int = 0
    overall_quality:         str = "unknown"
    quality_notes:           str = ""
    total_score:             int = 0

    def to_display_dict(self) -> dict:
        return {
            "Randomisation / Design": f"{self.randomisation_score}/3",
            "Sample Size":            f"{self.sample_size_score}/3",
            "Outcome Reporting":      f"{self.outcome_reporting_score}/3",
            "Follow-up":              f"{self.followup_score}/3",
            "Comparator":             f"{self.comparator_score}/3",
            "Total Score":            f"{self.total_score}/15",
            "Overall Quality":        self.overall_quality.capitalize(),
            "Notes":                  self.quality_notes,
        }


def assess_quality(abstract: str) -> QualityAssessment:
    if not abstract or not abstract.strip():
        return QualityAssessment(overall_quality="unknown",
                                 quality_notes="No abstract available.")
    try:
        prompt = QUALITY_ASSESSMENT_PROMPT.format(abstract=abstract)
        raw    = run_inference(prompt, task="scoring")
        logger.debug("Quality raw output: %r", raw[:300])
        return _parse_plain_integers(raw)
    except Exception as e:
        logger.error("Quality assessment failed: %s", e)
        return QualityAssessment(overall_quality="unknown",
                                 quality_notes=f"Assessment error: {e}")


def _parse_plain_integers(text: str) -> QualityAssessment:
    """
    Parse scores from plain-text output of the form:
        randomisation: 2
        sample_size: 2
        outcomes: 3
        followup: 2
        comparator: 1

    Handles:
    - Extra whitespace, mixed case
    - Model adding prose before/after the scores
    - Colon or equals sign separator
    - Scores written as words ("two") — converted via word map
    - Fallback: scan all integers in order if line-based fails
    """
    # Word → digit map for models that write out numbers
    WORD_DIGITS = {
        "one": 1, "two": 2, "three": 3,
        "1": 1, "2": 2, "3": 3,
    }

    def _extract_score(pattern: str) -> Optional[int]:
        """Find the integer after a pattern key."""
        # Try "key: N" or "key = N" format
        m = re.search(
            rf'{pattern}\s*[:=]\s*(\d|one|two|three)',
            text, re.IGNORECASE
        )
        if m:
            val = m.group(1).lower()
            return WORD_DIGITS.get(val, 1)
        return None

    r   = _extract_score(r'randomi[sz]ation') or _extract_score(r'domain\s*1')
    ss  = _extract_score(r'sample[\s_]size')  or _extract_score(r'domain\s*2')
    or_ = _extract_score(r'outcomes?')          or _extract_score(r'domain\s*3')
    fu  = _extract_score(r'follow[\s_-]?up')  or _extract_score(r'domain\s*4')
    co  = _extract_score(r'comparator')       or _extract_score(r'domain\s*5')

    # If line-based extraction missed any, try positional fallback
    # (scan all standalone integers 1-3 in the text)
    if None in (r, ss, or_, fu, co):
        integers = re.findall(r'\b([123])\b', text)
        # Try to assign positionally
        if len(integers) >= 5:
            r   = r   or int(integers[0])
            ss  = ss  or int(integers[1])
            or_ = or_ or int(integers[2])
            fu  = fu  or int(integers[3])
            co  = co  or int(integers[4])

    # Final defaults for anything still None
    def _clamp(v, default=1):
        if v is None:
            return default
        return max(1, min(3, int(v)))

    r   = _clamp(r)
    ss  = _clamp(ss)
    or_ = _clamp(or_)
    fu  = _clamp(fu)
    co  = _clamp(co)
    total = r + ss + or_ + fu + co

    quality = "high" if total >= 12 else "moderate" if total >= 8 else "low"

    # Try to find a quality_notes sentence in the output
    notes = ""
    for line in text.splitlines():
        line = line.strip()
        if len(line) > 30 and not re.match(r'^(randomi|sample|outcome|follow|comparator|domain)', line, re.I):
            notes = line[:150]
            break

    return QualityAssessment(
        randomisation_score=r, sample_size_score=ss,
        outcome_reporting_score=or_, followup_score=fu,
        comparator_score=co, overall_quality=quality,
        quality_notes=notes, total_score=total,
    )









