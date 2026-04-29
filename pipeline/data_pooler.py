# pipeline/data_pooler.py v1 - 25th March, 2026
#
# Data Pooling module — extracts structured quantitative data from study abstracts
# and aggregates it into a composite table suitable for systematic reviews and
# future meta-analysis.
#
# Architecture notes:
#   - Each study is extracted independently (parallelisable in future)
#   - Results are stored as JSON in the articles table (analysis_json column)
#   - The composite table is assembled on-demand in the UI
#   - Fields mirror PRISMA/CONSORT reporting requirements



# pipeline/data_pooler.py  — v2 27th March
# Same hardening pattern as quality_assessor v2.


# pipeline/data_pooler.py — v3  30th March
#
# ROOT CAUSE OF UNTERMINATED STRING ERRORS:
# max_tokens=350 was too small. A 14-field JSON object with realistic
# values (e.g. "adverse_events": "grade ≥3 neutropenia (65%) and
# thrombocytopenia (50%), with febrile neutropenia in 38%") easily
# exceeds 350 tokens. The model stops generating mid-string, leaving
# the JSON incomplete. json.loads() then raises "Unterminated string".
#
# FIXES:
# 1. max_tokens raised to 800 in settings.py
# 2. Prompt instructs model to use SHORT values (under 15 words each)
#    to stay within the token budget
# 3. JSON repair function: closes unterminated strings and objects
#    before attempting json.loads, recovering partial results
# 4. Field-by-field regex extraction as final fallback






# pipeline/data_pooler.py — v4 (FINAL) 31st March
#
# ROOT CAUSE OF UNTERMINATED STRING ERRORS:
# max_tokens=350 was too small. A 14-field JSON object with realistic
# values (e.g. "adverse_events": "grade ≥3 neutropenia (65%) and
# thrombocytopenia (50%), with febrile neutropenia in 38%") easily
# exceeds 350 tokens. The model stops generating mid-string, leaving
# the JSON incomplete. json.loads() then raises "Unterminated string".
#
# FIXES:
# 1. max_tokens raised to 800 in settings.py
# 2. Prompt instructs model to use SHORT values (under 15 words each)
#    to stay within the token budget
# 3. JSON repair function: closes unterminated strings and objects
#    before attempting json.loads, recovering partial results
# 4. Field-by-field regex extraction as final fallback

import json
import logging
import re
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

from config.prompts import DATA_POOLING_PROMPT
from core.llm import run_inference

logger = logging.getLogger(__name__)

_NR = "Not reported"   # shorthand


@dataclass
class StudyData:
    pmid: str = ""
    title: str = ""
    sample_size: str = _NR
    age_group: str = _NR
    sex_distribution: str = _NR
    condition_severity: str = _NR
    intervention: str = _NR
    comparator: str = _NR
    follow_up_duration: str = _NR
    primary_outcome: str = _NR
    primary_outcome_result: str = _NR
    # Per-arm event counts for SoF table absolute effect calculation
    events_intervention: str = _NR   # e.g. "19/266 (7.1%)"
    events_comparator:   str = _NR   # e.g. "44/359 (12.3%)"
    secondary_outcomes: List[str] = field(default_factory=list)
    adverse_events: str = _NR
    statistical_significance: str = _NR
    study_design: str = _NR
    country_setting: str = _NR

    def to_table_row(self) -> Dict:
        sec = "; ".join(self.secondary_outcomes) if self.secondary_outcomes else _NR
        return {
            "PMID":               self.pmid,
            "Title":              self.title[:70] + ("…" if len(self.title) > 70 else ""),
            "Study Design":       self.study_design,
            "N":                  self.sample_size,
            "Age Group":          self.age_group,
            "Sex (M/F)":          self.sex_distribution,
            "Severity/Baseline":  self.condition_severity,
            "Intervention":       self.intervention,
            "Comparator":         self.comparator,
            "Follow-up":          self.follow_up_duration,
            "Primary Outcome":    self.primary_outcome,
            "Primary Result":     self.primary_outcome_result,
            "Events (intervention)": self.events_intervention,
            "Events (comparator)":   self.events_comparator,
            "Secondary Outcomes": sec,
            "Adverse Events":     self.adverse_events,
            "Significance":       self.statistical_significance,
            "Country/Setting":    self.country_setting,
        }

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "StudyData":
        known = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        return cls(**known)


def extract_study_data(abstract: str, pmid: str = "", title: str = "") -> StudyData:
    if not abstract or not abstract.strip():
        return StudyData(pmid=pmid, title=title,
                         primary_outcome_result="No abstract available.")
    try:
        prompt = DATA_POOLING_PROMPT.format(abstract=abstract)
        raw    = run_inference(prompt, task="extraction")
        logger.debug("DataPool raw [%s]: %r", pmid, raw[:400])
        return _parse_study_data(raw, pmid=pmid, title=title)
    except Exception as e:
        logger.error("Data pooling failed PMID %s: %s", pmid, e)
        return StudyData(pmid=pmid, title=title,
                         primary_outcome_result=f"Extraction error: {e}")


def _repair_json(text: str) -> str:
    """
    Attempt to repair truncated JSON so json.loads can recover partial data.
    Handles the most common truncation pattern: an unterminated string value.

    Strategy:
    1. Strip everything before the first {
    2. If the string ends without a closing }, close all open structures
    3. Close any unterminated string (odd number of unescaped quotes)
    """
    start = text.find("{")
    if start == -1:
        return "{}"
    text = text[start:]

    # Count open braces to determine if we need to close the object
    open_braces  = text.count("{") - text.count("}")
    open_brackets = text.count("[") - text.count("]")

    # Check if we're inside an unterminated string
    # Simple heuristic: count unescaped " characters after the last :
    # If odd, we're inside a string value
    # Find the last colon to check what's after it
    last_colon = text.rfind(":")
    if last_colon != -1:
        after_colon = text[last_colon + 1:].strip()
        # Count unescaped double quotes in after_colon
        unescaped_quotes = len(re.findall(r'(?<!\\)"', after_colon))
        if unescaped_quotes % 2 == 1:
            # We're inside an unterminated string — close it
            # Truncate at the last safe position (before a comma or end of value)
            # Find the last complete value by looking backwards
            text = text.rstrip()
            # Remove trailing partial content after last complete comma-separated value
            # Find the last complete "key": "value" pair
            last_complete = text.rfind('",')
            last_complete2 = text.rfind('",\n')
            safe_end = max(last_complete, last_complete2)
            if safe_end > last_colon:
                text = text[:safe_end + 1]  # include the closing quote
            else:
                # Just close the string and the object
                text = text + '"'

    # Close any open arrays
    for _ in range(max(0, open_brackets)):
        text = text.rstrip().rstrip(",") + "]"

    # Close the main object
    for _ in range(max(0, open_braces)):
        text = text.rstrip().rstrip(",") + "}"

    return text


def _parse_study_data(text: str, pmid: str, title: str) -> StudyData:
    """Parse data pool JSON with repair and field-level regex fallback."""

    start = text.find("{")
    if start == -1:
        return StudyData(pmid=pmid, title=title,
                         primary_outcome_result="No JSON found in output.")

    json_str = text[start:]

    # Try direct parse first
    data = None
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        # Try repair
        repaired = _repair_json(json_str)
        try:
            data = json.loads(repaired)
            logger.info("DataPool JSON repaired for PMID %s", pmid)
        except json.JSONDecodeError as e:
            logger.warning("DataPool JSON repair failed for PMID %s: %s", pmid, e)
            # Fall through to regex extraction

    def _val(key: str) -> str:
        if data:
            v = data.get(key)
            if v and v not in ("", "NR", "Not reported", "not reported", None, []):
                return str(v).strip()
        # Regex fallback: look for "key": "value" in raw text
        m = re.search(rf'"{key}"\s*:\s*"([^"{{}}]+)"', text)
        if m:
            v = m.group(1).strip()
            if v and v.lower() not in ("nr", "not reported", ""):
                return v
        return _NR

    def _list_val(key: str) -> List[str]:
        if data:
            v = data.get(key, [])
            if isinstance(v, list) and v:
                return [str(x).strip() for x in v if x]
        m = re.search(rf'"{key}"\s*:\s*\[([^\]]*)\]', text)
        if m:
            items = re.findall(r'"([^"]+)"', m.group(1))
            return [i.strip() for i in items if i.strip()]
        return []

    return StudyData(
        pmid=pmid, title=title,
        sample_size=         _val("sample_size"),
        age_group=           _val("age_group"),
        sex_distribution=    _val("sex_distribution"),
        condition_severity=  _val("condition_severity"),
        intervention=        _val("intervention"),
        comparator=          _val("comparator"),
        follow_up_duration=  _val("follow_up_duration"),
        primary_outcome=     _val("primary_outcome"),
        primary_outcome_result= _val("primary_outcome_result"),
        events_intervention=     _val("events_intervention"),
        events_comparator=       _val("events_comparator"),
        secondary_outcomes=  _list_val("secondary_outcomes"),
        adverse_events=      _val("adverse_events"),
        statistical_significance= _val("statistical_significance"),
        study_design=        _val("study_design"),
        country_setting=     _val("country_setting"),
    )


def build_composite_table(study_data_list: List[StudyData]) -> List[Dict]:
    return [sd.to_table_row() for sd in study_data_list]











