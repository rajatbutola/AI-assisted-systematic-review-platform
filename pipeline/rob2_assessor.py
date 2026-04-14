# pipeline/rob2_assessor.py
#
# Cochrane Risk of Bias Tool version 2 (RoB 2) implementation.
#
# Reference: Sterne et al., BMJ 2019;366:l4898
#            https://doi.org/10.1136/bmj.l4898
#
# Architecture:
#   - The LLM answers signalling questions (natural language extraction)
#   - The domain-level judgement follows the published RoB 2 algorithm
#     (deterministic rules — NOT AI)
#   - This separation makes results auditable and defensible to reviewers
#
# The five RoB 2 domains:
#   D1: Randomisation process
#   D2: Deviations from intended interventions
#   D3: Missing outcome data
#   D4: Measurement of the outcome
#   D5: Selection of the reported result
#
# Signalling question answers: Y=Yes, PY=Probably Yes,
#                              PN=Probably No, N=No, NI=No Information
#
# Overall judgement algorithm (Cochrane):
#   Low risk    — all domains Low risk
#   Some concerns — any domain Some concerns, no domain High risk
#   High risk   — any domain High risk OR ≥3 domains Some concerns

import re
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict

from core.llm import run_inference
from config.prompts import ROB2_PROMPT

logger = logging.getLogger(__name__)


# ── Data classes ───────────────────────────────────────────────────────────────

RISK_LEVELS = ("low", "some_concerns", "high", "no_information")
RISK_EMOJI  = {
    "low":           "🟢",
    "some_concerns": "🟡",
    "high":          "🔴",
    "no_information":"⬜",
}
RISK_LABEL  = {
    "low":           "Low risk",
    "some_concerns": "Some concerns",
    "high":          "High risk",
    "no_information":"No information",
}


@dataclass
class RoB2Domain:
    name:        str
    judgement:   str = "no_information"   # low / some_concerns / high / no_information
    signals:     List[str] = field(default_factory=list)   # signalling question answers
    rationale:   str = ""

    @property
    def emoji(self) -> str:
        return RISK_EMOJI.get(self.judgement, "⬜")

    @property
    def label(self) -> str:
        return RISK_LABEL.get(self.judgement, "No information")


@dataclass
class RoB2Assessment:
    d1_randomisation:   RoB2Domain = field(default_factory=lambda: RoB2Domain("D1: Randomisation process"))
    d2_deviations:      RoB2Domain = field(default_factory=lambda: RoB2Domain("D2: Deviations from interventions"))
    d3_missing_data:    RoB2Domain = field(default_factory=lambda: RoB2Domain("D3: Missing outcome data"))
    d4_measurement:     RoB2Domain = field(default_factory=lambda: RoB2Domain("D4: Outcome measurement"))
    d5_reporting:       RoB2Domain = field(default_factory=lambda: RoB2Domain("D5: Selective reporting"))
    overall:            str = "no_information"
    overall_rationale:  str = ""
    notes:              str = ""

    @property
    def domains(self) -> List[RoB2Domain]:
        return [self.d1_randomisation, self.d2_deviations,
                self.d3_missing_data, self.d4_measurement, self.d5_reporting]

    @property
    def overall_emoji(self) -> str:
        return RISK_EMOJI.get(self.overall, "⬜")

    @property
    def overall_label(self) -> str:
        return RISK_LABEL.get(self.overall, "No information")

    def to_export_dict(self) -> dict:
        """For CSV/JSON export."""
        d = {"Overall RoB": self.overall_label}
        for dom in self.domains:
            d[dom.name] = dom.label
        d["RoB Notes"] = self.notes
        return d


# ── Main entry point ───────────────────────────────────────────────────────────

def assess_rob2(abstract: str, full_text: str = "") -> RoB2Assessment:
    """
    Run RoB 2 assessment on an RCT abstract (and full text if available).

    Step 1: LLM extracts signalling question answers from the text.
    Step 2: Deterministic algorithm converts answers to domain judgements.
    Step 3: Deterministic algorithm computes overall judgement.
    """
    if not abstract or not abstract.strip():
        return RoB2Assessment(notes="No abstract available for assessment.")

    # Use full text if available (more information = more accurate assessment)
    # Truncate to stay within token budget
    text_for_llm = (full_text[:3000] if full_text else abstract[:2000])

    try:
        prompt = ROB2_PROMPT.format(text=text_for_llm)
        raw    = run_inference(prompt, task="extraction")
        logger.debug("RoB 2 raw output: %r", raw[:400])
        return _parse_rob2(raw)
    except Exception as e:
        logger.error("RoB 2 assessment failed: %s", e)
        return RoB2Assessment(notes=f"Assessment error: {e}")


# ── Parser ─────────────────────────────────────────────────────────────────────

def _parse_rob2(text: str) -> RoB2Assessment:
    """
    Parse LLM output and apply the deterministic RoB 2 algorithm.

    Expected LLM output format (plain text, one answer per line):
        D1_random_sequence: Y
        D1_allocation_concealment: PY
        D1_baseline_imbalance: N
        D2_blinding_participants: Y
        D2_blinding_providers: PY
        D2_protocol_deviations: N
        D3_missing_data_proportion: PN
        D3_missing_data_related: NI
        D4_blinded_assessors: Y
        D4_outcome_measurement: Y
        D5_preregistered: PY
        D5_outcomes_reported: Y
    """
    answers = _extract_answers(text)

    d1 = _judge_d1(answers)
    d2 = _judge_d2(answers)
    d3 = _judge_d3(answers)
    d4 = _judge_d4(answers)
    d5 = _judge_d5(answers)
    overall, rationale = _judge_overall([d1, d2, d3, d4, d5])

    return RoB2Assessment(
        d1_randomisation=d1,
        d2_deviations=d2,
        d3_missing_data=d3,
        d4_measurement=d4,
        d5_reporting=d5,
        overall=overall,
        overall_rationale=rationale,
    )


def _extract_answers(text: str) -> Dict[str, str]:
    """Extract key: answer pairs from LLM output."""
    answers = {}
    # Match patterns like "D1_random_sequence: Y" or "D1_random_sequence = PY"
    pattern = re.compile(
        r'(D\d_\w+)\s*[:=]\s*(Y|PY|PN|N|NI)\b',
        re.IGNORECASE
    )
    for m in pattern.finditer(text):
        key = m.group(1).upper()
        val = m.group(2).upper()
        answers[key] = val
    logger.debug("RoB 2 extracted answers: %s", answers)
    return answers


def _pos(answer: str) -> bool:
    """Returns True if answer is positive (Y or PY)."""
    return answer in ("Y", "PY")

def _neg(answer: str) -> bool:
    """Returns True if answer is negative (N or PN)."""
    return answer in ("N", "PN")

def _ni(answer: str) -> bool:
    """Returns True if no information."""
    return answer in ("NI", "")


# ── Domain algorithms (from Cochrane RoB 2 guidance) ──────────────────────────

def _judge_d1(a: Dict[str, str]) -> RoB2Domain:
    """
    D1: Randomisation process
    Q1.1 Was allocation sequence random?
    Q1.2 Was allocation concealed?
    Q1.3 Were there baseline imbalances?
    """
    q1 = a.get("D1_RANDOM_SEQUENCE", "NI")
    q2 = a.get("D1_ALLOCATION_CONCEALMENT", "NI")
    q3 = a.get("D1_BASELINE_IMBALANCE", "NI")

    signals = [f"Allocation random: {q1}",
               f"Concealment: {q2}",
               f"Baseline imbalance: {q3}"]

    if _pos(q1) and _pos(q2) and not _pos(q3):
        j, r = "low", "Random sequence generation confirmed, concealment adequate, no baseline imbalance."
    elif _neg(q1) or _neg(q2):
        j, r = "high", "Allocation sequence not random or not concealed."
    elif _pos(q3):
        j, r = "high", "Baseline imbalances detected suggesting randomisation failure."
    elif _ni(q1) or _ni(q2):
        j, r = "some_concerns", "Insufficient information about randomisation process."
    else:
        j, r = "some_concerns", "Randomisation process partially reported."

    return RoB2Domain("D1: Randomisation process", j, signals, r)


def _judge_d2(a: Dict[str, str]) -> RoB2Domain:
    """
    D2: Deviations from intended interventions
    Q2.1 Were participants blinded?
    Q2.2 Were providers blinded?
    Q2.3 Were there protocol deviations arising from trial context?
    """
    q1 = a.get("D2_BLINDING_PARTICIPANTS", "NI")
    q2 = a.get("D2_BLINDING_PROVIDERS", "NI")
    q3 = a.get("D2_PROTOCOL_DEVIATIONS", "NI")

    signals = [f"Participant blinding: {q1}",
               f"Provider blinding: {q2}",
               f"Protocol deviations: {q3}"]

    if _pos(q3):
        j, r = "high", "Clinically meaningful protocol deviations occurred."
    elif _pos(q1) and _pos(q2) and not _pos(q3):
        j, r = "low", "Double-blind design with no reported deviations."
    elif _ni(q1) and _ni(q2):
        j, r = "some_concerns", "Blinding status not reported."
    elif _neg(q1) or _neg(q2):
        # Open-label is acceptable IF ITT analysis used and no deviations
        j, r = "some_concerns", "Open-label design — risk of performance/detection bias."
    else:
        j, r = "some_concerns", "Blinding partially reported."

    return RoB2Domain("D2: Deviations from interventions", j, signals, r)


def _judge_d3(a: Dict[str, str]) -> RoB2Domain:
    """
    D3: Missing outcome data
    Q3.1 Were outcome data available for all participants?
    Q3.2 Is missing data related to the outcome?
    """
    q1 = a.get("D3_MISSING_DATA_PROPORTION", "NI")
    q2 = a.get("D3_MISSING_DATA_RELATED", "NI")

    signals = [f"Missing data proportion: {q1}",
               f"Missing related to outcome: {q2}"]

    if _pos(q1) and not _pos(q2):
        # Low missing AND not related to outcome
        j, r = "low", "Missing data minimal and likely unrelated to outcome."
    elif _neg(q1) and _pos(q2):
        j, r = "high", "Substantial missing data that may be outcome-related."
    elif _neg(q1) and _ni(q2):
        j, r = "some_concerns", "Missing data present; unclear if related to outcome."
    elif _ni(q1):
        j, r = "some_concerns", "Completeness of outcome data not reported."
    else:
        j, r = "some_concerns", "Missing outcome data — incomplete information."

    return RoB2Domain("D3: Missing outcome data", j, signals, r)


def _judge_d4(a: Dict[str, str]) -> RoB2Domain:
    """
    D4: Measurement of the outcome
    Q4.1 Was the outcome assessor blinded?
    Q4.2 Was the outcome measurement valid and reliable?
    """
    q1 = a.get("D4_BLINDED_ASSESSORS", "NI")
    q2 = a.get("D4_OUTCOME_MEASUREMENT", "NI")

    signals = [f"Blinded outcome assessors: {q1}",
               f"Valid/reliable measurement: {q2}"]

    if _pos(q1) and _pos(q2):
        j, r = "low", "Blinded outcome assessment with validated measurement tool."
    elif _neg(q2):
        j, r = "high", "Outcome measurement method invalid or unreliable."
    elif _neg(q1) and _ni(q2):
        j, r = "some_concerns", "Unblinded outcome assessment."
    elif _ni(q1) or _ni(q2):
        j, r = "some_concerns", "Insufficient information on outcome measurement."
    else:
        j, r = "some_concerns", "Outcome measurement partially reported."

    return RoB2Domain("D4: Outcome measurement", j, signals, r)


def _judge_d5(a: Dict[str, str]) -> RoB2Domain:
    """
    D5: Selection of the reported result
    Q5.1 Was the trial pre-registered with outcomes specified?
    Q5.2 Were outcomes consistent with pre-registration?
    """
    q1 = a.get("D5_PREREGISTERED", "NI")
    q2 = a.get("D5_OUTCOMES_REPORTED", "NI")

    signals = [f"Pre-registered: {q1}",
               f"Outcomes consistent: {q2}"]

    if _pos(q1) and _pos(q2):
        j, r = "low", "Trial pre-registered; reported outcomes consistent with registration."
    elif _neg(q2):
        j, r = "high", "Reported outcomes inconsistent with pre-registration — likely selective reporting."
    elif _ni(q1):
        j, r = "some_concerns", "No pre-registration mentioned; selective reporting cannot be excluded."
    elif _pos(q1) and _ni(q2):
        j, r = "some_concerns", "Pre-registered but outcome consistency not verifiable."
    else:
        j, r = "some_concerns", "Insufficient reporting to assess selective outcome reporting."

    return RoB2Domain("D5: Selective reporting", j, signals, r)


def _judge_overall(domains: List[RoB2Domain]) -> tuple:
    """
    Apply the Cochrane RoB 2 overall judgement algorithm.

    Rules (from RoB 2 guidance):
      High risk    — ANY domain is High risk
                   OR ≥3 domains are Some concerns
      Some concerns— ANY domain is Some concerns (and none are High risk)
      Low risk     — ALL domains are Low risk
    """
    judgements = [d.judgement for d in domains]
    n_high     = judgements.count("high")
    n_some     = judgements.count("some_concerns")
    high_names = [d.name for d in domains if d.judgement == "high"]
    some_names = [d.name for d in domains if d.judgement == "some_concerns"]

    if n_high > 0:
        rationale = f"High risk in: {', '.join(high_names)}."
        return "high", rationale

    if n_some >= 3:
        rationale = (f"≥3 domains with some concerns: {', '.join(some_names)}. "
                     "Cochrane algorithm → overall High risk.")
        return "high", rationale

    if n_some > 0:
        rationale = f"Some concerns in: {', '.join(some_names)}."
        return "some_concerns", rationale

    if all(j == "low" for j in judgements):
        return "low", "All five domains rated Low risk of bias."

    return "no_information", "Insufficient information for overall judgement."




