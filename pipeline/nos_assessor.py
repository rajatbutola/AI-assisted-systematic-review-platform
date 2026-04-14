# pipeline/nos_assessor.py
#
# Newcastle-Ottawa Scale (NOS) implementation for observational studies.
#
# References:
#   Cohort/Case-control: Wells et al., University of Ottawa
#   Cross-sectional: Herzog et al., J Clin Epidemiol 2023 (NOS-xs2)
#
# Three separate instruments, selected based on study design:
#   nos_cohort          — prospective and retrospective cohort studies
#   nos_case_control    — case-control studies
#   nos_cross_sectional — cross-sectional studies (NOS-xs2)
#
# Architecture: same as RoB 2 — LLM answers signalling questions,
# deterministic algorithm assigns stars, deterministic algorithm
# computes overall quality grade.
#
# Star system:
#   Cohort/Case-control: max 9 stars
#     ≥7 = Good, 5-6 = Fair, <5 = Poor
#   Cross-sectional (NOS-xs2): max 10 stars
#     ≥8 = Good, 5-7 = Fair, <5 = Poor

import re
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List

from core.llm import run_inference
from config.prompts import NOS_COHORT_PROMPT, NOS_CASE_CONTROL_PROMPT, NOS_CROSS_SECTIONAL_PROMPT

logger = logging.getLogger(__name__)


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class NOSDomain:
    name:      str
    stars:     int = 0
    max_stars: int = 0
    items:     List[str] = field(default_factory=list)   # awarded items
    rationale: str = ""


@dataclass
class NOSAssessment:
    instrument:    str = ""     # "cohort" / "case_control" / "cross_sectional"
    selection:     NOSDomain = field(default_factory=lambda: NOSDomain("Selection"))
    comparability: NOSDomain = field(default_factory=lambda: NOSDomain("Comparability"))
    outcome:       NOSDomain = field(default_factory=lambda: NOSDomain("Outcome / Exposure"))
    total_stars:   int = 0
    max_stars:     int = 9
    quality_grade: str = "poor"   # good / fair / poor
    notes:         str = ""

    @property
    def quality_emoji(self) -> str:
        return {"good": "🟢", "fair": "🟡", "poor": "🔴"}.get(self.quality_grade, "⬜")

    @property
    def quality_label(self) -> str:
        return self.quality_grade.capitalize()

    @property
    def star_display(self) -> str:
        return "⭐" * self.total_stars + "☆" * (self.max_stars - self.total_stars)

    def to_export_dict(self) -> dict:
        return {
            "NOS Instrument":   self.instrument.replace("_", " ").title(),
            "Selection Stars":  f"{self.selection.stars}/{self.selection.max_stars}",
            "Comparability Stars": f"{self.comparability.stars}/{self.comparability.max_stars}",
            "Outcome Stars":    f"{self.outcome.stars}/{self.outcome.max_stars}",
            "Total Stars":      f"{self.total_stars}/{self.max_stars}",
            "Quality Grade":    self.quality_label,
            "NOS Notes":        self.notes,
        }


# ── Main entry points ──────────────────────────────────────────────────────────

def assess_nos(abstract: str,
               instrument: str,
               full_text: str = "") -> NOSAssessment:
    """
    Run NOS assessment.

    Parameters
    ----------
    abstract   : Article abstract
    instrument : "nos_cohort" | "nos_case_control" | "nos_cross_sectional"
    full_text  : Optional full text for richer assessment
    """
    if not abstract or not abstract.strip():
        return NOSAssessment(
            instrument=instrument,
            notes="No abstract available for assessment.",
        )

    text = (full_text[:3000] if full_text else abstract[:2000])

    try:
        if instrument == "nos_cohort":
            return _assess_cohort(text)
        elif instrument == "nos_case_control":
            return _assess_case_control(text)
        elif instrument == "nos_cross_sectional":
            return _assess_cross_sectional(text)
        else:
            return NOSAssessment(instrument=instrument,
                                 notes=f"Unknown NOS instrument: {instrument}")
    except Exception as e:
        logger.error("NOS assessment failed (%s): %s", instrument, e)
        return NOSAssessment(instrument=instrument, notes=f"Assessment error: {e}")


# ── Cohort NOS ─────────────────────────────────────────────────────────────────

def _assess_cohort(text: str) -> NOSAssessment:
    """
    NOS Cohort version.
    Selection (4 stars) + Comparability (2 stars) + Outcome (3 stars) = 9 max
    """
    prompt = NOS_COHORT_PROMPT.format(text=text)
    raw    = run_inference(prompt, task="scoring")
    a      = _extract_nos_answers(raw)
    logger.debug("NOS cohort answers: %s", a)

    # ── Selection domain (max 4 stars) ────────────────────────────────────────
    sel_items, sel_stars = [], 0

    # Item 1: Representativeness of exposed cohort
    if a.get("C_REPRESENTATIVENESS", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Cohort representative of community")
    else:
        sel_items.append("☆ Cohort not representative")

    # Item 2: Selection of non-exposed cohort
    if a.get("C_NON_EXPOSED_SOURCE", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Non-exposed from same community")
    else:
        sel_items.append("☆ Non-exposed from different source")

    # Item 3: Ascertainment of exposure
    if a.get("C_EXPOSURE_ASCERTAINMENT", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Exposure from secure records/structured interview")
    else:
        sel_items.append("☆ Exposure self-reported only")

    # Item 4: Outcome not present at start
    if a.get("C_OUTCOME_NOT_PRESENT", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Outcome not present at study start")
    else:
        sel_items.append("☆ Outcome presence at start unclear")

    selection = NOSDomain("Selection", sel_stars, 4, sel_items)

    # ── Comparability domain (max 2 stars) ────────────────────────────────────
    comp_items, comp_stars = [], 0

    if a.get("C_COMPARABILITY_DESIGN", "N") in ("Y", "PY"):
        comp_stars += 1
        comp_items.append("★ Controlled for most important factor")
    else:
        comp_items.append("☆ Most important factor not controlled")

    if a.get("C_COMPARABILITY_ADDITIONAL", "N") in ("Y", "PY"):
        comp_stars += 1
        comp_items.append("★ Controlled for additional factors")
    else:
        comp_items.append("☆ Additional factors not controlled")

    comparability = NOSDomain("Comparability", comp_stars, 2, comp_items)

    # ── Outcome domain (max 3 stars) ──────────────────────────────────────────
    out_items, out_stars = [], 0

    if a.get("C_OUTCOME_ASSESSMENT", "N") in ("Y", "PY"):
        out_stars += 1
        out_items.append("★ Outcome from blind/medical record assessment")
    else:
        out_items.append("☆ Outcome self-reported or unblinded")

    if a.get("C_FOLLOWUP_LENGTH", "N") in ("Y", "PY"):
        out_stars += 1
        out_items.append("★ Follow-up long enough for outcomes")
    else:
        out_items.append("☆ Follow-up duration insufficient/unclear")

    if a.get("C_FOLLOWUP_COMPLETENESS", "N") in ("Y", "PY"):
        out_stars += 1
        out_items.append("★ Follow-up adequate (≥80%) or reasons given")
    else:
        out_items.append("☆ Follow-up <80% or not reported")

    outcome = NOSDomain("Outcome", out_stars, 3, out_items)

    total = sel_stars + comp_stars + out_stars
    grade = "good" if total >= 7 else "fair" if total >= 5 else "poor"

    return NOSAssessment(
        instrument="nos_cohort",
        selection=selection,
        comparability=comparability,
        outcome=outcome,
        total_stars=total,
        max_stars=9,
        quality_grade=grade,
    )


# ── Case-control NOS ───────────────────────────────────────────────────────────

def _assess_case_control(text: str) -> NOSAssessment:
    """
    NOS Case-Control version.
    Selection (4 stars) + Comparability (2 stars) + Exposure (3 stars) = 9 max
    """
    prompt = NOS_CASE_CONTROL_PROMPT.format(text=text)
    raw    = run_inference(prompt, task="scoring")
    a      = _extract_nos_answers(raw)
    logger.debug("NOS case-control answers: %s", a)

    # ── Selection (max 4 stars) ───────────────────────────────────────────────
    sel_items, sel_stars = [], 0

    if a.get("CC_CASE_DEFINITION", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Adequate case definition with independent validation")
    else:
        sel_items.append("☆ Case definition not validated independently")

    if a.get("CC_CASE_REPRESENTATIVENESS", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Cases representative / consecutive")
    else:
        sel_items.append("☆ Cases not representative")

    if a.get("CC_CONTROL_SELECTION", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Controls from same community as cases")
    else:
        sel_items.append("☆ Controls from different source")

    if a.get("CC_CONTROL_NO_DISEASE", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Absence of disease confirmed in controls")
    else:
        sel_items.append("☆ Disease absence in controls not confirmed")

    selection = NOSDomain("Selection", sel_stars, 4, sel_items)

    # ── Comparability (max 2 stars) ───────────────────────────────────────────
    comp_items, comp_stars = [], 0

    if a.get("CC_COMPARABILITY_DESIGN", "N") in ("Y", "PY"):
        comp_stars += 1
        comp_items.append("★ Cases/controls comparable on most important factor")
    else:
        comp_items.append("☆ Most important confounder not controlled")

    if a.get("CC_COMPARABILITY_ADDITIONAL", "N") in ("Y", "PY"):
        comp_stars += 1
        comp_items.append("★ Additional factors controlled")
    else:
        comp_items.append("☆ Additional confounders not controlled")

    comparability = NOSDomain("Comparability", comp_stars, 2, comp_items)

    # ── Exposure (max 3 stars) ────────────────────────────────────────────────
    exp_items, exp_stars = [], 0

    if a.get("CC_EXPOSURE_ASCERTAINMENT", "N") in ("Y", "PY"):
        exp_stars += 1
        exp_items.append("★ Exposure from secure records or blinded interview")
    else:
        exp_items.append("☆ Exposure not from secure records")

    if a.get("CC_SAME_METHOD", "N") in ("Y", "PY"):
        exp_stars += 1
        exp_items.append("★ Same method of ascertainment for cases and controls")
    else:
        exp_items.append("☆ Different methods for cases and controls")

    if a.get("CC_NON_RESPONSE", "N") in ("Y", "PY"):
        exp_stars += 1
        exp_items.append("★ Non-response rate similar / accounted for")
    else:
        exp_items.append("☆ Non-response not accounted for")

    outcome = NOSDomain("Exposure", exp_stars, 3, exp_items)

    total = sel_stars + comp_stars + exp_stars
    grade = "good" if total >= 7 else "fair" if total >= 5 else "poor"

    return NOSAssessment(
        instrument="nos_case_control",
        selection=selection,
        comparability=comparability,
        outcome=outcome,
        total_stars=total,
        max_stars=9,
        quality_grade=grade,
    )


# ── Cross-sectional NOS-xs2 ────────────────────────────────────────────────────

def _assess_cross_sectional(text: str) -> NOSAssessment:
    """
    NOS-xs2 Cross-Sectional version (Herzog et al. 2023).
    Selection (5 stars) + Comparability (2 stars) + Outcome (3 stars) = 10 max
    """
    prompt = NOS_CROSS_SECTIONAL_PROMPT.format(text=text)
    raw    = run_inference(prompt, task="scoring")
    a      = _extract_nos_answers(raw)
    logger.debug("NOS cross-sectional answers: %s", a)

    # ── Selection (max 5 stars) ───────────────────────────────────────────────
    sel_items, sel_stars = [], 0

    if a.get("XS_REPRESENTATIVENESS", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Sample representative of target population")
    else:
        sel_items.append("☆ Sample not representative")

    if a.get("XS_SAMPLE_SIZE", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Sample size justified")
    else:
        sel_items.append("☆ Sample size not justified")

    if a.get("XS_NON_RESPONDENTS", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Non-respondents described")
    else:
        sel_items.append("☆ Non-respondents not described")

    if a.get("XS_EXPOSURE_ASCERTAINMENT", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Exposure ascertained from validated records")
    else:
        sel_items.append("☆ Exposure self-reported only")

    if a.get("XS_SAME_TIMEFRAME", "N") in ("Y", "PY"):
        sel_stars += 1
        sel_items.append("★ Exposure and outcome from same timeframe")
    else:
        sel_items.append("☆ Exposure/outcome timeframe unclear")

    selection = NOSDomain("Selection", sel_stars, 5, sel_items)

    # ── Comparability (max 2 stars) ───────────────────────────────────────────
    comp_items, comp_stars = [], 0

    if a.get("XS_COMPARABILITY_DESIGN", "N") in ("Y", "PY"):
        comp_stars += 1
        comp_items.append("★ Controlled for most important confounders")
    else:
        comp_items.append("☆ Most important confounder not controlled")

    if a.get("XS_COMPARABILITY_ADDITIONAL", "N") in ("Y", "PY"):
        comp_stars += 1
        comp_items.append("★ Additional confounders controlled")
    else:
        comp_items.append("☆ Additional confounders not controlled")

    comparability = NOSDomain("Comparability", comp_stars, 2, comp_items)

    # ── Outcome (max 3 stars) ─────────────────────────────────────────────────
    out_items, out_stars = [], 0

    if a.get("XS_OUTCOME_ASSESSMENT", "N") in ("Y", "PY"):
        out_stars += 1
        out_items.append("★ Outcome assessed by independent/blinded assessment")
    else:
        out_items.append("☆ Outcome self-reported or unvalidated")

    if a.get("XS_STATISTICAL_TEST", "N") in ("Y", "PY"):
        out_stars += 1
        out_items.append("★ Appropriate statistical test used")
    else:
        out_items.append("☆ Statistical test not appropriate or not reported")

    if a.get("XS_RESPONSE_RATE", "N") in ("Y", "PY"):
        out_stars += 1
        out_items.append("★ Response rate ≥70% or non-response analysis")
    else:
        out_items.append("☆ Response rate <70% or not reported")

    outcome = NOSDomain("Outcome", out_stars, 3, out_items)

    total = sel_stars + comp_stars + out_stars
    grade = "good" if total >= 8 else "fair" if total >= 5 else "poor"

    return NOSAssessment(
        instrument="nos_cross_sectional",
        selection=selection,
        comparability=comparability,
        outcome=outcome,
        total_stars=total,
        max_stars=10,
        quality_grade=grade,
    )


# ── Answer parser ──────────────────────────────────────────────────────────────

def _extract_nos_answers(text: str) -> Dict[str, str]:
    """
    Extract KEY: Y/N/PY/PN/NI answers from LLM output.
    Handles both prefixed keys (C_*, CC_*, XS_*) and plain keys.
    """
    answers = {}
    pattern = re.compile(
        r'([A-Z][A-Z0-9_]+)\s*[:=]\s*(Y|PY|PN|N|NI)\b',
        re.IGNORECASE
    )
    for m in pattern.finditer(text):
        key = m.group(1).upper()
        val = m.group(2).upper()
        answers[key] = val
    return answers




