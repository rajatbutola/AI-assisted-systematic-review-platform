# pipeline/study_classifier.py
#
# Classifies research articles by study design.
# This is the FIRST step in the RoB 2 / NOS / GRADE pipeline —
# all downstream tools depend on knowing the study design.
#
# Design philosophy:
#   1. Rule-based detection first (fast, deterministic, high precision)
#      Uses PubMed publication type tags when available, then
#      title/abstract keyword patterns with confidence scores.
#   2. LLM fallback only when rules are ambiguous
#      (abstract contains conflicting signals or is unclear)
#   3. Returns a StudyClassification with design type, confidence,
#      and the signals that drove the decision — fully auditable.
#
# Study design taxonomy (maps to RoB 2 / NOS tool selection):
#   RCT              → RoB 2
#   QUASI_EXP        → RoB 2 (modified, "some concerns" on Domain 1)
#   COHORT           → NOS cohort version
#   CASE_CONTROL     → NOS case-control version
#   CROSS_SECTIONAL  → NOS-xs2
#   CASE_SERIES      → descriptive only, no formal RoB tool
#   REVIEW           → not assessed (SR/MA of other studies)
#   OTHER            → not assessed

import re
import logging
from dataclasses import dataclass, field
from typing import Optional, List

from core.llm import run_inference
from config.prompts import STUDY_CLASSIFIER_PROMPT

logger = logging.getLogger(__name__)


# ── Study design constants ─────────────────────────────────────────────────────

class StudyDesign:
    RCT             = "rct"
    QUASI_EXP       = "quasi_experimental"
    COHORT          = "cohort"
    CASE_CONTROL    = "case_control"
    CROSS_SECTIONAL = "cross_sectional"
    CASE_SERIES     = "case_series"
    REVIEW          = "review"
    OTHER           = "other"

    # Which RoB tool to apply
    ROB_TOOL = {
        RCT:             "rob2",
        QUASI_EXP:       "rob2",
        COHORT:          "nos_cohort",
        CASE_CONTROL:    "nos_case_control",
        CROSS_SECTIONAL: "nos_cross_sectional",
        CASE_SERIES:     None,
        REVIEW:          None,
        OTHER:           None,
    }

    DISPLAY = {
        RCT:             "RCT",
        QUASI_EXP:       "Quasi-experimental",
        COHORT:          "Cohort study",
        CASE_CONTROL:    "Case-control study",
        CROSS_SECTIONAL: "Cross-sectional study",
        CASE_SERIES:     "Case series / report",
        REVIEW:          "Systematic review / Meta-analysis",
        OTHER:           "Other / unclear",
    }


@dataclass
class StudyClassification:
    design:     str   = StudyDesign.OTHER
    confidence: str   = "low"       # high / moderate / low
    method:     str   = "rule"      # rule / llm
    signals:    List[str] = field(default_factory=list)  # audit trail
    rob_tool:   Optional[str] = None  # which tool to apply downstream

    @property
    def display_design(self) -> str:
        return StudyDesign.DISPLAY.get(self.design, self.design)

    @property
    def emoji(self) -> str:
        return {
            StudyDesign.RCT:             "🎲",
            StudyDesign.QUASI_EXP:       "🔬",
            StudyDesign.COHORT:          "👥",
            StudyDesign.CASE_CONTROL:    "↔️",
            StudyDesign.CROSS_SECTIONAL: "📸",
            StudyDesign.CASE_SERIES:     "📋",
            StudyDesign.REVIEW:          "📚",
            StudyDesign.OTHER:           "❓",
        }.get(self.design, "❓")


# ── Rule-based detection patterns ─────────────────────────────────────────────
# Ordered by specificity — more specific patterns first.
# Each pattern has a confidence level (high/moderate).

_RULES = [
    # ── RCT (strongest signals first) ─────────────────────────────────────────
    (StudyDesign.RCT, "high", [
        r"\brandomis[e]?d\s+(?:controlled\s+)?trial\b",
        r"\bRCT\b",
        r"\bplacebo[- ]controlled\b",
        r"\brandomly\s+(?:assigned|allocated|divided)\b",
        r"\bdouble[- ]blind(?:ed)?\b",
        r"\bsingle[- ]blind(?:ed)?\b",
        r"\bopen[- ]label\s+randomi[sz]",
        r"\bNCT\d{8}\b",                         # ClinicalTrials.gov ID
        r"\bEUDRACT\s*\d{4}",                    # EU trial registry
        r"\bISRCTN\d+\b",                        # ISRCTN registry
    ]),
    # ── Quasi-experimental ────────────────────────────────────────────────────
    (StudyDesign.QUASI_EXP, "moderate", [
        r"\bquasi[- ](?:experimental|randomis[e]?d)\b",
        r"\bnon[- ]randomi[sz]ed\s+(?:controlled\s+)?trial\b",
        r"\bsingle[- ]arm\s+(?:trial|study)\b",
        r"\bbefore[- ]and[- ]after\s+study\b",
        r"\bpre[- ]post\s+(?:study|design)\b",
    ]),
    # ── Systematic review / meta-analysis ─────────────────────────────────────
    (StudyDesign.REVIEW, "high", [
        r"\bsystematic\s+review\b",
        r"\bmeta[- ]analysis\b",
        r"\bpooled\s+analysis\b",
        r"\bscoping\s+review\b",
        r"\bnarrative\s+review\b",
        r"\bindividual\s+patient\s+data\s+meta[- ]analysis\b",
    ]),
    # ── Cohort ────────────────────────────────────────────────────────────────
    (StudyDesign.COHORT, "high", [
        r"\bprospective\s+cohort\b",
        r"\bretrospective\s+cohort\b",
        r"\blongitudinal\s+(?:cohort\s+)?study\b",
        r"\bfollowed\s+(?:prospectively|for\s+\d+\s+(?:months?|years?))\b",
        r"\bincidence\s+(?:rate|ratio|cohort)\b",
    ]),
    (StudyDesign.COHORT, "moderate", [
        r"\bcohort\s+study\b",
        r"\bcohort\b",
        r"\bobservational\s+(?:prospective|longitudinal)\b",
    ]),
    # ── Case-control ──────────────────────────────────────────────────────────
    (StudyDesign.CASE_CONTROL, "high", [
        r"\bcase[- ]control\s+study\b",
        r"\bmatched\s+controls?\b",
        r"\bcases\s+(?:and|vs\.?)\s+controls?\b",
        r"\bodds\s+ratio\b",
    ]),
    (StudyDesign.CASE_CONTROL, "moderate", [
        r"\bcase[- ]control\b",
    ]),
    # ── Cross-sectional ───────────────────────────────────────────────────────
    (StudyDesign.CROSS_SECTIONAL, "high", [
        r"\bcross[- ]sectional\s+study\b",
        r"\bprevalence\s+study\b",
        r"\bsurvey\s+(?:study|design|of)\b",
    ]),
    (StudyDesign.CROSS_SECTIONAL, "moderate", [
        r"\bcross[- ]sectional\b",
        r"\bpoint[- ]in[- ]time\b",
    ]),
    # ── Case series / report ──────────────────────────────────────────────────
    (StudyDesign.CASE_SERIES, "high", [
        r"\bcase\s+report\b",
        r"\bcase\s+series\b",
        r"\bsingle\s+case\b",
        r"\bn\s*=\s*[1-5]\b",    # very small N suggests case series
    ]),
]


def classify_study(abstract: str,
                   title: str = "",
                   pub_types: Optional[List[str]] = None) -> StudyClassification:
    """
    Classify a study's design using rules then LLM fallback.

    Parameters
    ----------
    abstract  : Full abstract text
    title     : Article title (used as additional signal)
    pub_types : PubMed publication type tags e.g. ["Randomized Controlled Trial"]
    """
    if not abstract and not title:
        return StudyClassification(
            design=StudyDesign.OTHER,
            confidence="low",
            method="rule",
            signals=["No abstract or title available"],
        )

    # ── Step 1: PubMed publication type tags (highest precision) ──────────────
    if pub_types:
        pt_result = _classify_from_pub_types(pub_types)
        if pt_result:
            return pt_result

    # ── Step 2: Rule-based keyword matching ───────────────────────────────────
    text = f"{title} {abstract}".lower()
    rule_result = _classify_from_rules(text)
    if rule_result and rule_result.confidence == "high":
        return rule_result

    # ── Step 3: LLM fallback (when rules are ambiguous or low-confidence) ─────
    try:
        llm_result = _classify_with_llm(abstract, title)
        if llm_result:
            # If rules found something but with moderate confidence,
            # use the more specific result
            if rule_result and rule_result.design != StudyDesign.OTHER:
                if llm_result.design == rule_result.design:
                    # Both agree — boost confidence
                    rule_result.confidence = "high"
                    rule_result.signals.append(f"LLM confirms: {llm_result.design}")
                    return rule_result
                else:
                    # LLM disagrees — prefer LLM for ambiguous cases
                    llm_result.signals.append(
                        f"Rule found '{rule_result.design}' but LLM overrides"
                    )
                    return llm_result
            return llm_result
    except Exception as e:
        logger.warning("LLM classifier failed: %s", e)

    # Return rule result even if moderate, or OTHER
    return rule_result or StudyClassification(
        design=StudyDesign.OTHER,
        confidence="low",
        method="rule",
        signals=["No clear signals found"],
    )


def _classify_from_pub_types(pub_types: List[str]) -> Optional[StudyClassification]:
    """Map PubMed publication type tags to study design."""
    pt_lower = [p.lower() for p in pub_types]
    signals  = [f"PubType: {p}" for p in pub_types]

    if any("randomized controlled trial" in p or
           "randomised controlled trial" in p for p in pt_lower):
        d = StudyDesign.RCT
        return StudyClassification(design=d, confidence="high",
                                   method="rule", signals=signals,
                                   rob_tool=StudyDesign.ROB_TOOL[d])

    if any("clinical trial" in p for p in pt_lower):
        d = StudyDesign.QUASI_EXP
        return StudyClassification(design=d, confidence="moderate",
                                   method="rule", signals=signals,
                                   rob_tool=StudyDesign.ROB_TOOL[d])

    if any("meta-analysis" in p or "systematic review" in p for p in pt_lower):
        d = StudyDesign.REVIEW
        return StudyClassification(design=d, confidence="high",
                                   method="rule", signals=signals,
                                   rob_tool=StudyDesign.ROB_TOOL[d])

    if any("observational study" in p for p in pt_lower):
        # Observational but need text to distinguish cohort/case-control/cross-sectional
        return None   # fall through to text rules

    if any("case reports" in p for p in pt_lower):
        d = StudyDesign.CASE_SERIES
        return StudyClassification(design=d, confidence="high",
                                   method="rule", signals=signals,
                                   rob_tool=StudyDesign.ROB_TOOL[d])

    return None


def _classify_from_rules(text: str) -> Optional[StudyClassification]:
    """Apply regex pattern rules to combined title+abstract text."""
    for design, confidence, patterns in _RULES:
        matched = []
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                matched.append(m.group(0))
        if matched:
            return StudyClassification(
                design=design,
                confidence=confidence,
                method="rule",
                signals=[f"Pattern match: '{s}'" for s in matched[:3]],
                rob_tool=StudyDesign.ROB_TOOL[design],
            )
    return None


def _classify_with_llm(abstract: str, title: str) -> Optional[StudyClassification]:
    """Use LLM to classify study design when rules are insufficient."""
    prompt = STUDY_CLASSIFIER_PROMPT.format(
        title=title or "Not provided",
        abstract=abstract[:2000],   # truncate to save tokens
    )
    raw = run_inference(prompt, task="scoring")
    return _parse_llm_classification(raw)


def _parse_llm_classification(text: str) -> Optional[StudyClassification]:
    """Parse LLM output into a StudyClassification."""
    design_map = {
        "rct":              StudyDesign.RCT,
        "randomised":       StudyDesign.RCT,
        "randomized":       StudyDesign.RCT,
        "quasi":            StudyDesign.QUASI_EXP,
        "cohort":           StudyDesign.COHORT,
        "case-control":     StudyDesign.CASE_CONTROL,
        "case_control":     StudyDesign.CASE_CONTROL,
        "cross-sectional":  StudyDesign.CROSS_SECTIONAL,
        "cross_sectional":  StudyDesign.CROSS_SECTIONAL,
        "case series":      StudyDesign.CASE_SERIES,
        "case_series":      StudyDesign.CASE_SERIES,
        "review":           StudyDesign.REVIEW,
        "meta-analysis":    StudyDesign.REVIEW,
        "other":            StudyDesign.OTHER,
    }
    text_lower = text.lower()
    for key, design in design_map.items():
        if key in text_lower:
            return StudyClassification(
                design=design,
                confidence="moderate",
                method="llm",
                signals=[f"LLM output: {text[:100].strip()}"],
                rob_tool=StudyDesign.ROB_TOOL[design],
            )
    return None





