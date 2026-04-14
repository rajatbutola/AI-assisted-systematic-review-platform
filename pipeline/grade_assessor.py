# pipeline/grade_assessor.py
#
# GRADE (Grading of Recommendations Assessment, Development and Evaluation)
# evidence quality assessment.
#
# Reference: Guyatt et al., J Clin Epidemiol 2011;64(4):380-394
#            https://doi.org/10.1016/j.jclinepi.2010.09.012
#
# GRADE operates at the BODY OF EVIDENCE level (across all included studies
# for a specific outcome) — NOT at the individual study level.
#
# Architecture:
#   - Input: list of RoB2Assessment / NOSAssessment objects + pooled data
#   - LLM assists with indirectness and publication bias judgements
#   - Five downgrade factors: Risk of bias, Inconsistency, Indirectness,
#     Imprecision, Publication bias
#   - Two upgrade factors: Large effect, Dose-response, Residual confounders
#   - Final certainty: High → Moderate → Low → Very Low
#   - Also generates a Summary of Findings (SoF) table row

import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

from core.llm import run_inference
from config.prompts import GRADE_PROMPT

logger = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────────────────────

CERTAINTY_LEVELS = ["high", "moderate", "low", "very_low"]

CERTAINTY_DISPLAY = {
    "high":      "High ⊕⊕⊕⊕",
    "moderate":  "Moderate ⊕⊕⊕◯",
    "low":       "Low ⊕⊕◯◯",
    "very_low":  "Very Low ⊕◯◯◯",
}

CERTAINTY_EMOJI = {
    "high":     "🟢",
    "moderate": "🟡",
    "low":      "🟠",
    "very_low": "🔴",
}

CERTAINTY_MEANING = {
    "high":     "Further research is very unlikely to change our confidence in the effect estimate.",
    "moderate": "Further research is likely to have an important impact on our confidence.",
    "low":      "Further research is very likely to have an important impact. The estimate may change.",
    "very_low": "We have very little confidence in the effect estimate.",
}


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class GRADEFactor:
    """One GRADE downgrade or upgrade factor."""
    name:       str
    direction:  str = "none"    # "downgrade" / "upgrade" / "none"
    levels:     int = 0         # how many levels to downgrade (1 or 2) or upgrade
    rationale:  str = ""


@dataclass
class GRADEAssessment:
    """
    GRADE certainty of evidence for one outcome across all included studies.
    """
    outcome:          str = "Primary outcome"
    study_design:     str = "rct"       # "rct" or "observational"
    n_studies:        int = 0
    n_participants:   int = 0

    # Starting certainty (before adjustments)
    starting_certainty: str = "high"   # rct→high, observational→low

    # Five downgrade factors
    risk_of_bias:     GRADEFactor = field(default_factory=lambda: GRADEFactor("Risk of bias"))
    inconsistency:    GRADEFactor = field(default_factory=lambda: GRADEFactor("Inconsistency"))
    indirectness:     GRADEFactor = field(default_factory=lambda: GRADEFactor("Indirectness"))
    imprecision:      GRADEFactor = field(default_factory=lambda: GRADEFactor("Imprecision"))
    publication_bias: GRADEFactor = field(default_factory=lambda: GRADEFactor("Publication bias"))

    # Two upgrade factors (observational only)
    large_effect:     GRADEFactor = field(default_factory=lambda: GRADEFactor("Large effect"))
    dose_response:    GRADEFactor = field(default_factory=lambda: GRADEFactor("Dose-response"))

    # Final result
    final_certainty:  str = "moderate"
    certainty_rationale: str = ""

    # Summary of Findings fields
    relative_effect:  str = ""     # e.g. "HR 0.66 (95% CI 0.52–0.85)"
    absolute_effect:  str = ""     # e.g. "34 fewer per 1000"
    sof_notes:        str = ""

    @property
    def certainty_display(self) -> str:
        return CERTAINTY_DISPLAY.get(self.final_certainty, self.final_certainty)

    @property
    def certainty_emoji(self) -> str:
        return CERTAINTY_EMOJI.get(self.final_certainty, "⬜")

    @property
    def certainty_meaning(self) -> str:
        return CERTAINTY_MEANING.get(self.final_certainty, "")

    @property
    def all_factors(self) -> List[GRADEFactor]:
        return [self.risk_of_bias, self.inconsistency, self.indirectness,
                self.imprecision, self.publication_bias,
                self.large_effect, self.dose_response]

    def to_sof_row(self) -> Dict[str, str]:
        """Generate a Summary of Findings table row."""
        return {
            "Outcome":          self.outcome,
            "Studies (n)":      str(self.n_studies),
            "Participants":     str(self.n_participants) if self.n_participants else "NR",
            "Relative effect":  self.relative_effect or "NR",
            "Absolute effect":  self.absolute_effect or "NR",
            "Certainty":        self.certainty_display,
            "What happens":     self.sof_notes or self.certainty_meaning,
        }

    def to_export_dict(self) -> Dict[str, str]:
        d = self.to_sof_row()
        d["Certainty rationale"] = self.certainty_rationale
        for f in self.all_factors:
            if f.direction != "none":
                d[f"GRADE {f.name}"] = (
                    f"{'↓' * f.levels if f.direction == 'downgrade' else '↑'} {f.rationale}"
                )
        return d


# ── Main entry point ───────────────────────────────────────────────────────────

def assess_grade(
    rob2_assessments: List[Any],       # list of RoB2Assessment
    nos_assessments:  List[Any],       # list of NOSAssessment
    pooled_data:      List[Dict],      # from data_pooler extracted rows
    outcome:          str = "Primary outcome",
    study_design:     str = "rct",
) -> GRADEAssessment:
    """
    Compute GRADE certainty of evidence for a body of studies.

    Parameters
    ----------
    rob2_assessments : RoB2Assessment objects for RCTs
    nos_assessments  : NOSAssessment objects for observational studies
    pooled_data      : List of dicts from data_pooler (contains effect sizes, CIs)
    outcome          : Name of the outcome being graded
    study_design     : "rct" or "observational" (determines starting certainty)
    """
    grade = GRADEAssessment(
        outcome=outcome,
        study_design=study_design,
        n_studies=len(rob2_assessments) + len(nos_assessments),
        starting_certainty="high" if study_design == "rct" else "low",
    )
    grade.final_certainty = grade.starting_certainty

    # Count participants from pooled data
    grade.n_participants = _count_participants(pooled_data)

    # Extract effect sizes
    grade.relative_effect = _extract_effect(pooled_data)

    # ── Factor 1: Risk of bias ─────────────────────────────────────────────────
    grade.risk_of_bias = _assess_risk_of_bias_factor(
        rob2_assessments, nos_assessments
    )

    # ── Factor 2: Inconsistency (heterogeneity) ───────────────────────────────
    grade.inconsistency = _assess_inconsistency(pooled_data)

    # ── Factors 3-5 + upgrade factors: LLM-assisted ───────────────────────────
    # These require judgement about the review question context —
    # we use the LLM with a structured prompt
    llm_factors = _assess_with_llm(pooled_data, outcome, study_design)
    if llm_factors:
        grade.indirectness    = llm_factors.get("indirectness", grade.indirectness)
        grade.imprecision     = llm_factors.get("imprecision", grade.imprecision)
        grade.publication_bias= llm_factors.get("publication_bias", grade.publication_bias)
        grade.large_effect    = llm_factors.get("large_effect", grade.large_effect)
        grade.dose_response   = llm_factors.get("dose_response", grade.dose_response)

    # ── Compute final certainty ────────────────────────────────────────────────
    grade.final_certainty, grade.certainty_rationale = _compute_certainty(grade)

    return grade


# ── Factor assessors ───────────────────────────────────────────────────────────

def _assess_risk_of_bias_factor(rob2_list, nos_list) -> GRADEFactor:
    """
    Downgrade if majority of studies have serious/critical risk of bias.
    RoB 2: High = serious, Some concerns = moderate
    NOS: Poor = serious, Fair = moderate
    """
    if not rob2_list and not nos_list:
        return GRADEFactor("Risk of bias", "none", 0,
                           "No individual study quality data available.")

    n_high, n_some, n_total = 0, 0, 0

    for rob in rob2_list:
        n_total += 1
        if rob.overall == "high":
            n_high += 1
        elif rob.overall == "some_concerns":
            n_some += 1

    for nos in nos_list:
        n_total += 1
        if nos.quality_grade == "poor":
            n_high += 1
        elif nos.quality_grade == "fair":
            n_some += 1

    pct_high = n_high / n_total if n_total else 0
    pct_some = n_some / n_total if n_total else 0

    if pct_high > 0.5:
        return GRADEFactor("Risk of bias", "downgrade", 2,
                           f"{n_high}/{n_total} studies have serious risk of bias → downgrade 2 levels")
    if pct_high > 0.2 or pct_some > 0.5:
        return GRADEFactor("Risk of bias", "downgrade", 1,
                           f"{n_high} high / {n_some} some-concerns of {n_total} studies → downgrade 1 level")

    return GRADEFactor("Risk of bias", "none", 0,
                       f"Most studies ({n_total - n_high - n_some}/{n_total}) low risk → no downgrade")


def _assess_inconsistency(pooled_data: List[Dict]) -> GRADEFactor:
    """
    Downgrade if substantial heterogeneity exists.
    Uses I² proxy: look for heterogeneity language in pooled data results.
    (Full I² calculation requires individual study data — approximated here)
    """
    if len(pooled_data) < 2:
        return GRADEFactor("Inconsistency", "none", 0,
                           f"Only {len(pooled_data)} study/studies — inconsistency not applicable")

    # Look for I² or heterogeneity mentions in the pooled results
    all_text = " ".join([
        str(row.get("Primary Result", "")) + " " + str(row.get("Study Design", ""))
        for row in pooled_data
    ]).lower()

    if any(p in all_text for p in ["i² >75", "i²=8", "high heterogeneity",
                                    "substantial heterogeneity"]):
        return GRADEFactor("Inconsistency", "downgrade", 2,
                           "High heterogeneity detected (I²>75%) → downgrade 2 levels")

    if any(p in all_text for p in ["i² >50", "i²=5", "i²=6", "i²=7",
                                    "moderate heterogeneity"]):
        return GRADEFactor("Inconsistency", "downgrade", 1,
                           "Moderate heterogeneity (I²>50%) → downgrade 1 level")

    n = len(pooled_data)
    return GRADEFactor("Inconsistency", "none", 0,
                       f"{n} studies — no substantial heterogeneity detected")


def _assess_with_llm(pooled_data: List[Dict],
                     outcome: str,
                     study_design: str) -> Optional[Dict[str, GRADEFactor]]:
    """Use LLM to assess indirectness, imprecision, pub bias, large effect."""
    if not pooled_data:
        return None

    # Build a summary of the pooled data for the LLM
    summary_lines = []
    for row in pooled_data[:8]:   # limit to 8 studies to stay within tokens
        summary_lines.append(
            f"- {row.get('Title', 'Study')[:60]}: "
            f"N={row.get('Sample size', 'NR')}, "
            f"Result={row.get('Primary Result', 'NR')}, "
            f"Design={row.get('Study Design', 'NR')}"
        )

    try:
        prompt = GRADE_PROMPT.format(
            outcome=outcome,
            study_design=study_design,
            studies_summary="\n".join(summary_lines),
        )
        raw = run_inference(prompt, task="scoring")
        return _parse_grade_llm(raw)
    except Exception as e:
        logger.warning("GRADE LLM assessment failed: %s", e)
        return None


def _parse_grade_llm(text: str) -> Dict[str, GRADEFactor]:
    """Parse LLM output for GRADE factors."""
    import re
    factors = {}

    # Expected format:
    # INDIRECTNESS: none / downgrade_1 / downgrade_2
    # IMPRECISION: none / downgrade_1 / downgrade_2
    # PUBLICATION_BIAS: none / downgrade_1
    # LARGE_EFFECT: none / upgrade_1 / upgrade_2
    # DOSE_RESPONSE: none / upgrade_1

    key_map = {
        "INDIRECTNESS":     "indirectness",
        "IMPRECISION":      "imprecision",
        "PUBLICATION_BIAS": "publication_bias",
        "LARGE_EFFECT":     "large_effect",
        "DOSE_RESPONSE":    "dose_response",
    }

    for raw_key, attr in key_map.items():
        pattern = rf'{raw_key}\s*[:=]\s*(\w+)'
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            val = m.group(1).lower()
            if "downgrade_2" in val or "serious" in val:
                direction, levels = "downgrade", 2
            elif "downgrade" in val or "downgrade_1" in val:
                direction, levels = "downgrade", 1
            elif "upgrade_2" in val:
                direction, levels = "upgrade", 2
            elif "upgrade" in val:
                direction, levels = "upgrade", 1
            else:
                direction, levels = "none", 0

            # Extract rationale line
            rat_m = re.search(
                rf'{raw_key}_RATIONALE\s*[:=]\s*(.+)', text, re.IGNORECASE
            )
            rationale = rat_m.group(1).strip()[:200] if rat_m else val

            factors[attr] = GRADEFactor(
                name=attr.replace("_", " ").title(),
                direction=direction,
                levels=levels,
                rationale=rationale,
            )

    return factors


# ── Final certainty computation ────────────────────────────────────────────────

def _compute_certainty(grade: GRADEAssessment) -> tuple:
    """
    Apply all downgrade and upgrade factors to compute final certainty.
    Starting certainty: RCTs=High (index 0), Observational=Low (index 2).
    """
    idx = CERTAINTY_LEVELS.index(grade.starting_certainty)
    reasons = [f"Starting: {grade.starting_certainty.upper()} "
               f"({'RCTs' if grade.study_design == 'rct' else 'Observational studies'})"]

    for factor in grade.all_factors:
        if factor.direction == "downgrade" and factor.levels > 0:
            old_idx = idx
            idx = min(idx + factor.levels, len(CERTAINTY_LEVELS) - 1)
            if idx > old_idx:
                reasons.append(f"↓ {factor.name}: {factor.rationale}")

        elif factor.direction == "upgrade" and factor.levels > 0:
            old_idx = idx
            idx = max(idx - factor.levels, 0)
            if idx < old_idx:
                reasons.append(f"↑ {factor.name}: {factor.rationale}")

    final = CERTAINTY_LEVELS[idx]
    return final, " | ".join(reasons)


# ── Helper extractors from pooled data ────────────────────────────────────────

def _count_participants(pooled_data: List[Dict]) -> int:
    import re
    total = 0
    for row in pooled_data:
        ss = str(row.get("Sample size", "") or row.get("sample_size", ""))
        nums = re.findall(r'\d+', ss)
        if nums:
            total += int(nums[0])
    return total


def _extract_effect(pooled_data: List[Dict]) -> str:
    """Extract the most representative effect measure from pooled data."""
    effects = []
    for row in pooled_data:
        result = str(row.get("Primary Result", "") or "").strip()
        if result and result != "NR" and len(result) > 2:
            effects.append(result[:80])
    if not effects:
        return "Not reported"
    # Return the most common / first effect
    return effects[0] if len(effects) == 1 else f"{effects[0]} (and {len(effects)-1} others)"