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





# pipeline/grade_assessor.py 28th April
#
# GRADE (Grading of Recommendations Assessment, Development and Evaluation)
#
# References:
#   Guyatt et al., J Clin Epidemiol 2011;64(4):380-394 (original framework)
#   Schünemann et al., BMJ 2024/2025 GRADE series (BMJ Core GRADE 2025):
#     - OIS-based imprecision (not just CI width)
#     - Prediction interval for inconsistency alongside I²
#     - 4-domain indirectness (population, intervention, comparator, outcome)
#     - Explicit confounding upgrade criteria for observational studies
#     - Risk difference as primary imprecision anchor for binary outcomes
#
# Architecture:
#   assess_grade() → separate call per study design group (RCT / Observational)
#   Each group produces one GRADEAssessment → one SoF table row
#   Combined SoF table rendered in ai_analysis_panel.py

import logging
import re
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

# BMJ 2025: minimum OIS thresholds for binary and continuous outcomes
# OIS = Optimal Information Size. If total participants < OIS → imprecision.
BMJ2025_OIS_BINARY     = 300   # minimum participants for binary outcome (RD-based)
BMJ2025_OIS_CONTINUOUS = 400   # minimum participants for continuous outcome
BMJ2025_LARGE_EFFECT_RR_THRESHOLD = 2.0   # RR >2 or <0.5 → large effect upgrade


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class GRADEFactor:
    """One GRADE downgrade or upgrade factor."""
    name:      str
    direction: str = "none"   # "downgrade" / "upgrade" / "none"
    levels:    int = 0
    rationale: str = ""


@dataclass
class GRADEAssessment:
    """GRADE certainty of evidence for one outcome in one study-design group."""
    outcome:            str = "Primary outcome"
    study_design:       str = "rct"
    n_studies:          int = 0
    n_participants:     int = 0
    relative_effect:    str = "Not reported"
    starting_certainty: str = "high"
    final_certainty:    str = "high"
    certainty_rationale:str = ""
    certainty_meaning:  str = ""

    # ── Five downgrade factors (GRADE 2011 + BMJ 2025 updates) ────────────────
    risk_of_bias:    GRADEFactor = field(default_factory=lambda: GRADEFactor("Risk of bias"))
    inconsistency:   GRADEFactor = field(default_factory=lambda: GRADEFactor("Inconsistency"))
    indirectness:    GRADEFactor = field(default_factory=lambda: GRADEFactor("Indirectness"))
    imprecision:     GRADEFactor = field(default_factory=lambda: GRADEFactor("Imprecision"))
    publication_bias:GRADEFactor = field(default_factory=lambda: GRADEFactor("Publication bias"))

    # ── Two upgrade factors ────────────────────────────────────────────────────
    large_effect:    GRADEFactor = field(default_factory=lambda: GRADEFactor("Large effect"))
    dose_response:   GRADEFactor = field(default_factory=lambda: GRADEFactor("Dose-response"))
    # BMJ 2025: residual confounding upgrade (observational only)
    residual_confounding: GRADEFactor = field(
        default_factory=lambda: GRADEFactor("Residual confounding"))

    @property
    def all_factors(self) -> List[GRADEFactor]:
        return [
            self.risk_of_bias, self.inconsistency,
            self.indirectness, self.imprecision, self.publication_bias,
            self.large_effect, self.dose_response, self.residual_confounding,
        ]

    def to_sof_row(self) -> Dict[str, str]:
        """
        Summary of Findings table row — matches BMJ Core GRADE 2025 format.
        Columns match the example table provided by the user.
        """
        def _fmt(factor: GRADEFactor) -> str:
            if factor.direction == "downgrade":
                return "⬇ Serious" if factor.levels == 1 else "⬇⬇ Very serious"
            if factor.direction == "upgrade":
                return "⬆ Upgrade"
            return "Not serious"

        # Format certainty with symbols
        certainty_str = CERTAINTY_DISPLAY.get(self.final_certainty, self.final_certainty).upper()

        return {
            "Outcome":           self.outcome,
            "Study design":      "RCTs" if self.study_design == "rct" else "Observational",
            "No. of studies (N)":f"{self.n_studies} (n = {self.n_participants or 'NR'})",
            "Effect estimate":   self.relative_effect,
            "Risk of bias":      _fmt(self.risk_of_bias),
            "Inconsistency":     _fmt(self.inconsistency),
            "Indirectness":      _fmt(self.indirectness),
            "Imprecision":       _fmt(self.imprecision),
            "Publication bias":  _fmt(self.publication_bias),
            "Certainty":         certainty_str,
        }

    def to_export_dict(self) -> Dict[str, str]:
        d = self.to_sof_row()
        d["Certainty rationale"] = self.certainty_rationale
        for f in self.all_factors:
            if f.direction != "none":
                d[f"GRADE {f.name}"] = (
                    f"{'↓'*f.levels if f.direction=='downgrade' else '↑'} {f.rationale}"
                )
        return d


# ── Main entry point ───────────────────────────────────────────────────────────

def assess_grade(
    rob2_assessments: List[Any],
    nos_assessments:  List[Any],
    pooled_data:      List[Dict],
    outcome:          str = "Primary outcome",
    study_design:     str = "rct",
) -> GRADEAssessment:
    """
    Compute GRADE certainty for one body of evidence (one study design group).
    Call separately for RCTs and observational studies.

    BMJ Core GRADE 2025 updates applied:
      - Imprecision: OIS threshold (≥300 participants for binary, ≥400 continuous)
                     + confidence interval crossing null
      - Inconsistency: prediction interval consideration alongside I²
      - Indirectness: assessed across 4 domains (population, intervention,
                      comparator, outcome) per Schünemann 2024
      - Residual confounding: upgrade factor for observational studies when
                              all plausible confounders would attenuate the effect
    """
    grade = GRADEAssessment(
        outcome=outcome,
        study_design=study_design,
        n_studies=len(rob2_assessments) + len(nos_assessments),
        starting_certainty="high" if study_design == "rct" else "low",
    )
    grade.final_certainty = grade.starting_certainty

    grade.n_participants  = _count_participants(pooled_data)
    grade.relative_effect = _extract_effect(pooled_data)

    # ── Factor 1: Risk of bias ─────────────────────────────────────────────────
    grade.risk_of_bias = _assess_risk_of_bias_factor(
        rob2_assessments, nos_assessments)

    # ── Factor 2: Inconsistency (BMJ 2025: I² + prediction interval) ──────────
    grade.inconsistency = _assess_inconsistency_bmj2025(pooled_data)

    # ── Factors 3-5 + upgrade factors: LLM-assisted ───────────────────────────
    llm_factors = _assess_with_llm(pooled_data, outcome, study_design)
    if llm_factors:
        grade.indirectness     = llm_factors.get("indirectness",     grade.indirectness)
        grade.publication_bias = llm_factors.get("publication_bias", grade.publication_bias)
        grade.large_effect     = llm_factors.get("large_effect",     grade.large_effect)
        grade.dose_response    = llm_factors.get("dose_response",    grade.dose_response)
        # BMJ 2025 observational upgrade: residual confounding
        if study_design == "observational":
            grade.residual_confounding = llm_factors.get(
                "residual_confounding", grade.residual_confounding)

    # ── Factor 4: Imprecision (BMJ 2025: OIS-based) ───────────────────────────
    # Overrides LLM imprecision with rule-based OIS check
    grade.imprecision = _assess_imprecision_bmj2025(
        pooled_data, grade.n_participants, grade.relative_effect)

    # ── Compute final certainty ────────────────────────────────────────────────
    grade.final_certainty, grade.certainty_rationale = _compute_certainty(grade)
    grade.certainty_meaning = CERTAINTY_MEANING.get(grade.final_certainty, "")

    return grade


# ── Factor assessors ───────────────────────────────────────────────────────────

def _assess_risk_of_bias_factor(rob2_list, nos_list) -> GRADEFactor:
    """
    Downgrade if majority of studies have serious/critical risk of bias.
    RoB 2: High = serious, Some concerns = moderate
    NOS:   Poor = serious, Fair = moderate
    """
    if not rob2_list and not nos_list:
        return GRADEFactor("Risk of bias", "none", 0,
                           "No individual study quality data available.")
    n_high, n_some, n_total = 0, 0, 0

    for rob in rob2_list:
        n_total += 1
        if rob.overall == "high":              n_high += 1
        elif rob.overall == "some_concerns":   n_some += 1

    for nos in nos_list:
        n_total += 1
        if nos.quality_grade == "poor":        n_high += 1
        elif nos.quality_grade == "fair":      n_some += 1

    pct_high = n_high / n_total if n_total else 0
    pct_some = n_some / n_total if n_total else 0

    if pct_high > 0.5:
        return GRADEFactor("Risk of bias", "downgrade", 2,
            f"{n_high}/{n_total} studies high/critical risk of bias → downgrade 2 levels")
    if pct_high > 0.2 or pct_some > 0.5:
        return GRADEFactor("Risk of bias", "downgrade", 1,
            f"{n_high} high / {n_some} some-concerns of {n_total} → downgrade 1 level")
    return GRADEFactor("Risk of bias", "none", 0,
        f"Most studies ({n_total-n_high-n_some}/{n_total}) low risk → no downgrade")


def _assess_inconsistency_bmj2025(pooled_data: List[Dict]) -> GRADEFactor:
    """
    BMJ 2025 update: assess inconsistency using I² AND prediction interval.
    A wide prediction interval (crossing null) indicates inconsistency even
    when I² is moderate, because it shows the true effect may vary substantially
    across settings.
    """
    if len(pooled_data) < 2:
        return GRADEFactor("Inconsistency", "none", 0,
            f"Only {len(pooled_data)} study — inconsistency not applicable (≥2 required)")

    all_text = " ".join([
        str(row.get("Primary Result", "")) + " " +
        str(row.get("Study Design", "")) + " " +
        str(row.get("Notes", ""))
        for row in pooled_data
    ]).lower()

    # High heterogeneity signals
    if any(p in all_text for p in
           ["i² >75", "i²=8", "i²=9", "high heterogeneity",
            "substantial heterogeneity", "prediction interval crosses"]):
        return GRADEFactor("Inconsistency", "downgrade", 2,
            "High heterogeneity (I²>75%) or prediction interval crosses null "
            "→ downgrade 2 levels (BMJ 2025)")

    # Moderate heterogeneity
    if any(p in all_text for p in
           ["i² >50", "i²=5", "i²=6", "i²=7", "moderate heterogeneity",
            "considerable variability", "wide prediction interval"]):
        return GRADEFactor("Inconsistency", "downgrade", 1,
            "Moderate heterogeneity (I²>50%) or wide prediction interval "
            "→ downgrade 1 level (BMJ 2025)")

    n = len(pooled_data)
    return GRADEFactor("Inconsistency", "none", 0,
        f"{n} studies — no substantial heterogeneity detected")


def _assess_imprecision_bmj2025(
    pooled_data: List[Dict],
    n_participants: int,
    relative_effect: str,
) -> GRADEFactor:
    """
    BMJ Core GRADE 2025: OIS-based imprecision assessment.

    Two criteria (downgrade if EITHER met):
      1. Total N < OIS threshold (300 for binary, 400 for continuous)
      2. Confidence interval crosses the null (or decision threshold)

    Risk difference is the primary anchor for binary outcomes per BMJ 2025.
    The CI check looks for common effect measure patterns in the effect string.
    """
    reasons = []
    levels  = 0

    # ── Criterion 1: OIS (Optimal Information Size) ────────────────────────────
    # Determine outcome type from effect string
    eff_lower = (relative_effect or "").lower()
    is_binary = any(m in eff_lower for m in ["rr", "or", "hr", "rd", "risk ratio",
                                              "odds ratio", "hazard ratio", "risk diff"])
    ois = BMJ2025_OIS_BINARY if is_binary else BMJ2025_OIS_CONTINUOUS

    if n_participants > 0 and n_participants < ois:
        reasons.append(
            f"Total N={n_participants} < OIS threshold ({ois}) "
            f"for {'binary' if is_binary else 'continuous'} outcome (BMJ 2025)"
        )
        levels = max(levels, 1)

    # ── Criterion 2: CI crossing null ─────────────────────────────────────────
    # Look for CI patterns like "0.65–1.20" where it includes 1.0 (for RR/OR/HR)
    # or includes 0 for RD/MD
    ci_cross_null = False
    # Pattern: extract lower and upper CI bounds
    ci_pattern = re.search(
        r'(?:ci|95%|0\.?\d*)\s*[:\s]\s*([\d.]+)\s*[–\-to]+\s*([\d.]+)',
        eff_lower
    )
    if ci_pattern:
        try:
            lo = float(ci_pattern.group(1))
            hi = float(ci_pattern.group(2))
            if is_binary and lo < 1.0 < hi:
                ci_cross_null = True
                reasons.append("95% CI crosses null (includes 1.0) → imprecision")
            elif not is_binary and lo < 0.0 < hi:
                ci_cross_null = True
                reasons.append("95% CI crosses null (includes 0) → imprecision")
        except (ValueError, AttributeError):
            pass

    # Also check text patterns
    if any(p in eff_lower for p in ["crosses null", "includes 1", "includes one",
                                     "not statistically significant", "p > 0.05",
                                     "wide confidence"]):
        ci_cross_null = True
        reasons.append("CI or statistical significance suggests imprecision")

    if ci_cross_null:
        levels = max(levels, 1)

    # Very serious: both OIS and CI cross null
    if n_participants > 0 and n_participants < ois and ci_cross_null:
        levels = 2
        reasons = [f"OIS not met (N={n_participants}<{ois}) AND CI crosses null "
                   f"→ very serious imprecision (BMJ 2025)"]

    if not reasons and len(pooled_data) > 0:
        reasons = [f"N={n_participants} meets OIS threshold; CI does not cross null"]

    if levels == 0:
        return GRADEFactor("Imprecision", "none", 0,
                           "; ".join(reasons) if reasons else "No imprecision detected")
    return GRADEFactor("Imprecision", "downgrade", levels, "; ".join(reasons))


def _assess_with_llm(pooled_data: List[Dict],
                     outcome: str,
                     study_design: str) -> Optional[Dict[str, GRADEFactor]]:
    """
    LLM-assisted GRADE for indirectness (4-domain, BMJ 2025),
    publication bias, large effect, dose-response, residual confounding.
    """
    if not pooled_data:
        return None

    summary_lines = []
    for row in pooled_data[:6]:
        summary_lines.append(
            f"- {row.get('Title','Study')[:50]}: "
            f"N={row.get('Sample size','NR')}, "
            f"Result={row.get('Primary Result','NR')}, "
            f"Design={row.get('Study Design','NR')}"
        )

    try:
        prompt = GRADE_PROMPT.format(
            outcome=outcome,
            study_design=study_design,
            studies_summary="\n".join(summary_lines),
        )
        raw = run_inference(prompt, task="scoring")
        return _parse_grade_llm_bmj2025(raw, study_design)
    except Exception as e:
        logger.warning("GRADE LLM assessment failed: %s", e)
        return None


def _parse_grade_llm_bmj2025(text: str, study_design: str) -> Dict[str, GRADEFactor]:
    """
    Parse LLM GRADE output. BMJ 2025 additions:
      - INDIRECTNESS now covers 4 domains: population, intervention,
        comparator, outcome (any serious domain → downgrade)
      - RESIDUAL_CONFOUNDING: upgrade for observational studies
    """
    factors = {}

    key_map = {
        "INDIRECTNESS":         "indirectness",
        "PUBLICATION_BIAS":     "publication_bias",
        "LARGE_EFFECT":         "large_effect",
        "DOSE_RESPONSE":        "dose_response",
        "RESIDUAL_CONFOUNDING": "residual_confounding",
    }

    # BMJ 2025: 4-domain indirectness check
    # If LLM flags indirectness in ANY domain → downgrade
    indir_domains = ["POPULATION_INDIRECTNESS", "INTERVENTION_INDIRECTNESS",
                     "COMPARATOR_INDIRECTNESS", "OUTCOME_INDIRECTNESS"]
    n_indir_domains = 0
    for domain in indir_domains:
        m = re.search(rf'{domain}\s*[:=]\s*(\w+)', text, re.IGNORECASE)
        if m and "downgrade" in m.group(1).lower():
            n_indir_domains += 1

    if n_indir_domains >= 2:
        factors["indirectness"] = GRADEFactor(
            "Indirectness", "downgrade", 2,
            f"{n_indir_domains}/4 PICO domains indirect → very serious indirectness (BMJ 2025)")
    elif n_indir_domains == 1:
        factors["indirectness"] = GRADEFactor(
            "Indirectness", "downgrade", 1,
            f"{n_indir_domains}/4 PICO domains indirect → serious indirectness (BMJ 2025)")

    # Parse remaining factors
    for raw_key, attr in key_map.items():
        if attr == "indirectness" and attr in factors:
            continue  # already handled by 4-domain logic above

        pattern = rf'{raw_key}\s*[:=]\s*(\w+(?:_\w+)?)'
        m = re.search(pattern, text, re.IGNORECASE)
        if not m:
            continue

        val = m.group(1).lower()
        if "downgrade_2" in val or "very_serious" in val or "very serious" in val:
            direction, levels = "downgrade", 2
        elif "downgrade" in val or "serious" in val:
            direction, levels = "downgrade", 1
        elif "upgrade_2" in val:
            direction, levels = "upgrade", 2
        elif "upgrade" in val:
            direction, levels = "upgrade", 1
        else:
            direction, levels = "none", 0

        rat_m = re.search(
            rf'{raw_key}_RATIONALE\s*[:=]\s*(.+)', text, re.IGNORECASE)
        rationale = rat_m.group(1).strip()[:200] if rat_m else val

        # Residual confounding is an UPGRADE for observational studies only
        if attr == "residual_confounding":
            if study_design != "observational":
                continue
            if direction == "downgrade":
                direction = "none"; levels = 0  # confounding only upgrades
            rationale = (
                "All plausible confounders would reduce the effect → "
                "upgrade 1 level (BMJ 2025 observational upgrade criterion)"
            ) if direction == "upgrade" else rationale

        if attr not in factors:
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
    Apply downgrade/upgrade factors to compute final certainty.
    Floors: RCTs cannot go above High; Observational cannot go above Moderate
    (unless upgraded — BMJ 2025 cap).
    Cannot go below Very Low.
    """
    idx = CERTAINTY_LEVELS.index(grade.starting_certainty)
    reasons = [
        f"Starting: {grade.starting_certainty.upper()} "
        f"({'RCTs' if grade.study_design=='rct' else 'Observational studies'})"
    ]

    for factor in grade.all_factors:
        if factor.direction == "downgrade" and factor.levels > 0:
            old = idx
            idx = min(idx + factor.levels, len(CERTAINTY_LEVELS) - 1)
            if idx > old:
                reasons.append(f"↓ {factor.name}: {factor.rationale}")

        elif factor.direction == "upgrade" and factor.levels > 0:
            old = idx
            # BMJ 2025: observational studies can upgrade at most to Moderate (idx=1)
            min_idx = 1 if grade.study_design == "observational" else 0
            idx = max(idx - factor.levels, min_idx)
            if idx < old:
                reasons.append(f"↑ {factor.name}: {factor.rationale}")

    final = CERTAINTY_LEVELS[idx]
    return final, " | ".join(reasons)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _count_participants(pooled_data: List[Dict]) -> int:
    total = 0
    for row in pooled_data:
        ss = str(row.get("Sample size", "") or row.get("sample_size", ""))
        nums = re.findall(r'\d+', ss)
        if nums:
            total += int(nums[0])
    return total


def _extract_effect(pooled_data: List[Dict]) -> str:
    effects = []
    for row in pooled_data:
        result = str(row.get("Primary Result", "") or "").strip()
        if result and result != "NR" and len(result) > 2:
            effects.append(result[:80])
    if not effects:
        return "Not reported"
    return effects[0] if len(effects) == 1 else f"{effects[0]} (and {len(effects)-1} others)"