# config/prompts.py 27th March


# config/prompts.py — v3
#
# config/prompts.py — v4  (FINAL) 30th March
#
# ROOT CAUSE FIXES:
#
# QUALITY: The previous prompt ended with a JSON template containing "?"
# as placeholder values. The model reproduced these literally, producing
# invalid JSON like {"randomisation_score": ?} which json.loads() rejects.
# Even when the model tried to fill it in, it embedded quote characters
# in key names producing {'"randomisation_score"': 2} — also invalid.
# FIX: Remove ALL JSON examples from the quality prompt. Use plain English
# instructions only. Ask the model to output ONLY 5 integers on 5 lines.
# Then parse those 5 integers directly — no JSON parsing needed at all.
# This is far more reliable for a quantised 8B model.
#
# DATA POOLING: max_tokens=350 causes the JSON to be cut mid-string.
# FIX: Raise max_tokens to 800 in settings.py. Also simplify the prompt
# to produce shorter values (just key facts, not full sentences).
#
# SUMMARIZATION: Working well — keep as-is.
# PICO: Working well — keep as-is.

# ── Summarization ──────────────────────────────────────────────────────────────
SUMMARIZATION_PROMPT = """Summarize the following research abstract in exactly 5 bullet points.

Example of correct output format:
• Study Design: Randomised controlled trial
• Population: 240 adults with type 2 diabetes, mean age 58 years
• Intervention: Metformin 1g twice daily for 12 months
• Key Findings: HbA1c reduced by 1.2% (p<0.001) vs placebo
• Conclusion: Metformin significantly improves glycaemic control in type 2 diabetes

Now summarize this abstract using the same format. Use ONLY facts from the abstract. Write "Not reported" if a field is missing.

Abstract:
{abstract}

Summary:
•"""


# ── PICO Extraction ────────────────────────────────────────────────────────────
PICO_EXTRACTION_PROMPT = """Extract PICO elements from the abstract below.

Return a single JSON object. No markdown, no explanation, no code fences.
Keys: population, intervention, comparison, outcome, study_design, sample_size

Abstract:
{abstract}

JSON:"""


# ── Quality Assessment ─────────────────────────────────────────────────────────
# CRITICAL FIX: Output plain integers, NOT JSON.
# JSON template examples in prompts cause the model to echo "?" or copy
# the template with embedded quotes, producing unparseable output.
# Plain integer output on separate lines is trivially reliable to parse.
QUALITY_ASSESSMENT_PROMPT = """You are assessing the methodological quality of a research study.
Read the abstract and give a score from 1 to 3 for each of the 5 domains below.

Domain 1 - Randomisation:
  Score 3 if the study is a randomised controlled trial (RCT)
  Score 2 if the study has a control group but is NOT randomised
  Score 1 if there is no control group at all

Domain 2 - Sample Size:
  Score 3 if the total number of patients is MORE than 100
  Score 2 if the total number of patients is between 30 and 100 (inclusive)
  Score 1 if the total number of patients is FEWER than 30

Domain 3 - Outcome Reporting:
  Score 3 if outcomes are objective and reported with statistics (p-values, confidence intervals, etc.)
  Score 2 if outcomes are a mix of objective and subjective
  Score 1 if outcomes are subjective only or not clearly reported

Domain 4 - Follow-up:
  Score 3 if follow-up is 3 months or longer and complete
  Score 2 if follow-up is shorter than 3 months or not clearly reported
  Score 1 if follow-up is not mentioned at all

Domain 5 - Comparator:
  Score 3 if there is a concurrent control group
  Score 2 if the comparison is to historical data only
  Score 1 if there is no comparator at all

Abstract:
{abstract}

Output exactly 5 lines, one score per line, in this exact format:
randomisation: <integer>
sample_size: <integer>
outcomes: <integer>
followup: <integer>
comparator: <integer>"""


# ── Data Pooling ───────────────────────────────────────────────────────────────
# Simplified to produce SHORT values to fit within token budget.
# Full sentences in JSON values caused truncation and parse errors.
DATA_POOLING_PROMPT = """Extract key data from this research abstract. Be brief — use short phrases, not full sentences.

Return a single JSON object. No markdown, no explanation.
Use "NR" (not reported) for missing fields. Keep all values SHORT (under 15 words each).

Abstract:
{abstract}

JSON with these exact keys:
sample_size, age_group, sex_distribution, condition_severity, intervention, comparator, follow_up_duration, primary_outcome, primary_outcome_result, events_intervention, events_comparator, secondary_outcomes, adverse_events, statistical_significance, study_design, country_setting

events_intervention: number of events in intervention arm, format "n/N (x%)" e.g. "19/266 (7.1%)". Use "NR" if not reported.
events_comparator: number of events in comparator arm, format "n/N (x%)" e.g. "44/359 (12.3%)". Use "NR" if not reported.

JSON:"""




# ══════════════════════════════════════════════════════════════════════════════
# STUDY DESIGN CLASSIFIER
# ══════════════════════════════════════════════════════════════════════════════

STUDY_CLASSIFIER_PROMPT = """Classify the study design from the title and abstract below.

Choose EXACTLY ONE from this list:
  rct                — Randomised controlled trial (any phase, any blinding)
  quasi_experimental — Non-randomised trial, before-after, single-arm trial
  cohort             — Prospective or retrospective cohort study
  case_control       — Case-control study
  cross_sectional    — Cross-sectional or prevalence study
  case_series        — Case series or case report
  review             — Systematic review or meta-analysis
  other              — Unclear or does not fit above

Title: {title}

Abstract: {abstract}

Output exactly one line in this format:
DESIGN: <one of the options above>"""


# ══════════════════════════════════════════════════════════════════════════════
# ROB 2 — Cochrane Risk of Bias Tool version 2
# ══════════════════════════════════════════════════════════════════════════════
# Signalling questions use a 5-point scale:
#   Y=Yes  PY=Probably Yes  PN=Probably No  N=No  NI=No Information
#
# Each line MUST follow the format: KEY: ANSWER
# Do NOT add explanations on the same line as the answer.
# Provide rationale on a separate line if needed.

ROB2_PROMPT = """You are a systematic review methodologist applying the Cochrane Risk of Bias tool (RoB 2) to a randomised controlled trial.

Read the text below and answer each signalling question using ONLY:
Y (Yes)  |  PY (Probably Yes)  |  PN (Probably No)  |  N (No)  |  NI (No Information)

DOMAIN 1 — Randomisation process:
D1_RANDOM_SEQUENCE: [Was the allocation sequence generated by a truly random method?]
D1_ALLOCATION_CONCEALMENT: [Was allocation concealed until participants were enrolled?]
D1_BASELINE_IMBALANCE: [Were there baseline imbalances suggesting a problem with randomisation? Answer Y if imbalance present (bad), N if not (good)]

DOMAIN 2 — Deviations from intended interventions:
D2_BLINDING_PARTICIPANTS: [Were participants blinded to their assigned intervention?]
D2_BLINDING_PROVIDERS: [Were care providers blinded to the assigned intervention?]
D2_PROTOCOL_DEVIATIONS: [Were there clinically important deviations from the intended intervention? Answer Y if deviations present (bad)]

DOMAIN 3 — Missing outcome data:
D3_MISSING_DATA_PROPORTION: [Was outcome data available for all or nearly all participants? Answer Y if complete/near-complete (good)]
D3_MISSING_DATA_RELATED: [Could missingness in outcome data be related to the true outcome? Answer Y if likely related (bad)]

DOMAIN 4 — Measurement of the outcome:
D4_BLINDED_ASSESSORS: [Were outcome assessors blinded to the intervention assignment?]
D4_OUTCOME_MEASUREMENT: [Was the outcome measurement method valid and reliable?]

DOMAIN 5 — Selection of the reported result:
D5_PREREGISTERED: [Was the trial pre-registered before data collection with outcomes specified?]
D5_OUTCOMES_REPORTED: [Are the reported outcomes consistent with the pre-registration or protocol?]

Study text:
{text}

Answer each question using the KEY: ANSWER format. One key per line. Use NI if the information is not in the text."""


# ══════════════════════════════════════════════════════════════════════════════
# NOS — Newcastle-Ottawa Scale (three versions)
# ══════════════════════════════════════════════════════════════════════════════

NOS_COHORT_PROMPT = """You are applying the Newcastle-Ottawa Scale (NOS) to a cohort study.

Answer each item Y (Yes/adequate) or N (No/inadequate) or NI (No Information).
Format: KEY: ANSWER — one per line.

SELECTION DOMAIN:
C_REPRESENTATIVENESS: [Is the exposed cohort truly representative of the average person in the community?]
C_NON_EXPOSED_SOURCE: [Was the non-exposed cohort drawn from the same community as the exposed cohort?]
C_EXPOSURE_ASCERTAINMENT: [Was exposure ascertained from secure records or structured interview (not self-report)?]
C_OUTCOME_NOT_PRESENT: [Was the outcome of interest demonstrated to be absent at the start of the study?]

COMPARABILITY DOMAIN:
C_COMPARABILITY_DESIGN: [Did the study control for the most important confounding factor in the design or analysis?]
C_COMPARABILITY_ADDITIONAL: [Did the study control for any additional confounding factors?]

OUTCOME DOMAIN:
C_OUTCOME_ASSESSMENT: [Was outcome assessed by independent blind assessment or from secure records?]
C_FOLLOWUP_LENGTH: [Was the follow-up period long enough for the outcome to occur?]
C_FOLLOWUP_COMPLETENESS: [Was the follow-up adequate (≥80%) or were reasons for loss described?]

Study text:
{text}

Answer using the KEY: ANSWER format. Use NI if the information is not in the text."""


NOS_CASE_CONTROL_PROMPT = """You are applying the Newcastle-Ottawa Scale (NOS) to a case-control study.

Answer each item Y or N or NI. Format: KEY: ANSWER — one per line.

SELECTION DOMAIN:
CC_CASE_DEFINITION: [Is there an adequate definition of cases with independent validation?]
CC_CASE_REPRESENTATIVENESS: [Are the cases representative of the average patient with the disease (consecutive or random)?]
CC_CONTROL_SELECTION: [Were controls drawn from the same community as the cases?]
CC_CONTROL_NO_DISEASE: [Was absence of the disease of interest confirmed in controls?]

COMPARABILITY DOMAIN:
CC_COMPARABILITY_DESIGN: [Were cases and controls comparable for the most important confounding factor?]
CC_COMPARABILITY_ADDITIONAL: [Were additional relevant confounders controlled for?]

EXPOSURE DOMAIN:
CC_EXPOSURE_ASCERTAINMENT: [Was exposure ascertained by secure records or blinded interview?]
CC_SAME_METHOD: [Was the same method of ascertainment used for cases and controls?]
CC_NON_RESPONSE: [Was the non-response rate similar in cases and controls, or accounted for?]

Study text:
{text}

Answer using the KEY: ANSWER format. Use NI if the information is not in the text."""


NOS_CROSS_SECTIONAL_PROMPT = """You are applying the NOS-xs2 scale to a cross-sectional study.

Answer each item Y or N or NI. Format: KEY: ANSWER — one per line.

SELECTION DOMAIN:
XS_REPRESENTATIVENESS: [Is the sample representative of the target population?]
XS_SAMPLE_SIZE: [Was the sample size justified (power calculation or justification provided)?]
XS_NON_RESPONDENTS: [Were non-respondents described and response rate reported?]
XS_EXPOSURE_ASCERTAINMENT: [Was exposure measured by validated records, biological markers, or validated instruments?]
XS_SAME_TIMEFRAME: [Were exposure and outcome measured in the same timeframe?]

COMPARABILITY DOMAIN:
XS_COMPARABILITY_DESIGN: [Was the study controlled for the most important confounders?]
XS_COMPARABILITY_ADDITIONAL: [Were additional confounders controlled for?]

OUTCOME DOMAIN:
XS_OUTCOME_ASSESSMENT: [Was outcome assessed by independent assessment or validated instrument?]
XS_STATISTICAL_TEST: [Was an appropriate statistical test used (e.g. logistic regression)?]
XS_RESPONSE_RATE: [Was the response rate ≥70% or was a non-response analysis performed?]

Study text:
{text}

Answer using the KEY: ANSWER format. Use NI if the information is not in the text."""


# ══════════════════════════════════════════════════════════════════════════════
# GRADE — Evidence certainty factors
# ══════════════════════════════════════════════════════════════════════════════

GRADE_PROMPT = """You are a GRADE methodologist assessing the certainty of evidence.

Outcome: {outcome}
Study design: {study_design}

Studies included:
{studies_summary}

For each factor below, rate whether it should DOWNGRADE or UPGRADE certainty.
Use format: FACTOR_NAME: none / downgrade_1 / downgrade_2 / upgrade_1 / upgrade_2
Then provide a brief rationale on the next line: FACTOR_NAME_RATIONALE: <reason>

INDIRECTNESS: [Are the population, intervention, comparator, or outcome in the studies sufficiently different from the review question?]
INDIRECTNESS_RATIONALE:

IMPRECISION: [Are the confidence intervals wide? Is the total sample size small (<300 for RCTs)?]
IMPRECISION_RATIONALE:

PUBLICATION_BIAS: [Is there evidence of publication bias (e.g. only positive trials, no small negative studies)?]
PUBLICATION_BIAS_RATIONALE:

LARGE_EFFECT: [Is the effect size large (RR>2 or RR<0.5)? (For observational studies — upgrades certainty)]
LARGE_EFFECT_RATIONALE:

DOSE_RESPONSE: [Is there a dose-response relationship suggesting causality? (For observational studies)]
DOSE_RESPONSE_RATIONALE:

Use 'none' if no adjustment is needed. Use NI if impossible to judge from the information provided."""




