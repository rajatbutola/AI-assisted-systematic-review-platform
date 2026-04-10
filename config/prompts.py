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
sample_size, age_group, sex_distribution, condition_severity, intervention, comparator, follow_up_duration, primary_outcome, primary_outcome_result, secondary_outcomes, adverse_events, statistical_significance, study_design, country_setting

JSON:"""