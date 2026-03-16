# config/prompts.py

SUMMARIZATION_PROMPT = """You are a clinical research assistant.
Summarize the following medical abstract into exactly 5 bullet points covering:
study objective, study design, key findings, population, and clinical significance.

Abstract:
{abstract}

The 5 key points are:
•"""


# BUG FIXED — PICO_EXTRACTION_PROMPT had two contradictory instructions:
#
#   1. "Return ONLY valid JSON with keys: population, intervention, comparison, outcome."
#   2. "Population studied:"   ← a line-format continuation suffix
#
# These two instructions directly contradict each other. The model receives
# both a JSON instruction AND a line-format prefix and produces neither
# correctly — it tries to reconcile the conflict and emits garbage.
#
# Fix: pick ONE format and be consistent throughout.
# We use the prefix-forcing / line-continuation approach, which is more
# reliable for small models like TinyLlama because the model just continues
# text rather than having to generate structured syntax from scratch.
# The _parse_lines() fallback in pico_extractor.py handles this format.
#
# When you upgrade to a larger model (Llama 3.1 8B+), you can switch to
# the JSON version below which is commented out.

PICO_EXTRACTION_PROMPT = """From the medical abstract below, fill in each field.
Write one short phrase per field. Do not add any other text.

Abstract:
{abstract}

Population studied:"""

# --- JSON version for larger models (Llama 3.1 8B+, Mistral 7B+) ---
# PICO_EXTRACTION_PROMPT = """Extract PICO elements from the abstract below.
# Return ONLY valid JSON. No preamble, no explanation, no markdown fences.
#
# {{
#   "population": "who was studied",
#   "intervention": "what was given or done",
#   "comparison": "what it was compared to, or null",
#   "outcome": "what was measured"
# }}
#
# Abstract:
# {abstract}
#
# JSON:"""


# BUG FIXED — RELEVANCE_SCORING_PROMPT:
#
# The prompt asks for YES/NO but parse_score() in relevance_scorer.py
# uses numeric regex patterns — it never checks for YES/NO strings.
# So when the model correctly outputs "YES", parse_score() returns None,
# and `None` is falsy, which displays as False in st.write().
#
# Two complementary fixes are needed:
#   1. This prompt (already correct — YES/NO is reliable for small models)
#   2. parse_score() must be updated to handle YES/NO → see relevance_scorer.py

RELEVANCE_SCORING_PROMPT = """Is the following abstract about a clinical study in humans?
Answer with exactly one word: YES or NO.

Abstract: {abstract}

Answer:"""
