SUMMARIZATION_PROMPT = """You are a clinical research assistant.
Summarize the following medical abstract into exactly 5 bullet points covering:
study objective, study design, key findings, population, and clinical significance.

Abstract:
{abstract}

Summary (The 5 key points are):
"""

PICO_EXTRACTION_PROMPT = """Extract PICO elements from the abstract below.
Return ONLY valid JSON with keys: population, intervention, comparison, outcome.
Use null for missing elements.

Abstract:
{abstract}

JSON:
Population studied:
"""

# RELEVANCE_SCORING_PROMPT = """Rate the relevance of this abstract for a systematic review.
# Return ONLY a single number from 0 to 10, where:
# 0 = irrelevant
# 10 = highly relevant

# Abstract:
# {abstract}

# Score:
# """

RELEVANCE_SCORING_PROMPT = """Is the following abstract about a clinical study in humans?
Answer with one word: YES or NO.

Abstract: {abstract}

Answer:"""