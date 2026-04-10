# core/query_builder.py v1 30th March, 2026
#
# HOW PUBMED FILTERS FOR SR AND MA — TECHNICAL EXPLANATION
# =========================================================
#
# PubMed indexes every article with structured MeSH (Medical Subject Headings)
# metadata, including a "Publication Type" [pt] field. This is assigned by
# MEDLINE indexers based on the full article content — NOT just the title.
# A paper can be a systematic review even if "systematic review" never appears
# in its title, as long as MEDLINE categorised it as one.
#
# The authoritative PubMed Publication Type filters are:
#
#   "systematic review"[pt]
#       → Matches articles indexed as Publication Type = Systematic Review
#         This is a formal MeSH Publication Type added since 2019 for newer
#         articles. Older SRs may not have it.
#
#   "meta-analysis"[pt]
#       → Matches articles indexed as Publication Type = Meta-Analysis
#         Very precise — only articles that performed quantitative pooling.
#
# For older articles and broader coverage, we ALSO search title/abstract
# using [tiab]:
#
#   "systematic review"[tiab]
#       → Title or Abstract contains the phrase "systematic review"
#
#   "meta-analysis"[tiab] OR "meta analysis"[tiab] OR "metaanalysis"[tiab]
#       → Catches all spelling variants in text
#
# We combine them with OR to maximise recall:
#   ("systematic review"[pt] OR "systematic review"[tiab] OR ...)
#
# This is exactly how PubMed's own Clinical Queries filter works.
# Reference: https://pubmed.ncbi.nlm.nih.gov/help/#clinical-study-categories
#
# STUDY DESIGN FILTER (new — for primary studies only)
# ======================================================
# When the user wants to restrict to a specific study design (RCT, cohort,
# case-control, etc.), we apply the Cochrane Highly Sensitive Search Strategy
# filters. For example, for RCTs:
#   ("randomized controlled trial"[pt] OR "controlled clinical trial"[pt] OR
#    "randomized"[tiab] OR "placebo"[tiab] OR "randomly"[tiab])
#
# SEARCH MODE OPTIONS
# ===================
# "All Studies"         → no filter (default, broad PICO search)
# "Systematic Reviews"  → adds SR filter
# "Meta-Analyses"       → adds MA filter (subset of SRs)
# "RCTs Only"           → adds RCT filter (primary studies)
# "Observational"       → adds cohort/case-control filter

# core/query_builder.py  v2 31st March

# ── Publication-type filter strings (PubMed E-utilities syntax) ──────────────

# Systematic Review filter: Publication Type AND title/abstract variants
_SR_FILTER = (
    '("systematic review"[pt] OR "systematic review"[tiab] OR '
    '"systematic literature review"[tiab] OR "cochrane review"[tiab] OR '
    '"scoping review"[tiab])'
)

# Meta-Analysis filter: subset of SR — quantitative pooling confirmed
_MA_FILTER = (
    '("meta-analysis"[pt] OR "meta-analysis"[tiab] OR '
    '"meta analysis"[tiab] OR "metaanalysis"[tiab] OR '
    '"pooled analysis"[tiab] OR "network meta-analysis"[tiab])'
)

# RCT filter (Cochrane Highly Sensitive Search Strategy, sensitivity-maximising)
_RCT_FILTER = (
    '("randomized controlled trial"[pt] OR "controlled clinical trial"[pt] OR '
    '"randomized"[tiab] OR "randomised"[tiab] OR "randomly"[tiab] OR '
    '"placebo"[tiab] OR "double blind"[tiab] OR "single blind"[tiab])'
)

# Observational filter
_OBS_FILTER = (
    '("cohort studies"[mh] OR "cohort study"[tiab] OR "cohort studies"[tiab] OR '
    '"case-control"[tiab] OR "case control"[tiab] OR "observational study"[tiab] OR '
    '"prospective study"[tiab] OR "retrospective study"[tiab] OR '
    '"cross-sectional"[tiab])'
)

# Map user-facing labels to filter strings
STUDY_TYPE_FILTERS = {
    "All Studies":        "",           # no filter
    "Systematic Reviews": _SR_FILTER,
    "Meta-Analyses":      _MA_FILTER,
    "RCTs Only":          _RCT_FILTER,
    "Observational":      _OBS_FILTER,
}


def build_query(
    population:   str,
    intervention: str,
    comparison:   str = "",
    outcome:      str = "",
    year_from:    int = 2015,
    year_to:      int = 2024,
    study_type:   str = "All Studies",   # ← new parameter
) -> str:
    """
    Build a structured PubMed Boolean query.

    Parameters
    ----------
    population, intervention, comparison, outcome : str
        PICO fields. At least one must be non-empty.
    year_from, year_to : int
        Publication year range — applied as [dp] (date of publication) filter.
    study_type : str
        One of the keys in STUDY_TYPE_FILTERS.
        "Systematic Reviews" and "Meta-Analyses" use Publication Type [pt]
        AND title/abstract [tiab] filters for maximum sensitivity.
        This does NOT rely on the word appearing in the title alone.

    Returns
    -------
    str
        A PubMed-compatible Boolean query string ready for the E-utilities API.

    Examples
    --------
    >>> build_query("CLL", "venetoclax", study_type="Meta-Analyses")
    '(CLL) AND (venetoclax) AND 2015:2024[dp] AND ("meta-analysis"[pt] OR ...)'
    """
    terms = []
    if population.strip():
        terms.append(f"({population.strip()})")
    if intervention.strip():
        terms.append(f"({intervention.strip()})")
    if comparison.strip():
        terms.append(f"({comparison.strip()})")
    if outcome.strip():
        terms.append(f"({outcome.strip()})")
    if not terms:
        raise ValueError("Please enter at least one PICO field before searching.")

    query = " AND ".join(terms)
    query += f" AND {year_from}:{year_to}[dp]"

    # Apply study-type filter
    type_filter = STUDY_TYPE_FILTERS.get(study_type, "")
    if type_filter:
        query += f" AND {type_filter}"

    return query


def build_ml_query(
    topic:    str,
    keywords: str = "",
    venues:   list = None,
    year_from: int = 2019,
    year_to:   int = 2025,
) -> str:
    """Free-text query builder for ML/AI literature (Semantic Scholar, OpenAlex)."""
    parts = [topic.strip()]
    if keywords.strip():
        for kw in keywords.split(","):
            kw = kw.strip()
            if kw:
                parts.append(kw)
    query = " ".join(parts)
    if venues:
        venue_str = " OR ".join(venues)
        query += f" ({venue_str})"
    return query


# ── Europe PMC query builder ──────────────────────────────────────────────────
# Europe PMC uses a different field syntax from PubMed E-utilities.
# Key field tags:
#   TITLE:     searches article title
#   ABSTRACT:  searches abstract
#   AUTH:      author name
#   PUB_TYPE:  publication type (e.g. "systematic review")
#   FIRST_PDATE: first publication date (YYYY format)
#
# Boolean operators: AND OR NOT (uppercase required)
# Phrase search: use double quotes
# Reference: https://europepmc.org/searchsyntax

_EPMC_SR_FILTER  = '(PUB_TYPE:"systematic review" OR TITLE:"systematic review" OR ABSTRACT:"systematic review" OR TITLE:"scoping review")'
_EPMC_MA_FILTER  = '(PUB_TYPE:"meta-analysis" OR TITLE:"meta-analysis" OR ABSTRACT:"meta-analysis" OR ABSTRACT:"pooled analysis")'
_EPMC_RCT_FILTER = '(PUB_TYPE:"randomized controlled trial" OR PUB_TYPE:"clinical trial" OR ABSTRACT:"randomized" OR ABSTRACT:"randomised" OR ABSTRACT:"placebo")'
_EPMC_OBS_FILTER = '(ABSTRACT:"cohort study" OR ABSTRACT:"cohort studies" OR ABSTRACT:"case-control" OR ABSTRACT:"cross-sectional")'

EPMC_STUDY_TYPE_FILTERS = {
    "All Studies":        "",
    "Systematic Reviews": _EPMC_SR_FILTER,
    "Meta-Analyses":      _EPMC_MA_FILTER,
    "RCTs Only":          _EPMC_RCT_FILTER,
    "Observational":      _EPMC_OBS_FILTER,
}


def build_epmc_query(
    population:   str,
    intervention: str,
    comparison:   str = "",
    outcome:      str = "",
    year_from:    int = 2015,
    year_to:      int = 2024,
    study_type:   str = "All Studies",
) -> str:
    """
    Build a Europe PMC Boolean query from PICO fields.
    Searches in TITLE and ABSTRACT fields for each PICO term.
    Applies PUB_TYPE and text filters for study type.

    Parameters are the same as build_query() so the two can be called
    interchangeably from the UI.
    """
    terms = []

    def _pico_term(value: str) -> str:
        """Search a term in both TITLE and ABSTRACT."""
        v = value.strip()
        if not v:
            return ""
        # Wrap multi-word terms in quotes
        if " " in v:
            return f'(TITLE:"{v}" OR ABSTRACT:"{v}")'
        return f'(TITLE:{v} OR ABSTRACT:{v})'

    for field in [population, intervention, comparison, outcome]:
        t = _pico_term(field)
        if t:
            terms.append(t)

    if not terms:
        raise ValueError("Please enter at least one PICO field before searching.")

    query = " AND ".join(terms)
    query += f" AND FIRST_PDATE:[{year_from}-01-01 TO {year_to}-12-31]"

    type_filter = EPMC_STUDY_TYPE_FILTERS.get(study_type, "")
    if type_filter:
        query += f" AND {type_filter}"

    return query



