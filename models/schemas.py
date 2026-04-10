# models/schemas.py

# Changes from previous version:
#   1. ArticleSource.PMC added
#   2. Article.full_text field added (Optional, not stored in DB by default)

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class ResearchDomain(str, Enum):
    MEDICAL = "medical"
    ML_AI   = "ml_ai"


class ArticleSource(str, Enum):
    """Which API the article came from."""
    PUBMED            =  "pubmed"
    PMC               =  "pmc"           # ← NEW: PubMed Central full-text
    EUROPE_PMC        =  "europe_pmc" # edit on 31st March
    CORE              =  "core" # edit on 10th April
    SEMANTIC_SCHOLAR  =  "semantic_scholar"
    OPENALEX          =  "openalex"


class PICOQuery(BaseModel):
    population:   str = ""
    intervention: str = ""
    comparison:   Optional[str] = ""
    outcome:      str = ""
    year_from:    int = 2015
    year_to:      int = 2024
    max_results:  int = 10


class Article(BaseModel):
    pmid:     str
    title:    str
    abstract: str
    authors:  List[str] = Field(default_factory=list)
    journal:  str = ""
    year:     str = ""
    doi:      Optional[str] = None
    source:   ArticleSource = ArticleSource.PUBMED
    domain:   ResearchDomain = ResearchDomain.MEDICAL
    url:      Optional[str] = None
    venue:    Optional[str] = None
    citation_count: Optional[int] = None
    # ── NEW: full text from PMC (not persisted to articles table,
    #         stored separately in the full_texts table) ──────────
    full_text: Optional[str] = None


class PICOExtraction(BaseModel):
    population:   str = ""
    intervention: str = ""
    comparison:   str = ""
    outcome:      str = ""


class ArticleAnalysis(BaseModel):
    pmid:               str
    summary:            Optional[str]          = None
    pico:               Optional[PICOExtraction] = None
    relevance_score:    Optional[float]        = None
    screening_decision: Optional[str]          = None


class ScreeningDecision(BaseModel):
    review_id:   int
    pmid:        str
    stage:       str
    decision:    str
    reason:      Optional[str] = None
    reviewer_id: Optional[str] = "user_1"
