from typing import List, Optional
from pydantic import BaseModel, Field


class PICOQuery(BaseModel):
    population: str = ""
    intervention: str = ""
    comparison: Optional[str] = ""
    outcome: str = ""
    year_from: int = 2015
    year_to: int = 2024
    max_results: int = 10


class Article(BaseModel):
    pmid: str
    title: str
    abstract: str
    authors: List[str] = Field(default_factory=list)
    journal: str = ""
    year: str = ""


class PICOExtraction(BaseModel):
    population: str = ""
    intervention: str = ""
    comparison: str = ""
    outcome: str = ""


class ArticleAnalysis(BaseModel):
    pmid: str
    summary: Optional[str] = None
    pico: Optional[PICOExtraction] = None
    relevance_score: Optional[float] = None
    screening_decision: Optional[str] = None


class ScreeningDecision(BaseModel):
    review_id: int
    pmid: str
    stage: str
    decision: str
    reason: Optional[str] = None
    reviewer_id: Optional[str] = "user"
