# storage/session_store.py

import streamlit as st
from typing import List, Optional

from models.schemas import Article, PICOExtraction

def init_session_state() -> None:
    defaults = {
        "pubmed_articles": [],
        "summaries": {},
        "pico_results": {},
        "relevance_scores": {},
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def store_articles(articles: List[Article]) -> None:
    st.session_state["pubmed_articles"] = articles

def get_articles() -> List[Article]:
    return st.session_state.get("pubmed_articles", [])

def store_summary(pmid: str, summary: str) -> None:
    st.session_state["summaries"][pmid] = summary

def get_summary(pmid: str) -> Optional[str]:
    return st.session_state.get("summaries", {}).get(pmid)

def store_pico(pmid: str, pico: PICOExtraction) -> None:
    st.session_state["pico_results"][pmid] = pico

def get_pico(pmid: str) -> Optional[PICOExtraction]:
    return st.session_state.get("pico_results", {}).get(pmid)

def store_score(pmid: str, score: Optional[float]) -> None:
    st.session_state["relevance_scores"][pmid] = score

def get_score(pmid: str) -> Optional[float]:
    return st.session_state.get("relevance_scores", {}).get(pmid)
