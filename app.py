# app.py v1 - 25th March
#
# AI-Assisted Systematic Review Platform
#
# Tab order (updated per spec):
#   1.  Search       — PubMed / PMC Full-Text / ML-AI retrieval
#   2.  Screening    — Multi-reviewer title/abstract screening
#   3.  AI Analysis  — Summarisation, PICO, Quality, Data Pooling
#                         (only included articles, AFTER screening)
#   4.  PRISMA       — Publication-quality PRISMA 2020 diagram
#
# Key changes from previous version:
#   - AI Analysis moved after Screening (dependency enforced in ui/ai_analysis_panel.py)
#   - PMC now uses two-step pipeline (PubMed search → PMC filter)
#   - PubMed results are shown immediately in "PubMed" mode
#   - PMC filter results shown separately in "PMC Full-Text" mode
#   - PRISMA redesigned with B&W + Colour dual modes, dynamic databases

# app.py v2 27th March

# ── BUG FIXES ────────────────────────────────────────────────────────────────
#
#  FIX 1 — Search results flash then disappear
#    Root cause: st.rerun() was called immediately after saving articles,
#    which re-executed the entire script. Because the fetched `articles` list
#    lived only in local variables inside the `if submitted:` block, it was
#    gone on the next render. The user saw results for a split second then they
#    vanished.
#    Fix: store search results in st.session_state keyed by (review_id, source).
#    The display block is placed OUTSIDE the `if submitted:` guard so it runs
#    on every render and always reads from session_state. st.rerun() is only
#    called after results are already safely persisted in session_state, so
#    they survive into the next render cycle.
#
#  FIX 2 — Blinded screening (see ui/screening_panel.py)
#    Primary reviewers now only see their own decisions. Other reviewers'
#    decisions are hidden until the Editor's conflict-resolution view.
#
#  FIX 3 — AI Analysis wrong counts / quality parse error (see ui/ai_analysis_panel.py)
#    - Adjudicator decision lookup now checks the correct reviewer_id key that
#      the AdjudicationRepository stores (current_reviewer_id of the Editor).
#    - "Pending" count is articles with zero decisions from any reviewer.
#    - Quality prompt no longer has the double-brace injection bug.
#
#  FIX 4 — PRISMA B&W toggle infinite rerun + customisation not applying
#    Mode is stored in session_state immediately; DB write happens only on
#    "Apply Changes". Dynamic db labels pulled from session_state automatically.
# ─────────────────────────────────────────────────────────────────────────────

# app.py  — v3 -27th March
#
# PubMed/PMC interface redesign:
#   - Removed "PMC Full-Text" from the Data Source selectbox entirely.
#   - After a PubMed search the results area shows TWO sub-tabs:
#       PubMed Results  — all articles retrieved from PubMed
#       PMC Full-Text   — subset with open-access full text + links
#   - The PMC sub-tab is populated by a "🔍 Find Full Texts in PMC" button
#     that only appears AFTER a PubMed search has been done.
#   - This makes clear that PMC is an enrichment step, not a separate search.


# app.py  — v3 30th March
#
# PubMed/PMC interface redesign:
#   - Removed "PMC Full-Text" from the Data Source selectbox entirely.
#   - After a PubMed search the results area shows TWO sub-tabs:
#        PubMed Results  — all articles retrieved from PubMed
#        PMC Full-Text   — subset with open-access full text + links
#   - The PMC sub-tab is populated by a "🔍 Find Full Texts in PMC" button
#     that only appears AFTER a PubMed search has been done.
#   - This makes clear that PMC is an enrichment step, not a separate search.



# app.py — v4 31st March
#
# NEW IN THIS VERSION:
#
# 1. Europe PMC as a second medical source (dropdown: PubMed / Europe PMC)
#    - Same PICO form, same study-type filters, same PMC full-text tab
#    - Europe PMC query uses different syntax (TITLE: ABSTRACT: fields)
#
# 2. Multi-source search with deduplication
#    - When both PubMed AND Europe PMC have been searched for the same review,
#      a "Combined + Deduplicate" button appears
#    - Deduplication is by PMID (exact match); EPMC-only articles keep their EPMC: id
#    - Duplicate count is tracked and stored in session_state for PRISMA
#
# 3. PRISMA dynamic database boxes
#    - Stores per-source counts: {"PubMed": 2500, "Europe PMC": 3150, "duplicates": 1150}
#    - PRISMA panel reads this and renders two identification boxes + dedup note
#
# 4. Enhanced article cards
#    - Authors (truncated to first 3 + et al.)
#    - Citation count with icon
#    - Study type / publication type badge
#    - Source badge (PubMed vs Europe PMC)
#
# 5. Sort functionality
#    - Sort by: Newest first / Oldest first / Most cited
#    - Applied client-side to the persisted results list



# app.py — v6 7th April
#
# FIX 1 — PRISMA source count accumulation (not overwrite)
#   Old: prisma_sources[source_name] = len(articles)
#        This REPLACED the old PubMed count with new PubMed count.
#        PRISMA showed wrong numbers after subsequent searches.
#   Fix: Store per-source counts CUMULATIVELY in a persistent DB table
#        (search_repository already tracks n_results per search_id).
#        The PRISMA panel reads the SUM of all searches per source from
#        the search_repository, not from session_state.
#        session_state still caches the LATEST search results for display,
#        but PRISMA counts come from the DB truth.
#        Additionally: if a user changes their PICO query under the same
#        review, a warning is shown offering to either (a) continue adding
#        to the same review, or (b) create a new review automatically.
#
# FIX 2 — Unified full-text button for all articles regardless of source
#   Old: separate "Find Full Texts in PMC" per source (PubMed vs Europe PMC)
#        and Europe PMC articles with EPMC: prefix were excluded from PMC lookup.
#   Fix: One unified "Find All Full Texts" button that:
#        (a) Takes ALL unique articles from BOTH sources (deduplicated)
#        (b) Checks NCBI PMC for all articles that have a real PMID
#        (c) Also checks Europe PMC's own full-text endpoint for EPMC: articles
#        (d) Shows source of each full text (PMC / Europe PMC)
#        Europe PMC articles that are also in PubMed (same PMID) are only
#        fetched once, not twice.
#
# FIX 3 — PRISMA diagram arrow rendering (see ui/prisma_panel.py)
#   Arrows now use SVG path shapes (go.layout.Shape with type="path")
#   instead of annotations with axref/ayref, which had inconsistent
#   coordinate mapping in Plotly and caused arrows to miss box edges.




# app.py — v6
#
# v6 additions:
#   PERF 1 — Parallel full-text retrieval in _run_unified_fulltext
#     All three passes now use ThreadPoolExecutor so articles are processed
#     concurrently instead of sequentially.
#     Pass 1: HEAD validation parallelised (was the biggest bottleneck — 8s per article)
#     Pass 2: Europe PMC search parallelised (one HTTP request per article)
#     Pass 3: Unpaywall parallelised (one HTTP request per DOI)
#     Pass 4: CORE Aggregate search for articles still missing a PDF
#     All shared dicts (pmc_urls, pdf_urls, full_texts, enriched_map) are
#     protected with threading.Lock() for thread-safe writes.
#     Expected speedup: ~8x for 40 articles, scales linearly with max_workers.
#   PERF 2 — Validated NCBI PDF URLs via shared _validate_ncbi_pdf() helper
#     (avoids storing broken /pdf/ URLs that redirect to Silverchair CDN tokens)



# app.py — v7
# v7: UMLS concept expansion integrated into PICO search form
#
# v5 fixes (unchanged):
#   FIX 1 — PRISMA source count accumulation (cumulative from DB, not overwrite)
#   FIX 2 — Unified full-text tab: 3-pass additive strategy (NCBI → Europe PMC → Unpaywall)
#   FIX 3 — PRISMA diagram arrow rendering (SVG path shapes)





import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

import streamlit as st

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("sr_platform.log"),
    ],
)
logger = logging.getLogger(__name__)

from storage.database          import init_database
from storage.migrations        import run_migrations
from storage.repository        import (
    ReviewRepository, ArticleRepository, ScreeningRepository,
    AdjudicationRepository,
)
from storage.search_repository import SearchRepository
from core.domain_registry      import build_default_registry
from core.query_builder        import build_query, build_ml_query, build_epmc_query
from core.pubmed_pmc_pipeline  import PubMedPMCPipeline
from models.schemas            import ResearchDomain, Article
from ui.prisma_panel           import render_prisma_diagram
from ui.screening_panel        import render_screening_panel
from ui.ai_analysis_panel      import render_ai_analysis_panel

init_database()
run_migrations()

review_repo       = ReviewRepository()
article_repo      = ArticleRepository()
screen_repo       = ScreeningRepository()
adjudication_repo = AdjudicationRepository()
search_repo       = SearchRepository()


@st.cache_resource
def get_registry(_version=7):
    return build_default_registry()


registry = get_registry()

st.set_page_config(page_title="AI-Assisted Systematic Review", layout="wide")

if st.session_state.get("dark_mode", False):
    st.markdown("""
    <style>
        .stApp { background-color: #0E1117; color: #FAFAFA; }
        section[data-testid="stSidebar"] { background-color: #1A1F2E; }
        .stExpander, .stButton>button, .stSelectbox, .stTextInput,
        .stNumberInput, .stSlider, .stRadio, .stTabs [data-testid="stTab"] {
            background-color: #1A1F2E !important; color: #FAFAFA !important; }
        .stMetric { background-color: #1A1F2E; border: 1px solid #2E3A4D; }
    </style>
    """, unsafe_allow_html=True)

with st.sidebar:
    st.header("📋 Systematic Reviews")
    reviews = review_repo.list_reviews()
    if reviews:
        options   = {r["title"]: r["id"] for r in reviews}
        selected  = st.selectbox("Select Review", list(options.keys()))
        review_id = options[selected]
    else:
        review_id = None
        st.info("Create a review to begin.")

    st.divider()
    st.subheader("Create New Review")
    new_title = st.text_input("Review Title")
    if st.button("Create Review") and new_title.strip():
        review_id = review_repo.create_review(new_title.strip())
        st.success(f"Review '{new_title}' created.")
        st.rerun()

    st.divider()
    st.subheader("🌙 Appearance")
    if "dark_mode" not in st.session_state:
        st.session_state.dark_mode = False
    dark = st.toggle("Dark Mode", value=st.session_state.dark_mode)
    if dark != st.session_state.dark_mode:
        st.session_state.dark_mode = dark
        st.rerun()

    st.divider()
    st.subheader("👤 Reviewer")
    selected_reviewer = st.selectbox(
        "Screening as:",
        ["Reviewer 1", "Reviewer 2", "Reviewer 3", "Editor"],
        index=0, key="current_reviewer_select",
    )
    current_reviewer_id = f"rev_{selected_reviewer.lower().replace(' ', '_')}"
    st.session_state["current_reviewer_id"] = current_reviewer_id
    if "editor" in current_reviewer_id:
        st.info("🔑 Editor mode: see all decisions, resolve conflicts.")
    else:
        st.info("🔒 Blinded — only your decisions are visible.")

if not review_id:
    st.title("Welcome to the SR Platform")
    st.info("Create a new review in the sidebar to get started.")
    st.stop()

review = review_repo.get_review(review_id)
st.title(f"📚 {review['title']}")

counts = article_repo.get_screening_counts(review_id)
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Articles", counts["total"])
c2.metric("✅ Included",    counts["included"])
c3.metric("❌ Excluded",    counts["excluded"])
c4.metric("🟡 Unsure",      counts["unsure"])
c5.metric("⚠️ Conflict",    counts.get("conflict", 0))
c6.metric("⏳ Pending",     counts["pending"])


# ── Session-state helpers ─────────────────────────────────────────────────────

def _sk(rid, key):
    return f"{key}_{rid}"

def _store(rid, key, value):
    st.session_state[_sk(rid, key)] = value

def _load(rid, key, default=None):
    return st.session_state.get(_sk(rid, key), default)


# ── FIX 1: Persistent PRISMA source counts from DB ────────────────────────────

def _get_prisma_sources_from_db(review_id: int) -> Dict:
    try:
        source_totals = search_repo.get_source_totals(review_id)
    except Exception as e:
        logger.error("_get_prisma_sources_from_db failed: %s", e)
        return {}
    dupes = _load(review_id, "n_duplicates_removed", 0) or 0
    if dupes:
        source_totals["duplicates_removed"] = dupes
    return source_totals


def _update_prisma_info(review_id: int, source_name: str, n_articles: int,
                        n_dupes: int = 0) -> None:
    if n_dupes > 0:
        _store(review_id, "n_duplicates_removed", n_dupes)
    sources = _get_prisma_sources_from_db(review_id)
    st.session_state[f"prisma_db_{review_id}"] = sources


# ── Sort helper ───────────────────────────────────────────────────────────────

def _sort_articles(articles: List[Article], sort_by: str) -> List[Article]:
    if sort_by == "Newest first":
        return sorted(articles,
                      key=lambda a: int(a.year) if str(a.year).isdigit() else 0,
                      reverse=True)
    elif sort_by == "Oldest first":
        return sorted(articles,
                      key=lambda a: int(a.year) if str(a.year).isdigit() else 9999)
    elif sort_by == "Most cited":
        return sorted(articles,
                      key=lambda a: a.citation_count if a.citation_count is not None else -1,
                      reverse=True)
    return articles


# ── Deduplication helper ──────────────────────────────────────────────────────

def _clean_doi(doi_raw: str) -> str:
    """Normalise DOI to bare 10.xxxx/... form for comparison."""
    doi = (doi_raw or "").strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/",
                   "https://dx.doi.org/", "doi:"):
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
            break
    return doi.strip()


def _title_key(title: str) -> str:
    """Normalised title key: lowercase, strip punctuation, first 60 chars."""
    import re
    t = re.sub(r"[^a-z0-9 ]", "", (title or "").lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t[:60]


def _deduplicate(*lists) -> tuple:
    """
    Deduplicate articles across any number of source lists.

    Three-key matching (tried in order, highest precision first):
      1. pmid  — exact match (PubMed ↔ Europe PMC)
      2. doi   — normalised DOI (catches CORE vs PubMed/EPMC duplicates,
                  because CORE articles use DOI as their pmid field)
      3. title — first 60 chars, lowercased, punctuation stripped
                  (last resort for articles with neither pmid nor doi)

    Returns
    -------
    (unique_articles: List[Article], n_duplicates: int)
    Earlier lists take priority over later ones (first-seen wins).
    """
    seen_pmid:  Dict[str, Article] = {}
    seen_doi:   Dict[str, Article] = {}
    seen_title: Dict[str, Article] = {}
    unique:     list = []
    dupes = 0

    for art_list in lists:
        for art in (art_list or []):
            pmid  = (art.pmid or "").strip()
            doi   = _clean_doi(art.doi or "")
            # CORE articles store their DOI as the pmid field — detect that
            if not doi and pmid.startswith("10."):
                doi = pmid   # CORE uid IS the doi
            title = _title_key(art.title or "")

            # Check for duplicate using any key
            is_dupe = (
                (pmid  and pmid  in seen_pmid)  or
                (doi   and doi   in seen_doi)   or
                (title and title in seen_title)
            )

            if is_dupe:
                dupes += 1
            else:
                unique.append(art)
                if pmid:
                    seen_pmid[pmid]   = art
                if doi:
                    seen_doi[doi]     = art
                if title:
                    seen_title[title] = art

    return unique, dupes


# ── FIX 1: PICO change detection ─────────────────────────────────────────────

def _pico_fingerprint(population, intervention, comparison, outcome,
                      year_from, year_to, study_type) -> str:
    return f"{population}|{intervention}|{comparison}|{outcome}|{year_from}|{year_to}|{study_type}"


def _check_pico_change(review_id: int, new_fp: str) -> bool:
    key = f"pico_fp_{review_id}"
    old_fp = st.session_state.get(key)
    if old_fp is None:
        return False
    return old_fp != new_fp


def _store_pico_fp(review_id: int, fp: str) -> None:
    st.session_state[f"pico_fp_{review_id}"] = fp


# ── Article card renderer ─────────────────────────────────────────────────────

def _render_article_cards(articles: List[Article], pmc_urls: dict,
                           pdf_urls: dict = None,
                           sort_by: str = "Newest first",
                           is_fulltext_view: bool = False) -> None:
    """
    Render article cards.

    is_fulltext_view: True only when called from the Full Texts tab
    (after the user has clicked "Find All Full Texts").
    Controls whether PDF links, "Open PMC Article → click PDF" hints,
    and "Open DOI page → click PDF" hints are shown.
    Prevents these from appearing in the initial search results tabs
    where no full-text lookup has been run yet.
    """
    if not articles:
        st.info("No articles to display.")
        return
    if pdf_urls is None:
        pdf_urls = {}

    for article in _sort_articles(articles, sort_by):
        pmid    = article.pmid
        pmc_url = pmc_urls.get(pmid)

        source_label = getattr(article.source, "value", str(article.source))
        source_badge = {
            "pubmed":     "🔵 PubMed",
            "europe_pmc": "🟢 Europe PMC",
            "pmc":        "📄 PMC",
            "core":       "🟠 CORE",
        }.get(source_label, source_label.upper())

        with st.expander(f"**{article.title[:85]}**", expanded=False):
            col_meta, col_links = st.columns([3, 1])

            with col_meta:
                if article.authors:
                    author_str = (
                        ", ".join(article.authors[:3]) + " et al."
                        if len(article.authors) > 3
                        else ", ".join(article.authors)
                    )
                    st.caption(f"👥 {author_str}")

                meta_parts = []
                if article.journal:
                    meta_parts.append(f"**Journal:** {article.journal}")
                if article.year:
                    meta_parts.append(f"**Year:** {article.year}")
                if source_label == "pubmed" and pmid and not pmid.startswith("EPMC:"):
                    meta_parts.append(f"**PMID:** {pmid}")
                elif pmid and pmid.startswith("EPMC:"):
                    meta_parts.append(f"**ID:** {pmid}")
                if article.citation_count is not None:
                    meta_parts.append(f"📊 **Cited:** {article.citation_count}")
                st.caption("  |  ".join(meta_parts))

                if article.venue:
                    st.caption(f"📋 **Type:** {article.venue}")
                st.caption(f"🔍 **Source:** {source_badge}")

                if article.abstract:
                    import re
                    clean_abstract = re.sub(r"<[^>]+>", " ", article.abstract)
                    clean_abstract = re.sub(r"\s+", " ", clean_abstract).strip()
                    st.write(clean_abstract[:400] +
                             ("…" if len(clean_abstract) > 400 else ""))
                else:
                    st.write("*No abstract available.*")

            with col_links:
                if source_label == "pubmed" and pmid and not pmid.startswith("EPMC:"):
                    st.markdown(f"[🔗 PubMed](https://pubmed.ncbi.nlm.nih.gov/{pmid}/)")
                if article.url and "europepmc" in (article.url or ""):
                    st.markdown(f"[🟢 Europe PMC]({article.url})")
                if source_label == "core" and article.url:
                    st.markdown(f"[🟠 CORE]({article.url})")
                if source_label == "semantic_scholar" and pmid:
                    st.markdown(f"[🔬 Semantic Scholar](https://www.semanticscholar.org/paper/{pmid})")
                if source_label == "openalex" and article.url:
                    st.markdown(f"[🔍 OpenAlex]({article.url})")

                source_url = article.url or ""
                pdf_url    = pdf_urls.get(pmid)

                # Article page and PDF links — only shown in Full Texts tab
                # (after user has clicked "Find All Full Texts"). In the
                # initial search tabs pmc_urls={} so pmc_url is always None,
                # but we gate explicitly to be safe and clear in intent.
                if is_fulltext_view and pmc_url and pmc_url != source_url:
                    if "ncbi.nlm.nih.gov/pmc" in pmc_url:
                        st.markdown(f"[📄 PMC Article]({pmc_url})")
                        if not pdf_url:
                            st.caption("💡 Open PMC Article → click PDF")
                    elif "europepmc.org" in pmc_url:
                        st.markdown(f"[📄 Europe PMC Article]({pmc_url})")
                    elif "doi.org" in pmc_url:
                        pass
                    else:
                        st.markdown(f"[📄 Article Page]({pmc_url})")

                if is_fulltext_view and pdf_url:
                    st.markdown(f"[📥 Download PDF]({pdf_url})")
                elif is_fulltext_view and not pdf_url and article.doi and not pmc_url:
                    st.caption("💡 Open DOI page → click PDF")

                if article.doi:
                    doi_display = _clean_doi_for_display(article.doi)
                    st.markdown(f"[🌐 DOI]({doi_display})")

            if hasattr(article, "full_text") and article.full_text:
                ft = article.full_text or ""
                if ft.startswith("PDF:"):
                    # CORE articles encode downloadUrl as "PDF:{url}" at parse time.
                    # Only show in Full Texts tab — not in initial CORE search tab.
                    if is_fulltext_view:
                        _core_pdf = ft[4:].strip()
                        if _core_pdf and not pdf_url:
                            st.markdown(f"[📥 Download PDF (CORE)]({_core_pdf})")
                else:
                    st.success("✅ Full text available")


def _clean_doi_for_display(doi_raw: str) -> str:
    doi = doi_raw.strip()
    for prefix in ("https://doi.org/", "http://doi.org/",
                   "https://dx.doi.org/", "doi:"):
        if doi.lower().startswith(prefix.lower()):
            doi = doi[len(prefix):]
            break
    return f"https://doi.org/{doi.strip()}"


# ── FIX 2: Unified full-text tab ─────────────────────────────────────────────

def _render_unified_fulltext_tab(review_id: int, pmc_cl, article_repo):
    pubmed_arts = _load(review_id, "articles_PubMed", []) or []
    epmc_arts   = _load(review_id, "articles_Europe PMC", []) or []
    core_arts   = _load(review_id, "articles_CORE", []) or []

    if not pubmed_arts and not epmc_arts and not core_arts:
        st.info("Search for articles first, then use this tab to find full texts.")
        return

    # Deduplicate across all three sources (pmid + doi + title)
    all_unique, _ = _deduplicate(pubmed_arts, epmc_arts, core_arts)
    n_total = len(all_unique)

    # Split for pass routing: real PMIDs, EPMC-prefixed, CORE-prefixed
    pmid_articles  = [a for a in all_unique
                      if a.pmid and not a.pmid.startswith("EPMC:")
                      and not a.pmid.startswith("CORE:")]
    epmc_articles_ = [a for a in all_unique
                      if a.pmid and (a.pmid.startswith("EPMC:")
                                     or a.pmid.startswith("CORE:"))]

    ft_results = _load(review_id, "unified_fulltext")
    ft_urls    = _load(review_id, "unified_fulltext_urls") or {}
    ft_pdfs    = _load(review_id, "unified_fulltext_pdfs") or {}

    if ft_results is None:
        st.info(
            f"**{n_total}** unique articles across all your searches "
            f"({len(pmid_articles)} with real PMIDs, {len(epmc_articles_)} Europe PMC-only). "
            f"Click below to search for full texts. "
            f"**Europe PMC is checked first** for all {n_total} articles, "
            f"then NCBI PMC is used as fallback for any not found there."
        )
        if st.button("🔍 Find All Full Texts (PMC + Europe PMC)",
                     type="primary", key=f"unified_ft_{review_id}"):
            _run_unified_fulltext(
                review_id, pmid_articles, epmc_articles_,
                pmc_cl, article_repo
            )
    elif len(ft_results) == 0:
        st.warning(
            "No PMC or Europe PMC coverage found for any article. "
            "This usually means none of the articles are open-access. "
            "You can still access them via the PubMed/DOI links in each article card."
        )
        if st.button("🔄 Re-check", key=f"unified_ft_retry_{review_id}"):
            _store(review_id, "unified_fulltext", None)
            st.rerun()
    else:
        n_with_text = sum(1 for a in ft_results if getattr(a, "full_text", None))
        n_link_only = len(ft_results) - n_with_text

        st.success(
            f"✅ **{len(ft_results)}** of {n_total} articles have PMC/Europe PMC coverage.  "
            f"**{n_with_text}** have extractable full text · "
            f"**{n_link_only}** have a link only (open in browser for full text)."
        )
        if n_link_only > 0:
            st.caption(
                "Articles marked 🔗 have a PMC/Europe PMC page but the text could not be "
                "extracted automatically (e.g. PDF-only articles). Use the link to read them."
            )
        if st.button("🔄 Re-check", key=f"unified_ft_retry2_{review_id}"):
            _store(review_id, "unified_fulltext", None)
            st.rerun()

        sort_by = st.selectbox("Sort by",
                               ["Newest first", "Oldest first", "Most cited"],
                               key=f"ft_sort_{review_id}")
        _render_article_cards(ft_results, pmc_urls=ft_urls, pdf_urls=ft_pdfs,
                              sort_by=sort_by, is_fulltext_view=True)


# ── PERF 1: Parallel unified full-text retrieval ─────────────────────────────

def _run_unified_fulltext(review_id, pmid_articles, epmc_articles_,
                          pmc_cl, article_repo):
    """
    Find full text and PDF links for ALL unique articles.

    ARCHITECTURE — three independent additive passes, now parallelised:

    Each pass runs ALL articles concurrently using ThreadPoolExecutor.
    Shared result dicts are protected with threading.Lock() for thread safety.
    setdefault() semantics preserved: earlier passes are never overwritten.

    Pass 1 — NCBI PMC elink (batch) + parallel HEAD validation per article
    Pass 2 — Europe PMC search API: one request per article, parallelised
    Pass 3 — Unpaywall: one request per DOI, parallelised

    Speedup vs sequential: ~8x with max_workers=8 (wall-clock, not CPU).
    The bottleneck is network I/O (HTTP requests) not CPU — threads are ideal.
    """
    from core.pubmed_pmc_pipeline import PubMedPMCPipeline
    from config.settings import NCBI_API_KEY, NCBI_EMAIL
    import requests
    import time

    all_articles = list(pmid_articles) + list(epmc_articles_)
    n_total      = len(all_articles)
    oa_email     = NCBI_EMAIL or "research@example.com"

    # Shared result dicts — written from multiple threads, protected by lock
    lock          = threading.Lock()
    pmc_urls:     Dict[str, str]     = {}
    pdf_urls:     Dict[str, str]     = {}
    full_texts:   Dict[str, str]     = {}
    enriched_map: Dict[str, Article] = {}

    # ── Shared helpers ────────────────────────────────────────────────────

    def _try_url(url: str, timeout: int = 15) -> Optional[requests.Response]:
        try:
            r = requests.get(url, timeout=timeout,
                             headers={"User-Agent": f"SR-Platform/1.0 ({oa_email})"})
            if r.status_code == 200 and len(r.content) > 200:
                return r
        except Exception as e:
            logger.debug("HTTP fetch error %s: %s", url, e)
        return None

    def _clean_doi(doi_raw: str) -> str:
        doi = doi_raw.strip()
        for prefix in ("https://doi.org/", "http://doi.org/",
                        "https://dx.doi.org/", "doi:"):
            if doi.lower().startswith(prefix.lower()):
                doi = doi[len(prefix):]
                break
        return doi.strip()

    # CDN patterns indicating time-limited token URLs — never store these
    _EXPIRING_CDN_PATTERNS = (
        "silverchair", "atypon", "token=",
        "watermark", "access_token", "Authorization=",
    )

    def _validate_ncbi_pdf(pmcid: str) -> Optional[str]:
        """
        Validate an NCBI PMC PDF URL before storing.
        Tries both NCBI domains, follows all redirects, rejects CDN token URLs.
        Returns the confirmed working URL or None.
        Called from Pass 1 and Pass 2 threads.
        """
        candidates = [
            f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/pdf/",
            f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/",
        ]
        for url in candidates:
            try:
                head = requests.head(
                    url, timeout=8, allow_redirects=True,
                    headers={"User-Agent": f"SR-Platform/1.0 ({oa_email})"}
                )
                final_url = head.url
                is_cdn    = any(p in final_url for p in _EXPIRING_CDN_PATTERNS)
                if head.status_code == 200 and not is_cdn:
                    return final_url
                if head.status_code == 200 and is_cdn:
                    logger.debug("PMC %s /pdf/ → expiring CDN: %s",
                                 pmcid, final_url[:60])
                    return None
            except Exception:
                continue
        return None

    # ══════════════════════════════════════════════════════════════════════
    # PASS 1 — NCBI PMC elink (batch) + parallel HEAD validation
    #
    # The elink batch call is inherently sequential (one API call for all PMIDs).
    # The per-article HEAD validation that follows is parallelised — this was
    # the biggest bottleneck (2-8 seconds per article × N articles).
    # ══════════════════════════════════════════════════════════════════════
    real_pmid_articles = [
        a for a in all_articles
        if a.pmid and not a.pmid.startswith("EPMC:")
    ]

    pmid_to_pmcid: Dict[str, str] = {}
    pmc_by_pmcid:  Dict[str, Article] = {}

    if real_pmid_articles and pmc_cl:
        pipeline = PubMedPMCPipeline.__new__(PubMedPMCPipeline)
        pipeline.pubmed_client = None
        pipeline.pmc_client    = pmc_cl
        pipeline._params_base  = {"tool": "SR_Platform", "email": oa_email}
        if NCBI_API_KEY:
            pipeline._params_base["api_key"] = NCBI_API_KEY

        with st.spinner(
            f"Pass 1/4 — NCBI PMC elink: mapping {len(real_pmid_articles)} PMIDs…"
        ):
            pmids         = [a.pmid for a in real_pmid_articles]
            pmid_to_pmcid = pipeline._map_pmids_to_pmcids(pmids)

        if pmid_to_pmcid:
            with st.spinner(
                f"Pass 1/4 — NCBI PMC: fetching full text + validating {len(pmid_to_pmcid)} PDFs in parallel…"
            ):
                pmc_fetched  = pmc_cl.fetch(list(pmid_to_pmcid.values()))
                pmc_by_pmcid = {a.pmid: a for a in pmc_fetched}

                # Worker: validate one article's PDF URL in parallel
                def _pass1_worker(article: Article):
                    pmcid = pmid_to_pmcid.get(article.pmid)
                    if not pmcid:
                        return
                    article_page  = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
                    validated_pdf = _validate_ncbi_pdf(pmcid)   # HEAD request
                    pmc_art       = pmc_by_pmcid.get(pmcid)
                    ft            = pmc_art.full_text if pmc_art and pmc_art.full_text else None

                    with lock:
                        pmc_urls[article.pmid]  = article_page
                        if validated_pdf:
                            pdf_urls[article.pmid] = validated_pdf
                        if ft:
                            full_texts[article.pmid] = ft
                        enriched_map[article.pmid] = article

                with ThreadPoolExecutor(max_workers=10) as ex:
                    futs = [ex.submit(_pass1_worker, a) for a in real_pmid_articles]
                    for f in as_completed(futs):
                        try:
                            f.result()
                        except Exception as e:
                            logger.debug("Pass 1 worker error: %s", e)

        logger.info("Pass 1 NCBI PMC done: %d found", len(pmid_to_pmcid))

    # ══════════════════════════════════════════════════════════════════════
    # PASS 2 — Europe PMC search API (ALL articles, parallelised)
    #
    # One HTTP request per article → perfect for parallelism.
    # setdefault() ensures Pass 1 results are never overwritten.
    # Uses lock for all writes to shared dicts.
    # ══════════════════════════════════════════════════════════════════════
    with st.spinner(f"Pass 2/4 — Europe PMC: {n_total} articles in parallel…"):
        epmc_search = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

        def _pass2_worker(article: Article):
            raw = article.pmid.replace("EPMC:", "").strip()

            # EPMC native ID — direct fullTextXML (no search needed)
            if ":" not in raw and not raw.isdigit():
                if ":" in raw:
                    src_tag, epmc_id = raw.split(":", 1)
                else:
                    src_tag, epmc_id = "MED", raw
                article_url = f"https://europepmc.org/article/{src_tag}/{epmc_id}"
                xml_url     = (f"https://www.ebi.ac.uk/europepmc/webservices/rest/"
                               f"{src_tag}/{epmc_id}/fullTextXML")
                ft_resp = _try_url(xml_url)
                with lock:
                    pmc_urls.setdefault(article.pmid, article_url)
                    if ft_resp and article.pmid not in full_texts:
                        full_texts[article.pmid] = ft_resp.text[:60000]
                    enriched_map[article.pmid] = article
                return

            # Build search query
            if ":" not in raw and raw.isdigit():
                search_q = f'EXT_ID:{raw} AND SRC:MED'
            elif article.doi:
                search_q = f'DOI:"{_clean_doi(article.doi)}"'
            else:
                return  # nothing to search on

            try:
                params = {"query": search_q, "resultType": "core",
                          "pageSize": 1, "format": "json"}
                sr = requests.get(epmc_search, params=params, timeout=15)
                if sr.status_code != 200:
                    return
                results = sr.json().get("resultList", {}).get("result", [])
                if not results:
                    return

                hit     = results[0]
                src     = hit.get("source", "MED")
                ext_id  = hit.get("id", raw)
                pmcid_h = hit.get("pmcid", "")
                is_oa   = hit.get("isOpenAccess", "N") == "Y"

                article_url = f"https://europepmc.org/article/{src}/{ext_id}"

                # Resolve PDF URL (outside lock — may do HTTP)
                new_pdf_url = None
                if is_oa:
                    with lock:
                        already_has_pdf = article.pmid in pdf_urls
                    if not already_has_pdf:
                        if pmcid_h:
                            new_pdf_url = _validate_ncbi_pdf(pmcid_h)
                        if new_pdf_url is None and article.doi:
                            doi_c = _clean_doi(article.doi)
                            if doi_c.startswith("10.1101") or doi_c.startswith("10.64898"):
                                for server in ("biorxiv", "medrxiv"):
                                    new_pdf_url = (
                                        f"https://www.{server}.org/content/{doi_c}v1.full.pdf"
                                    )
                                    break
                        if new_pdf_url is None and not pmcid_h:
                            new_pdf_url = (
                                f"https://europepmc.org/articles/{ext_id}/pdf/render"
                            )

                # Fetch full text XML (outside lock)
                new_ft = None
                if is_oa:
                    with lock:
                        already_has_ft = article.pmid in full_texts
                    if not already_has_ft:
                        xml_url = (f"https://www.ebi.ac.uk/europepmc/webservices/rest/"
                                   f"{src}/{ext_id}/fullTextXML")
                        ft_resp = _try_url(xml_url)
                        if ft_resp:
                            new_ft = ft_resp.text[:60000]

                # Write all results under lock
                with lock:
                    pmc_urls.setdefault(article.pmid, article_url)
                    enriched_map[article.pmid] = article
                    if new_pdf_url and article.pmid not in pdf_urls:
                        pdf_urls[article.pmid] = new_pdf_url
                    if new_ft and article.pmid not in full_texts:
                        full_texts[article.pmid] = new_ft

            except Exception as e:
                logger.debug("Pass 2 worker %s: %s", article.pmid, e)

        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = [ex.submit(_pass2_worker, a) for a in all_articles]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    logger.debug("Pass 2 worker exception: %s", e)

    logger.info("Pass 2 Europe PMC done. Covered: %d, PDFs: %d",
                len(enriched_map), len(pdf_urls))

    # ══════════════════════════════════════════════════════════════════════
    # PASS 3 — Unpaywall (all articles with DOI, parallelised)
    #
    # One HTTP request per DOI → ideal for parallelism.
    # Adds publisher-specific stable PDF URLs (Wiley epdf, bioRxiv).
    # Rejects Silverchair/Atypon CDN token URLs (they expire within hours).
    # setdefault() ensures PMC PDF URLs from Passes 1+2 are never overwritten.
    # ══════════════════════════════════════════════════════════════════════
    articles_with_doi = [a for a in all_articles if a.doi]

    if articles_with_doi:
        with st.spinner(
            f"Pass 3/4 — Unpaywall: {len(articles_with_doi)} DOIs in parallel…"
        ):
            _EXPIRING_CDN_HOSTS = (
                "silverchair", "atypon", "token=",
                "watermark", "access_token", "Authorization=",
            )

            def _pass3_worker(article: Article):
                doi_clean = _clean_doi(article.doi)
                oa_url    = f"https://api.unpaywall.org/v2/{doi_clean}?email={oa_email}"
                try:
                    r = requests.get(oa_url, timeout=15,
                                     headers={"User-Agent": f"SR-Platform/1.0 ({oa_email})"})
                    if r.status_code == 404:
                        return
                    if r.status_code != 200:
                        return

                    data         = r.json()
                    best         = data.get("best_oa_location") or {}
                    oa_status    = data.get("oa_status", "")
                    direct_pdf   = best.get("url_for_pdf")
                    landing_page = best.get("url_for_landing_page")

                    # Publisher-specific stable PDF URL (computed outside lock)
                    stable_pdf = None

                    # Wiley OA: DOI prefix 10.1002 → stable epdf URL, no token needed
                    _is_wiley = (
                        doi_clean.startswith("10.1002") or
                        any(h in (landing_page or "") for h in
                            ("wiley.com", "onlinelibrary.wiley"))
                    )
                    if _is_wiley:
                        stable_pdf = (
                            f"https://onlinelibrary.wiley.com/doi/epdf/{doi_clean}"
                        )

                    # bioRxiv/medRxiv: stable versioned PDF URL, no token needed
                    elif doi_clean.startswith("10.1101") or doi_clean.startswith("10.64898"):
                        for server in ("biorxiv", "medrxiv"):
                            for ver in ("v1", "v2", "v3"):
                                candidate = (
                                    f"https://www.{server}.org/content/"
                                    f"{doi_clean}{ver}.full.pdf"
                                )
                                try:
                                    hd = requests.head(
                                        candidate, timeout=8, allow_redirects=True,
                                        headers={"User-Agent": f"SR-Platform/1.0 ({oa_email})"}
                                    )
                                    if hd.status_code == 200:
                                        stable_pdf = candidate
                                        break
                                except Exception:
                                    continue
                            if stable_pdf:
                                break

                    # Unpaywall direct PDF — only if non-expiring and different from landing
                    unpaywall_pdf = None
                    if direct_pdf and direct_pdf != landing_page:
                        is_expiring = any(cdn in direct_pdf for cdn in _EXPIRING_CDN_HOSTS)
                        if not is_expiring:
                            unpaywall_pdf = direct_pdf
                        else:
                            logger.debug("Unpaywall PDF for %s is CDN token (skipped): %s",
                                         article.pmid, direct_pdf[:60])

                    # Write under lock — setdefault preserves PMC URLs from Passes 1+2
                    with lock:
                        pmc_urls.setdefault(
                            article.pmid,
                            landing_page or f"https://doi.org/{doi_clean}"
                        )
                        # Priority: stable publisher URL > Unpaywall URL
                        # setdefault: never overwrite PMC PDF from Passes 1+2
                        chosen_pdf = stable_pdf or unpaywall_pdf
                        if chosen_pdf and article.pmid not in pdf_urls:
                            pdf_urls[article.pmid] = chosen_pdf

                        if oa_status in ("gold", "green", "bronze"):
                            enriched_map.setdefault(article.pmid, article)

                except Exception as e:
                    logger.debug("Pass 3 Unpaywall %s: %s", article.pmid, e)

            with ThreadPoolExecutor(max_workers=8) as ex:
                futs = [ex.submit(_pass3_worker, a) for a in articles_with_doi]
                for f in as_completed(futs):
                    try:
                        f.result()
                    except Exception as e:
                        logger.debug("Pass 3 worker exception: %s", e)

    logger.info("Pass 3 Unpaywall done. Total PDFs: %d", len(pdf_urls))

    # ══════════════════════════════════════════════════════════════════════
    # PASS 4 — CORE Aggregate (https://core.ac.uk)
    #
    # WHY CORE adds value beyond Passes 1-3:
    #   - Harvests from 10,000+ institutional repositories worldwide
    #   - Includes papers deposited directly by authors to their university
    #     repository that never appear in NCBI PMC or Europe PMC
    #   - Covers non-MEDLINE journals from developing countries
    #   - Grey literature: technical reports, preprints on non-standard servers
    #   - Returns direct downloadUrl (permanent, non-tokenised PDF links)
    #
    # Strategy:
    #   - Search by DOI first (most precise) → if no DOI, search by title
    #   - Only store pdf_url if CORE returns a non-empty downloadUrl
    #   - Uses setdefault() — never overwrites PMC/Unpaywall results
    #   - Parallelised: one API request per article with max_workers=8
    #   - Requires CORE_API_KEY in .env (free registration at core.ac.uk)
    # ══════════════════════════════════════════════════════════════════════
    from config.settings import CORE_API_KEY

    if CORE_API_KEY:
        articles_needing_core = [
            a for a in all_articles
            if a.pmid not in pdf_urls   # only articles that still lack a PDF
        ]

        if articles_needing_core:
            with st.spinner(
                f"Pass 4/4 — CORE: searching {len(articles_needing_core)} articles "
                f"without PDF yet…"
            ):
                _CORE_BASE   = "https://api.core.ac.uk/v3"
                _CORE_HEADER = {
                    "Authorization": f"Bearer {CORE_API_KEY}",
                    "User-Agent":    f"SR-Platform/1.0 ({oa_email})",
                }

                def _pass4_worker(article: Article):
                    """
                    Query CORE for one article.
                    Search priority: DOI → title.
                    Returns nothing — writes directly to shared dicts under lock.
                    """
                    doi_c = _clean_doi(article.doi) if article.doi else ""

                    # ── Search by DOI (most reliable) ─────────────────────
                    core_hit = None
                    if doi_c:
                        try:
                            resp = requests.get(
                                f"{_CORE_BASE}/search/works",
                                params={"q": f"doi:{doi_c}", "limit": 1},
                                headers=_CORE_HEADER,
                                timeout=12,
                            )
                            if resp.status_code == 200:
                                items = resp.json().get("results", [])
                                if items:
                                    core_hit = items[0]
                        except Exception as e:
                            logger.debug("CORE DOI search %s: %s", doi_c, e)

                    # ── Fallback: search by title ──────────────────────────
                    if not core_hit and article.title:
                        try:
                            # Limit title to first 10 words for best API match
                            short_title = " ".join(article.title.split()[:10])
                            resp = requests.get(
                                f"{_CORE_BASE}/search/works",
                                params={"q": short_title, "limit": 3},
                                headers=_CORE_HEADER,
                                timeout=12,
                            )
                            if resp.status_code == 200:
                                items = resp.json().get("results", [])
                                # Verify title match to avoid false positives
                                title_lower = article.title.lower()
                                for item in items:
                                    candidate_title = (item.get("title") or "").lower()
                                    # Accept if first 30 chars match
                                    if (candidate_title[:30] == title_lower[:30]
                                            and len(candidate_title) > 10):
                                        core_hit = item
                                        break
                        except Exception as e:
                            logger.debug("CORE title search %s: %s",
                                         article.pmid, e)

                    if not core_hit:
                        return

                    # ── Extract PDF and article page URLs from hit ─────────
                    # CORE v3 fields:
                    #   downloadUrl   — direct PDF URL (permanent, hosted by CORE)
                    #   fullTextLink  — external link to publisher/repository page
                    #   links[]       — list of {"url": ..., "type": "download"|"reader"}
                    download_url  = core_hit.get("downloadUrl") or ""
                    full_text_url = core_hit.get("fullTextLink") or ""
                    core_id       = core_hit.get("id", "")

                    # Also check links array for additional PDF URLs
                    if not download_url:
                        for link in core_hit.get("links", []):
                            if link.get("type") == "download" and link.get("url"):
                                download_url = link["url"]
                                break

                    # CORE article page on core.ac.uk
                    core_page = (
                        f"https://core.ac.uk/works/{core_id}" if core_id else ""
                    )

                    # Write under lock — setdefault preserves Passes 1-3 results
                    with lock:
                        if core_page:
                            pmc_urls.setdefault(article.pmid, core_page)
                        elif full_text_url:
                            pmc_urls.setdefault(article.pmid, full_text_url)

                        if download_url and article.pmid not in pdf_urls:
                            # CORE downloadUrls are permanent hosted PDFs
                            # — no tokens, no expiry. Safe to store directly.
                            pdf_urls[article.pmid] = download_url

                        enriched_map.setdefault(article.pmid, article)

                with ThreadPoolExecutor(max_workers=8) as ex:
                    futs = [ex.submit(_pass4_worker, a)
                            for a in articles_needing_core]
                    for f in as_completed(futs):
                        try:
                            f.result()
                        except Exception as e:
                            logger.debug("Pass 4 CORE worker exception: %s", e)

            logger.info("Pass 4 CORE done. Total PDFs now: %d", len(pdf_urls))

    else:
        logger.info("CORE_API_KEY not set — skipping Pass 4 (CORE search).")

    # ══════════════════════════════════════════════════════════════════════
    # MERGE full_texts into Article objects
    # ══════════════════════════════════════════════════════════════════════
    for article in all_articles:
        pmid = article.pmid
        ft   = full_texts.get(pmid)
        if pmid in enriched_map:
            if ft and not getattr(enriched_map[pmid], "full_text", None):
                enriched_map[pmid] = enriched_map[pmid].model_copy(update={"full_text": ft})
        elif pmid in pmc_urls or ft:
            enriched_map[pmid] = article.model_copy(update={"full_text": ft}) if ft else article

    # Fallback: any article with a DOI gets at minimum a DOI page link
    for article in all_articles:
        if article.doi and article.pmid not in pmc_urls:
            pmc_urls[article.pmid] = f"https://doi.org/{_clean_doi(article.doi)}"

    # ══════════════════════════════════════════════════════════════════════
    # COMPILE AND SAVE
    # ══════════════════════════════════════════════════════════════════════
    covered_articles = list(enriched_map.values())
    ft_articles      = [a for a in covered_articles if getattr(a, "full_text", None)]

    ft_count = 0
    if ft_articles:
        try:
            ft_count = article_repo.save_full_texts(ft_articles)
        except Exception as e:
            logger.error("Failed to save full texts: %s", e)

    _store(review_id, "unified_fulltext",      covered_articles if covered_articles else [])
    _store(review_id, "unified_fulltext_urls", pmc_urls)
    _store(review_id, "unified_fulltext_pdfs", pdf_urls)

    n_covered = len(covered_articles)
    n_ft      = len(ft_articles)
    n_pdfs    = len(pdf_urls)

    if n_covered == 0:
        st.warning(
            f"No open-access full text or database links found for any of the "
            f"{n_total} articles. This is normal for subscription-only journals."
        )
    else:
        st.success(
            f"✅ **{n_covered}** of {n_total} articles found · "
            f"**{n_ft}** have extractable full text · "
            f"**{n_pdfs}** have a direct PDF link."
        )
    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# MEDICAL SEARCH FORM
# ══════════════════════════════════════════════════════════════════════════════

def _render_pubmed_form(review_id, registry, search_repo, article_repo):
    from core.query_builder import STUDY_TYPE_FILTERS

    st.markdown("#### PICO Query Builder — Medical Research")

    clients   = registry.get_clients(ResearchDomain.MEDICAL)
    pubmed_cl = next((c for c in clients if c.source_name == "PubMed"), None)
    epmc_cl   = next((c for c in clients if c.source_name == "Europe PMC"), None)
    pmc_cl    = next((c for c in clients if c.source_name not in ("PubMed", "Europe PMC")
                      and "pmc" in c.source_name.lower()), None)

    from config.settings import CORE_API_KEY as _CORE_KEY
    _core_options = ["PubMed", "Europe PMC", "CORE"] if _CORE_KEY else ["PubMed", "Europe PMC"]

    source_name = st.selectbox(
        "Data Source",
        options=_core_options,
        key="medical_source_select",
        help=(
            "**PubMed**: NCBI's 35M+ article index. Publication Type filters.\n\n"
            "**Europe PMC**: 40M+ life-science articles including preprints. Free, no API key.\n\n"
            "**CORE**: 200M+ open-access articles from institutional repositories worldwide. "
            "Finds papers not indexed in PubMed or Europe PMC."
        ),
    )

    # ── UMLS toggle (outside form so it can read settings) ───────────────────
    from config.settings import UMLS_API_KEY as _UMLS_KEY
    _umls_available = bool(_UMLS_KEY)

    if _umls_available:
        use_umls = st.toggle(
            "🔬 Use UMLS concept expansion",
            value=True,
            key="use_umls_toggle",
            help=(
                "When enabled, each PICO field is mapped to a UMLS Concept Unique "
                "Identifier (CUI). The search query is enriched with:\n\n"
                "• **MeSH preferred term** → added as `[MeSH Terms]` in PubMed "
                "(high recall via MeSH explosion, includes narrower terms)\n"
                "• **Synonyms** from all UMLS source vocabularies (trade names, "
                "abbreviations, variant spellings)\n"
                "• **ChEMBL fallback** for new drugs not yet in UMLS\n\n"
                "Disable for simple keyword-only searches."
            ),
        )
    else:
        use_umls = False
        st.info(
            "💡 **UMLS expansion not active.** Add `UMLS_API_KEY` to your `.env` "
            "to enable MeSH term lookup and synonym expansion for higher-recall "
            "queries. Register free at https://uts.nlm.nih.gov/uts/signup-login"
        )

    with st.form("pico_form"):
        population   = st.text_input("Population",   placeholder="e.g. adults with CLL")
        intervention = st.text_input("Intervention", placeholder="e.g. venetoclax")
        comparison   = st.text_input("Comparison (optional)", placeholder="e.g. ibrutinib")
        outcome      = st.text_input("Outcome",      placeholder="e.g. overall survival")

        col1, col2, col3 = st.columns(3)
        year_from   = col1.number_input("Year From", 1900, 2100, 2015)
        year_to     = col2.number_input("Year To",   1900, 2100, 2025)
        max_results = col3.slider("Max Results", 1, 2000, 10)

        study_type = st.radio(
            "Study Type Filter",
            options=list(STUDY_TYPE_FILTERS.keys()),
            index=0, horizontal=True,
        )
        submitted = st.form_submit_button(f"🔍 Search {source_name}", type="primary")

    pending_key = f"pico_pending_{review_id}"
    pending     = st.session_state.get(pending_key, {})

    if submitted:
        new_fp = _pico_fingerprint(population, intervention, comparison,
                                   outcome, year_from, year_to, study_type)
        pico_changed   = _check_pico_change(review_id, new_fp)
        existing_total = counts["total"]

        if pico_changed and existing_total > 0:
            st.session_state[pending_key] = {
                "source_name":  source_name,
                "population":   population,
                "intervention": intervention,
                "comparison":   comparison,
                "outcome":      outcome,
                "year_from":    year_from,
                "year_to":      year_to,
                "max_results":  max_results,
                "study_type":   study_type,
                "new_fp":       new_fp,
                "use_umls":     use_umls,
            }
            st.rerun()
        else:
            _store_pico_fp(review_id, new_fp)
            _do_search(review_id, source_name, population, intervention,
                       comparison, outcome, year_from, year_to,
                       max_results, study_type, pubmed_cl, epmc_cl,
                       search_repo, article_repo, use_umls=use_umls)

    if pending:
        p = pending
        st.warning(
            "⚠️ **Your PICO query has changed** since your last search. "
            f"This review already has **{counts['total']}** screened articles. "
            "What would you like to do?"
        )
        st.caption(
            f"New query: **{p['population']}** / **{p['intervention']}** "
            f"/ {p['comparison']} / {p['outcome']} "
            f"({p['year_from']}–{p['year_to']}, {p['study_type']})"
        )

        c1, c2 = st.columns(2)
        with c1:
            if st.button(
                "➕ Add to this review", type="primary",
                key=f"pico_add_same_{review_id}",
                help="Keep all existing articles and add results of the new query too.",
            ):
                st.session_state.pop(pending_key, None)
                _store_pico_fp(review_id, p["new_fp"])
                _do_search(
                    review_id, p["source_name"], p["population"], p["intervention"],
                    p["comparison"], p["outcome"], p["year_from"], p["year_to"],
                    p["max_results"], p["study_type"],
                    pubmed_cl, epmc_cl, search_repo, article_repo,
                    use_umls=p.get("use_umls", False),
                )

        with c2:
            if st.button(
                "🆕 Create new review for this query",
                key=f"pico_new_review_{review_id}",
                help="Start a fresh review with the new query.",
            ):
                st.session_state.pop(pending_key, None)
                short_pico = f"{p['population'][:20]} / {p['intervention'][:20]}"
                review_repo.create_review(
                    f"Review: {short_pico} ({p['year_from']}–{p['year_to']})"
                )
                st.success("New review created — switch to it in the sidebar.")
                st.rerun()

        with st.columns(1)[0]:
            if st.button("✕ Cancel (keep original query)",
                         key=f"pico_cancel_{review_id}"):
                st.session_state.pop(pending_key, None)
                st.rerun()
        return

    current_articles = _load(review_id, f"articles_{source_name}", [])
    pubmed_articles  = _load(review_id, "articles_PubMed", []) or []
    epmc_articles    = _load(review_id, "articles_Europe PMC", []) or []
    core_articles    = _load(review_id, "articles_CORE", []) or []

    if not pubmed_articles and not epmc_articles and not core_articles:
        _refresh_prisma_from_db(review_id)
        return

    sort_by = st.selectbox(
        "Sort results by",
        ["Newest first", "Oldest first", "Most cited"],
        key=f"sort_medical_{review_id}",
    )

    # Multi-source dedup: 3-key matching across all sources
    # Catches: PubMed↔EPMC (pmid), PubMed/EPMC↔CORE (doi), title fallback
    n_searched = sum(1 for x in [pubmed_articles, epmc_articles, core_articles] if x)
    if n_searched > 1:
        combined, n_dupes = _deduplicate(pubmed_articles, epmc_articles, core_articles)
        _store(review_id, "n_duplicates_removed", n_dupes)
        _refresh_prisma_from_db(review_id)
        parts = []
        if pubmed_articles:
            parts.append(f"PubMed: **{len(pubmed_articles)}**")
        if epmc_articles:
            parts.append(f"Europe PMC: **{len(epmc_articles)}**")
        if core_articles:
            parts.append(f"CORE: **{len(core_articles)}**")
        parts.append(f"Duplicates: **{n_dupes}**")
        parts.append(f"Combined unique: **{len(combined)}**")
        st.info("📊 **Multi-source:** " + " | ".join(parts))

    tab_labels = []
    tab_data   = []
    if pubmed_articles:
        tab_labels.append(f"📘 PubMed ({len(pubmed_articles)})")
        tab_data.append(pubmed_articles)
    if epmc_articles:
        tab_labels.append(f"📗 Europe PMC ({len(epmc_articles)})")
        tab_data.append(epmc_articles)
    if core_articles:
        tab_labels.append(f"🟠 CORE ({len(core_articles)})")
        tab_data.append(core_articles)
    tab_labels.append("📄 Full Texts")

    tabs_out = st.tabs(tab_labels)

    # Track which source corresponds to each tab (for query lookup)
    _tab_sources = []
    if pubmed_articles:  _tab_sources.append("PubMed")
    if epmc_articles:    _tab_sources.append("Europe PMC")
    if core_articles:    _tab_sources.append("CORE")

    for i, (tab, articles_list) in enumerate(zip(tabs_out, tab_data)):
        src = _tab_sources[i] if i < len(_tab_sources) else ""
        with tab:
            # Persistent query — stored before st.rerun(), displayed after
            _q = _load(review_id, f"query_{src}")
            _ql = _load(review_id, f"query_label_{src}", "Query")
            if _q:
                with st.expander(
                    f" {_ql} — click to view / copy full query",
                    expanded=False
                ):
                    st.code(_q, language="text")
            _render_article_cards(articles_list, pmc_urls={}, sort_by=sort_by)

    with tabs_out[-1]:
        _render_unified_fulltext_tab(review_id, pmc_cl, article_repo)


def _do_search(review_id, source_name, population, intervention,
               comparison, outcome, year_from, year_to,
               max_results, study_type, pubmed_cl, epmc_cl,
               search_repo, article_repo, use_umls: bool = False):
    """
    Run a database search, optionally with UMLS concept expansion.

    When use_umls=True:
      1. Each PICO field is mapped to a UMLS CUI via /search/current
      2. MeSH preferred term retrieved via /atoms (sabs=MSH)
      3. Synonyms collected from all English vocabulary atoms
      4. ChEMBL fallback for new drugs not yet in UMLS
      5. PubMed query uses: MeSH[MeSH Terms] OR orig[tiab] OR syn1[tiab] ...
      6. Europe PMC query uses: TITLE/ABSTRACT for all terms
      7. The original PICO-based query is shown alongside the UMLS query

    When use_umls=False (or UMLS unavailable):
      Falls back to the existing build_query / build_epmc_query logic.
    """
    try:
        from core.query_builder import STUDY_TYPE_FILTERS, EPMC_STUDY_TYPE_FILTERS

        # ── UMLS expansion (runs for ALL sources including CORE) ─────────────
        # Build concept_set before routing, so CORE gets the expanded query too.
        if use_umls:
            _, concept_set = _build_umls_query(
                source_name="PubMed",   # field expansion is source-independent
                population=population, intervention=intervention,
                comparison=comparison, outcome=outcome,
                year_from=int(year_from), year_to=int(year_to),
                study_type=study_type,
            )
            if concept_set:
                _render_umls_expansion_detail(concept_set, source_name)
        else:
            concept_set = None

        # ── Route CORE to its dedicated handler (with UMLS query if available) ─
        if source_name == "CORE":
            core_q = (concept_set.core_query
                      if concept_set and concept_set.core_query else None)
            _do_core_search(
                review_id, population, intervention, comparison, outcome,
                year_from, year_to, max_results, search_repo, article_repo,
                expanded_query=core_q,
            )
            return

        # ── Build query for PubMed / Europe PMC ──────────────────────────────
        query = None
        if use_umls and concept_set:
            query = (concept_set.pubmed_query if source_name == "PubMed"
                     else concept_set.epmc_query) or None

        # ── Standard path (or UMLS fallback if expansion failed) ─────────────
        if not query:
            if source_name == "PubMed":
                query = build_query(
                    population=population, intervention=intervention,
                    comparison=comparison, outcome=outcome,
                    year_from=int(year_from), year_to=int(year_to),
                    study_type=study_type,
                )
            else:
                query = build_epmc_query(
                    population=population, intervention=intervention,
                    comparison=comparison, outcome=outcome,
                    year_from=int(year_from), year_to=int(year_to),
                    study_type=study_type,
                )

        # Store query persistently — displayed in results tab after rerun
        _store(review_id, f"query_{source_name}", query)
        _store(review_id, f"query_label_{source_name}",
               "🔬 UMLS-expanded" if (use_umls and concept_set) else "📝 Original PICO")

        client = pubmed_cl if source_name == "PubMed" else epmc_cl

        with st.spinner(f"Searching {source_name}…"):
            articles  = client.search_and_fetch(query, max_results)
            search_id = search_repo.create_search(
                review_id=review_id, query=query,
                n_results=len(articles),
                source_name=source_name,
            )
            result = article_repo.save_articles(articles, review_id, search_id)

        st.success(
            f"✅ **{len(articles)}** articles found | "
            f"{result['saved']} new | "
            f"{result['duplicates']} already in DB"
        )

        _store(review_id, f"articles_{source_name}", articles)
        _store(review_id, f"search_id_{source_name}", search_id)
        _store(review_id, "unified_fulltext", None)
        _refresh_prisma_from_db(review_id)

        if result["saved"] > 0:
            st.rerun()

    except ValueError as e:
        st.error(str(e))
    except Exception as e:
        logging.getLogger(__name__).exception(f"{source_name} search failed")
        st.error(f"Search error: {e}")


def _build_umls_query(source_name: str,
                      population: str, intervention: str,
                      comparison: str, outcome: str,
                      year_from: int, year_to: int,
                      study_type: str):
    """
    Build a UMLS-expanded query for PubMed or Europe PMC.

    Returns (query_string, ConceptSet) or (None, None) on failure.
    The caller falls back to the standard query builder if this returns None.
    """
    from core.concept_expander import get_concept_expander
    from core.query_builder import STUDY_TYPE_FILTERS, EPMC_STUDY_TYPE_FILTERS

    try:
        expander = get_concept_expander()
        if not expander.umls_available:
            logger.info("UMLS expander not available — falling back to standard query")
            return None, None

        # Get study-type filter strings for each database
        st_pubmed = STUDY_TYPE_FILTERS.get(study_type, "")
        st_epmc   = EPMC_STUDY_TYPE_FILTERS.get(study_type, "")

        with st.spinner("🔬 UMLS: mapping concepts and expanding synonyms…"):
            cs = expander.expand(
                population=population,
                intervention=intervention,
                comparison=comparison,
                outcome=outcome,
                year_from=year_from,
                year_to=year_to,
                study_type_filter_pubmed=st_pubmed,
                study_type_filter_epmc=st_epmc,
            )

        if source_name == "PubMed":
            query = cs.pubmed_query
        else:
            query = cs.epmc_query

        if not query:
            logger.warning("UMLS expansion produced empty query — falling back")
            return None, None

        return query, cs

    except Exception as e:
        logger.error("UMLS query build failed: %s", e)
        st.warning(f"⚠️ UMLS expansion failed ({e}). Falling back to standard query.")
        return None, None


def _render_umls_expansion_detail(concept_set, source_name: str) -> None:
    """
    Show an expandable panel with per-field UMLS expansion results.
    Lets the researcher verify what MeSH terms and synonyms were used.
    """
    with st.expander("🔬 UMLS Concept Expansion Detail", expanded=False):
        if not concept_set.umls_used:
            st.warning("UMLS was not used for any field — API may be unavailable.")
            return

        for label, fe in concept_set.fields:
            if not fe:
                continue
            cols = st.columns([1, 2, 2])
            cols[0].markdown(f"**{label}**")
            cols[0].caption(f"*{fe.original}*")

            if fe.concept:
                cols[1].markdown(f"**CUI:** `{fe.concept.cui}`")
                cols[1].markdown(f"**UMLS name:** {fe.concept.name}")
                if fe.mesh_term:
                    cols[1].markdown(f"**MeSH:** `{fe.mesh_term}`")
                if fe.concept.semantic_names:
                    cols[1].caption(
                        "Semantic type: " + ", ".join(fe.concept.semantic_names[:2])
                    )
            else:
                cols[1].caption(f"No UMLS concept found — using raw text")

            if fe.synonyms:
                cols[2].markdown("**Synonyms used:**")
                for syn in fe.synonyms[:6]:
                    cols[2].caption(f"  · {syn}")
            else:
                cols[2].caption("No synonyms added")

            src_badge = {"umls": "🔬 UMLS", "chembl": "💊 ChEMBL",
                         "original": "📝 Raw text"}.get(fe.source, fe.source)
            cols[2].caption(f"Source: {src_badge}")

        st.divider()
        st.caption(
            f"**Expansion log:** " + " | ".join(concept_set.expansion_log[:6])
        )


def _do_core_search(review_id, population, intervention, comparison,
                    outcome, year_from, year_to, max_results,
                    search_repo, article_repo,
                    expanded_query: str = None):
    """
    Search CORE Aggregate API (https://core.ac.uk).

    Parameters
    ----------
    expanded_query : UMLS-expanded query string (from concept_expander).
                     When provided, used instead of raw PICO text.
                     Contains synonyms in CORE Boolean free-text format.
    """
    import requests
    from models.schemas import Article, ArticleSource, ResearchDomain
    from config.settings import CORE_API_KEY, NCBI_EMAIL

    if not CORE_API_KEY:
        st.error("CORE_API_KEY not found in .env. Please add it and restart the app.")
        return

    # ── Build query: UMLS-expanded OR raw PICO fallback ───────────────────────
    if expanded_query:
        # Strip yearPublished clauses — CORE requires them as separate API params
        import re as _re
        query     = _re.sub(r' AND yearPublished[><=]+\d{4}', '', expanded_query).strip()
        umls_label = "🔬 UMLS-expanded"
    else:
        terms = [t.strip() for t in [population, intervention, comparison, outcome]
                 if t.strip()]
        if not terms:
            st.error("Please fill at least one PICO field to search CORE.")
            return
        query      = " AND ".join(f'("{t}")' for t in terms)
        umls_label = "📝 Original PICO"

    # yearPublished MUST be a separate API param — NOT embedded in q
    core_params: dict = {"q": query, "limit": max_results}
    if year_from:
        core_params["yearPublished"] = f">={int(year_from)}"

    # Store for persistent display in results tab
    _store(review_id, "query_CORE", query)
    _store(review_id, "query_label_CORE", umls_label)

    oa_email = NCBI_EMAIL or "research@example.com"
    headers  = {
        "Authorization": f"Bearer {CORE_API_KEY}",
        "User-Agent":    f"SR-Platform/1.0 ({oa_email})",
    }

    with st.spinner(f"Searching CORE for up to {max_results} articles…"):
        try:
            resp = requests.get(
                "https://api.core.ac.uk/v3/search/works",
                params=core_params,
                headers=headers,
                timeout=30,
            )
            if resp.status_code == 401:
                st.error("CORE API key rejected (401). Check CORE_API_KEY in .env.")
                return
            if resp.status_code == 500:
                # CORE Elasticsearch occasionally returns 500 with partial shard
                # failures but recovers immediately. Retry once automatically.
                logger.warning("CORE returned 500 — retrying once…")
                import time as _time; _time.sleep(1.5)
                resp = requests.get(
                    "https://api.core.ac.uk/v3/search/works",
                    params={"q": query, "limit": max_results},
                    headers=headers, timeout=30,
                )
            if resp.status_code != 200:
                st.error(f"CORE API error {resp.status_code}: {resp.text[:200]}")
                return
            results = resp.json().get("results", [])
        except Exception as e:
            st.error(f"CORE search failed: {e}")
            return

    if not results:
        st.info("No results from CORE. Try broadening your PICO terms.")
        return

    articles: list = []
    for hit in results:
        try:
            core_id  = str(hit.get("id", ""))
            title    = (hit.get("title") or "No title").strip()
            abstract = (hit.get("abstract") or "").strip()
            year     = str(hit.get("yearPublished") or "")
            doi      = (hit.get("doi") or "").strip()

            raw_authors = hit.get("authors") or []
            authors = []
            for a in raw_authors:
                name = (a.get("name") or
                        f"{a.get('lastName','')} {a.get('firstName','')}".strip()
                        ) if isinstance(a, dict) else str(a)
                if name.strip():
                    authors.append(name.strip())

            journal = (hit.get("publisher") or
                       (hit.get("journals") or [{}])[0].get("title", ""))

            download_url  = hit.get("downloadUrl") or ""
            full_text_url = hit.get("fullTextLink") or ""
            if not download_url:
                for link in hit.get("links", []):
                    if isinstance(link, dict) and link.get("type") == "download":
                        download_url = link.get("url", "")
                        break

            core_page   = f"https://core.ac.uk/works/{core_id}" if core_id else ""
            article_url = core_page or full_text_url or (
                f"https://doi.org/{doi}" if doi else ""
            )
            uid = doi or f"CORE:{core_id}"

            # Use ArticleSource("core") — requires CORE = "core" in ArticleSource enum.
            # If not yet added to schemas.py, falls back to EUROPE_PMC gracefully.
            try:
                _core_source = ArticleSource("core")
            except ValueError:
                _core_source = ArticleSource.EUROPE_PMC

            art = Article(
                pmid=uid,
                title=title,
                abstract=abstract,
                authors=authors,
                journal=journal,
                year=year,
                doi=doi or None,
                url=article_url,
                source=_core_source,
                domain=ResearchDomain.MEDICAL,
            )
            # Encode downloadUrl in full_text for card renderer (no schema change)
            if download_url:
                art = art.model_copy(update={"full_text": f"PDF:{download_url}"})

            articles.append(art)

        except Exception as e:
            logger.warning("CORE parse error: %s", e)

    if not articles:
        st.warning("CORE returned results but none could be parsed.")
        return

    search_id = search_repo.create_search(
        review_id=review_id, query=query,
        n_results=len(articles),
        source_name="CORE",
    )
    result = article_repo.save_articles(articles, review_id, search_id)

    st.success(
        f"✅ **{len(articles)}** CORE articles found | "
        f"{result['saved']} new | "
        f"{result['duplicates']} already in DB"
    )

    _store(review_id, "articles_CORE", articles)
    _store(review_id, "search_id_CORE", search_id)
    _store(review_id, "unified_fulltext", None)
    _refresh_prisma_from_db(review_id)

    if result["saved"] > 0:
        st.rerun()


def _refresh_prisma_from_db(review_id: int) -> None:
    sources = _get_prisma_sources_from_db(review_id)
    st.session_state[f"prisma_db_{review_id}"] = sources


# ── ML/AI form ────────────────────────────────────────────────────────────────

def _render_ml_form(review_id, registry, search_repo, article_repo):
    st.markdown("#### ML/AI Literature Search")
    clients    = registry.get_clients(ResearchDomain.ML_AI)
    client_map = {c.source_name: c for c in clients}

    source_name     = st.selectbox("Data Source", options=list(client_map.keys()),
                                   key="ml_source_select")
    selected_client = client_map[source_name]

    if source_name == "Semantic Scholar" and not os.environ.get("SEMANTIC_SCHOLAR_API_KEY"):
        st.info("💡 No Semantic Scholar API key — "
                "get one at https://www.semanticscholar.org/product/api")

    with st.form("ml_search_form"):
        topic        = st.text_input("Research Topic",
                                     placeholder="e.g. transformer attention mechanism")
        keywords     = st.text_input("Additional Keywords (comma-separated, optional)")
        venue_filter = st.multiselect("Filter by Conference (optional)",
                                     ["NeurIPS","ICML","ICLR","AAAI","IJCAI","CVPR","ACL","EMNLP"])
        col1, col2  = st.columns(2)
        year_from   = col1.number_input("Year From", 2010, 2026, 2019)
        year_to     = col2.number_input("Year To",   2010, 2026, 2025)
        max_results = st.slider("Max Results", 1, 50, 10)
        submitted   = st.form_submit_button(f"Search {source_name}", type="primary")

    if submitted:
        if not topic.strip():
            st.error("Please enter a research topic.")
            return
        try:
            query = build_ml_query(topic=topic, keywords=keywords,
                                   venues=venue_filter,
                                   year_from=int(year_from), year_to=int(year_to))
            _store(review_id, "query_CORE", query)
            _store(review_id, "query_label_CORE", "📝 Original PICO")

            with st.spinner(f"Searching {source_name}…"):
                result_data = selected_client.search_and_fetch(query, max_results)
                if isinstance(result_data, tuple):
                    articles, err_msg = result_data
                    if err_msg:
                        st.warning(f"⚠️ {err_msg}")
                else:
                    articles = result_data

                search_id = search_repo.create_search(
                    review_id=review_id, query=query, n_results=len(articles)
                )
                result = article_repo.save_articles(articles, review_id, search_id)

            st.success(f"✅ {result['saved']} new | {result['duplicates']} duplicates")
            _store(review_id, f"ml_{source_name}", articles)
            if result["saved"] > 0:
                st.rerun()

        except Exception as e:
            logging.getLogger(__name__).exception("ML search failed")
            st.error(f"Search error: {e}")

    stored = _load(review_id, f"ml_{source_name}", [])
    if stored:
        _store(review_id, f"ml_{source_name}", stored)

    all_ml_sources   = list(client_map.keys())
    tab_labels       = []
    tab_results_data = []

    SOURCE_ICONS = {
        "Semantic Scholar": "🔬",
        "OpenAlex":         "🔍",
    }

    for src in all_ml_sources:
        src_articles = _load(review_id, f"ml_{src}", [])
        if src_articles:
            icon = SOURCE_ICONS.get(src, "📄")
            tab_labels.append(f"{icon} {src} ({len(src_articles)})")
            tab_results_data.append((src, src_articles))

    if not tab_labels:
        return

    sort_by  = st.selectbox(
        "Sort by", ["Newest first", "Oldest first", "Most cited"],
        key=f"ml_sort_{review_id}"
    )
    tabs_out = st.tabs(tab_labels)
    for tab, (src, articles_list) in zip(tabs_out, tab_results_data):
        with tab:
            _render_article_cards(articles_list, pmc_urls={}, sort_by=sort_by)


# ══════════════════════════════════════════════════════════════════════════════
# Tabs
# ══════════════════════════════════════════════════════════════════════════════

_refresh_prisma_from_db(review_id)

tabs = st.tabs(["🔍 Search", "📋 Screening", "🤖 AI Analysis", "📊 PRISMA"])

with tabs[0]:
    domain_options = registry.domain_display_names()
    domain_choice  = st.selectbox("Research Domain",
                                  options=list(domain_options.keys()),
                                  format_func=lambda k: domain_options[k],
                                  key="domain_select")
    selected_domain = ResearchDomain(domain_choice)
    if selected_domain == ResearchDomain.MEDICAL:
        _render_pubmed_form(review_id, registry, search_repo, article_repo)
    else:
        _render_ml_form(review_id, registry, search_repo, article_repo)

with tabs[1]:
    render_screening_panel(review_id)

with tabs[2]:
    render_ai_analysis_panel(review_id)

with tabs[3]:
    render_prisma_diagram(review_id)













