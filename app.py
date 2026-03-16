# app.py
import logging
import streamlit as st

# ── Logging — configure before any other imports so module loggers work ───────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("sr_platform.log"),
    ],
)

# DATABASE
from storage.database import init_database
from storage.repository import (
    ReviewRepository,
    ArticleRepository,
    ScreeningRepository,
)

# CORE SEARCH
from core.query_builder import build_query
from core.pubmed import search_pubmed, fetch_articles

# AI PIPELINE
from pipeline.summarizer import summarize_with_llm
from pipeline.pico_extractor import extract_pico
from pipeline.relevance_scorer import score_relevance

# UI PANELS
from ui.prisma_panel import render_prisma_diagram
from storage.search_repository import SearchRepository

# ── Initialise ────────────────────────────────────────────────────────────────
init_database()

review_repo  = ReviewRepository()
article_repo = ArticleRepository()
screen_repo  = ScreeningRepository()
search_repo  = SearchRepository()

st.set_page_config(page_title="AI-Assisted Systematic Review", layout="wide")

# ── Sidebar: Review Management ────────────────────────────────────────────────
with st.sidebar:
    st.header("📋 Systematic Reviews")
    reviews = review_repo.list_reviews()

    if reviews:
        options = {r["title"]: r["id"] for r in reviews}
        selected = st.selectbox("Select Review", list(options.keys()))
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

if not review_id:
    st.title("Welcome")
    st.info("Create a new review in the sidebar to get started.")
    st.stop()

review = review_repo.get_review(review_id)
st.title(f"📚 {review['title']}")

# ── Dashboard Metrics ─────────────────────────────────────────────────────────
counts = article_repo.get_screening_counts(review_id)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Articles", counts["total"])
c2.metric("✅ Included",    counts["included"])
c3.metric("❌ Excluded",    counts["excluded"])
c4.metric("⏳ Pending",     counts["pending"])

# ── Tabs ──────────────────────────────────────────────────────────────────────
tabs = st.tabs(["🔍 Search PubMed", "🤖 AI Analysis", "📋 Screening", "📊 PRISMA"])


# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — PubMed Search
# ════════════════════════════════════════════════════════════════════════════════
with tabs[0]:
    st.subheader("PICO Search Builder")

    with st.form("pico_form"):
        population   = st.text_input("Population")
        intervention = st.text_input("Intervention")
        comparison   = st.text_input("Comparison (optional)")
        outcome      = st.text_input("Outcome")
        year_from    = st.number_input("Year From", min_value=1900, max_value=2100, value=2015)
        year_to      = st.number_input("Year To",   min_value=1900, max_value=2100, value=2024)
        max_results  = st.slider("Max Results", min_value=1, max_value=50, value=10)
        submitted    = st.form_submit_button("Search PubMed")

    if submitted:
        try:
            query = build_query(
                population=population,
                intervention=intervention,
                comparison=comparison,
                outcome=outcome,
                year_from=int(year_from),
                year_to=int(year_to),
            )
            st.write("### Generated Query")
            st.code(query)

            with st.spinner("Searching PubMed…"):
                pmids    = search_pubmed(query, max_results=max_results)
                articles = fetch_articles(pmids)

                search_id = search_repo.create_search(
                    review_id=review_id,
                    query=query,
                    n_results=len(pmids),
                )
                result = article_repo.save_articles(articles, review_id, search_id)

            st.success(
                f"✅ {result['saved']} new articles saved | "
                f"{result['duplicates']} duplicates skipped"
            )
            if result["saved"] > 0:
                st.rerun()   # Refresh dashboard counts immediately

        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            logging.getLogger(__name__).exception("Search failed")
            st.error(f"Search error: {e}")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — AI Analysis
# ════════════════════════════════════════════════════════════════════════════════
with tabs[1]:
    st.subheader("AI Analysis")
    # BUG FIXED: was calling get_articles_for_review() with a positional
    # `stage` arg that was not passed (default is fine), but the bigger issue
    # was Tab 2 showing nothing when the DB was empty due to the executescript
    # transaction bug. Once the DB bug is fixed, articles load normally here.
    articles = article_repo.get_articles_for_review(review_id)

    if not articles:
        st.info("No articles yet. Run a PubMed search first.")
    else:
        for article in articles:
            pmid         = article["pmid"]
            summary_key  = f"summary_{pmid}"
            pico_key     = f"pico_{pmid}"
            score_key    = f"score_{pmid}"

            with st.container():
                st.markdown(f"### {article['title']}")
                st.caption(
                    f"PMID: {pmid}  ·  "
                    f"{article.get('journal', '')} ({article.get('year', '')})  ·  "
                    f"[PubMed ↗](https://pubmed.ncbi.nlm.nih.gov/{pmid}/)"
                )

                with st.expander("Abstract", expanded=False):
                    st.write(article["abstract"] or "No abstract available.")

                col1, col2, col3 = st.columns(3)

                with col1:
                    if st.button("📝 Summarize", key=f"sum_{pmid}"):
                        with st.spinner("Generating summary…"):
                            st.session_state[summary_key] = summarize_with_llm(
                                article["abstract"]
                            )

                with col2:
                    if st.button("🔬 Extract PICO", key=f"pico_{pmid}"):
                        with st.spinner("Extracting PICO…"):
                            st.session_state[pico_key] = extract_pico(
                                article["abstract"]
                            )

                with col3:
                    if st.button("⭐ Score Relevance", key=f"score_{pmid}"):
                        with st.spinner("Scoring…"):
                            st.session_state[score_key] = score_relevance(
                                article["abstract"]
                            )

                # ── Display cached results ─────────────────────────────────
                if summary_key in st.session_state:
                    st.markdown("**🤖 AI Summary**")
                    st.write(st.session_state[summary_key])

                if pico_key in st.session_state:
                    st.markdown("**🔍 Extracted PICO**")
                    pico_obj = st.session_state[pico_key]
                    if hasattr(pico_obj, "model_dump"):
                        pico_data = pico_obj.model_dump()
                        if any(pico_data.values()):
                            st.json(pico_data)
                        else:
                            st.warning(
                                "PICO extraction returned empty fields. "
                                "This is expected with TinyLlama — results "
                                "will improve with a larger model."
                            )
                    else:
                        st.warning("Unexpected PICO result type.")

                if score_key in st.session_state:
                    score_val = st.session_state[score_key]
                    st.markdown("**⭐ Relevance Score**")
                    # BUG FIXED: previously score_val was None (parsed from
                    # "YES"), which st.write() renders as the boolean False.
                    # Now parse_score handles YES/NO → returns 8.0 or 2.0.
                    if score_val is not None:
                        st.metric(
                            label="Score (0–10)",
                            value=f"{score_val:.1f}",
                            help="8.0 = YES (clinical study in humans) | 2.0 = NO"
                        )
                    else:
                        st.warning("Could not parse a relevance score from model output.")

                st.divider()


# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 — Screening
# ════════════════════════════════════════════════════════════════════════════════
with tabs[2]:
    st.subheader("Title / Abstract Screening")

    # BEHAVIOUR CONFIRMED CORRECT:
    # When the user clicks Include or Exclude, save_decision() writes to
    # screening_decisions, st.rerun() fires, get_screening_counts() is called
    # again and pending = total - screened decreases by 1. This is the correct
    # and expected behaviour — it matches PRISMA workflow.
    #
    # Enhancement added: show the current decision badge on each article card
    # so reviewers can see and change their previous decision.

    articles = article_repo.get_articles_for_review(review_id)

    if not articles:
        st.info("No articles to screen. Run a PubMed search first.")
    else:
        # Summary progress bar
        total_a   = counts["total"]
        screened_a = total_a - counts["pending"]
        if total_a > 0:
            st.progress(screened_a / total_a,
                        text=f"Screened {screened_a} / {total_a} articles")

        DECISION_BADGE = {
            "include": "🟢 Included",
            "exclude": "🔴 Excluded",
            "unsure":  "🟡 Unsure",
            None:      "⬜ Pending",
        }

        for article in articles:
            pmid             = article["pmid"]
            current_decision = article.get("decision")

            with st.container():
                # Title row with current decision badge
                title_col, badge_col = st.columns([5, 1])
                with title_col:
                    st.markdown(f"**{article['title']}**")
                    st.caption(
                        f"PMID: {pmid}  ·  "
                        f"{article.get('journal', '')} ({article.get('year', '')})"
                    )
                with badge_col:
                    st.markdown(DECISION_BADGE.get(current_decision, "⬜ Pending"))

                with st.expander("Abstract", expanded=(current_decision is None)):
                    st.write(article.get("abstract") or "No abstract.")

                col1, col2, col3 = st.columns(3)

                if col1.button("✅ Include", key=f"inc_{pmid}",
                               type="primary" if current_decision == "include" else "secondary"):
                    screen_repo.save_decision(review_id, pmid, "title_abstract", "include")
                    st.rerun()

                if col2.button("❌ Exclude", key=f"exc_{pmid}",
                               type="primary" if current_decision == "exclude" else "secondary"):
                    screen_repo.save_decision(review_id, pmid, "title_abstract", "exclude")
                    st.rerun()

                if col3.button("❓ Unsure", key=f"uns_{pmid}",
                               type="primary" if current_decision == "unsure" else "secondary"):
                    screen_repo.save_decision(review_id, pmid, "title_abstract", "unsure")
                    st.rerun()

                st.divider()


# ════════════════════════════════════════════════════════════════════════════════
# TAB 4 — PRISMA
# ════════════════════════════════════════════════════════════════════════════════
with tabs[3]:
    st.subheader("PRISMA Flow Diagram")
    if counts["total"] == 0:
        st.info("No articles yet. Run a PubMed search to populate the PRISMA diagram.")
    else:
        render_prisma_diagram(review_id)
