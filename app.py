import streamlit as st

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
# --------------------------------------------------
# INITIALIZATION
# --------------------------------------------------

init_database()

review_repo = ReviewRepository()
article_repo = ArticleRepository()
screen_repo = ScreeningRepository()
search_repo = SearchRepository()

st.set_page_config(page_title="AI Assisted Systematic Review", layout="wide")


# --------------------------------------------------
# SIDEBAR : REVIEW MANAGEMENT
# --------------------------------------------------

with st.sidebar:

    st.header("Systematic Reviews")

    reviews = review_repo.list_reviews()

    if reviews:

        options = {r["title"]: r["id"] for r in reviews}

        selected = st.selectbox(
            "Select Review",
            list(options.keys())
        )

        review_id = options[selected]

    else:

        review_id = None
        st.info("Create a review to begin")

    st.divider()

    st.subheader("Create New Review")

    new_title = st.text_input("Review Title")

    if st.button("Create Review") and new_title:

        review_id = review_repo.create_review(new_title)

        st.success("Review created")

        st.rerun()


if not review_id:
    st.stop()


review = review_repo.get_review(review_id)

st.title(review["title"])


# --------------------------------------------------
# DASHBOARD METRICS
# --------------------------------------------------

counts = article_repo.get_screening_counts(review_id)

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Articles", counts["total"])
col2.metric("Included", counts["included"])
col3.metric("Excluded", counts["excluded"])
col4.metric("Pending", counts["pending"])


# --------------------------------------------------
# TABS
# --------------------------------------------------

tabs = st.tabs([
    "Search PubMed",
    "AI Analysis",
    "Screening",
    "PRISMA"
])


# --------------------------------------------------
# TAB 1 : PUBMED SEARCH
# --------------------------------------------------

with tabs[0]:

    st.subheader("PICO Search Builder")

    with st.form("pico_form"):

        population = st.text_input("Population")
        intervention = st.text_input("Intervention")
        comparison = st.text_input("Comparison (optional)")
        outcome = st.text_input("Outcome")

        year_from = st.number_input(
            "Year From",
            min_value=1900,
            max_value=2100,
            value=2015
        )

        year_to = st.number_input(
            "Year To",
            min_value=1900,
            max_value=2100,
            value=2024
        )

        max_results = st.slider(
            "Max Results",
            min_value=1,
            max_value=50,
            value=10
        )

        submitted = st.form_submit_button("Search PubMed")

    if submitted:

        try:

            query = build_query(
                population=population,
                intervention=intervention,
                comparison=comparison,
                outcome=outcome,
                year_from=year_from,
                year_to=year_to,
            )

            st.write("### Generated Query")
            st.code(query)

            with st.spinner("Searching PubMed..."):

                pmids = search_pubmed(query, max_results=max_results)
                articles = fetch_articles(pmids)

                search_id = search_repo.create_search(
                    review_id=review_id,
                    query=query,
                    n_results=len(pmids)
                )

                result = article_repo.save_articles(
                    articles,
                    review_id,
                    search_id
                )

            st.success(
                f"{result['saved']} new articles saved | "
                f"{result['duplicates']} duplicates"
            )

        except Exception as e:

            st.error(str(e))


# --------------------------------------------------
# TAB 2 : AI ANALYSIS
# --------------------------------------------------

with tabs[1]:

    st.subheader("AI Analysis")

    articles = article_repo.get_articles_for_review(review_id)

    if not articles:

        st.info("No articles available")

    else:

        for article in articles:

            with st.container():

                st.markdown(f"## {article['title']}")

                st.write(f"**PMID:** {article['pmid']}")
                st.write(f"**Journal:** {article['journal']} ({article['year']})")

                st.write("**Abstract:**")

                st.write(article["abstract"] if article["abstract"] else "No abstract")

                col1, col2, col3 = st.columns(3)

                summary_key = f"summary_{article['pmid']}"
                pico_key = f"pico_{article['pmid']}"
                score_key = f"score_{article['pmid']}"

                with col1:

                    if st.button("Summarize", key=f"sum_{article['pmid']}"):

                        with st.spinner("Generating summary..."):

                            summary = summarize_with_llm(article["abstract"])

                            st.session_state[summary_key] = summary

                with col2:

                    if st.button("Extract PICO", key=f"pico_{article['pmid']}"):

                        with st.spinner("Extracting PICO..."):

                            pico = extract_pico(article["abstract"])

                            st.session_state[pico_key] = pico

                with col3:

                    if st.button("Score Relevance", key=f"score_{article['pmid']}"):

                        with st.spinner("Scoring relevance..."):

                            score = score_relevance(article["abstract"])

                            st.session_state[score_key] = score

                if summary_key in st.session_state:

                    st.write("### AI Summary")
                    st.write(st.session_state[summary_key])

                if pico_key in st.session_state:

                    st.write("### Extracted PICO")
                    pico_obj = st.session_state[pico_key]

                    if hasattr(pico_obj, "model_dump"):
                        st.json(pico_obj.model_dump())
                    else:
                        st.warning("PICO extraction failed")

                if score_key in st.session_state:

                    st.write("### Relevance Score")
                    st.write(st.session_state[score_key])

                st.divider()


# --------------------------------------------------
# TAB 3 : SCREENING
# --------------------------------------------------

with tabs[2]:

    st.subheader("Title / Abstract Screening")

    articles = article_repo.get_articles_for_review(review_id)

    for article in articles:

        with st.container():

            st.markdown(f"### {article['title']}")

            st.write(article["abstract"])

            col1, col2, col3 = st.columns(3)

            if col1.button("Include", key=f"inc_{article['pmid']}"):

                screen_repo.save_decision(
                    review_id,
                    article["pmid"],
                    "title_abstract",
                    "include"
                )

                st.rerun()

            if col2.button("Exclude", key=f"exc_{article['pmid']}"):

                screen_repo.save_decision(
                    review_id,
                    article["pmid"],
                    "title_abstract",
                    "exclude"
                )

                st.rerun()

            if col3.button("Unsure", key=f"uns_{article['pmid']}"):

                screen_repo.save_decision(
                    review_id,
                    article["pmid"],
                    "title_abstract",
                    "unsure"
                )

                st.rerun()

            st.divider()


# --------------------------------------------------
# TAB 4 : PRISMA
# --------------------------------------------------

with tabs[3]:

    st.subheader("PRISMA Flow Diagram")

    render_prisma_diagram(review_id)