# ui/ai_analysis_panel.py v1 -  25th March, 2026
#
# AI Analysis panel — Tab 2 in the updated workflow.
#
# Workflow dependency (enforced here):
#   Screening must run first. Only articles with an "include" consensus
#   decision are passed into AI Analysis. This mirrors real-world SR
#   practice where AI-assisted analysis is performed only on eligible studies.
#
# Tabs within this panel:
#   1. Summarization    — structured 5-point summaries
#   2. PICO Extraction  — population, intervention, comparison, outcome
#   3. Quality Assessment — methodological quality (replaces binary scorer)
#   4. Data Pooling     — composite table for meta-analysis preparation
#
# Architecture:
#   - Each sub-tab operates independently so users can run only what they need.
#   - Results are cached in session_state per (review_id, pmid) to avoid
#     redundant LLM calls on re-renders.
#   - The Data Pooling tab aggregates across all included articles and
#     provides a one-click CSV export.

# ui/ai_analysis_panel.py v2 -  27th March
# #
# ── FIX 3: AI ANALYSIS CORRECT COUNTS + QUALITY SCORING ─────────────────────
#
# Problems fixed:
#
# (a) Wrong "pending" count
#     Old: counted articles with NO consensus (including genuine conflicts)
#     New: "pending" = articles where ZERO reviewers have voted at all.
#          Articles with a conflict (2+ reviewers, disagreement) are shown as
#          "conflict", not "pending". This matches what the dashboard shows.
#
# (b) Old conflict count shown after Editor resolution
#     Old: _get_consensus_decision() only checked a hard-coded list of special
#          keys ("final_resolved", "editor", "adjudicator"). But the actual key
#          stored by AdjudicationRepository.save_adjudication() is the Editor's
#          reviewer_id, e.g. "rev_editor". The check was missing this.
#     New: Check the adjudications table FIRST via adjudication_repo directly,
#          which is the authoritative source after resolution.
#
# (c) Quality scores all 0 / "Parse error"
#     Old: The QUALITY_ASSESSMENT_PROMPT ended with `{{` which the LLM
#          saw literally and produced malformed partial JSON. The prompt
#          is fixed in config/prompts.py (remove the trailing `{{`).
#     The parser here is also made more robust: it strips preamble text and
#     handles truncated outputs gracefully.
#
# (d) AI Analysis should use ALL included articles (from PubMed abstract
#     screening), not just PMC-subset articles.
#     This was already correct in the data flow (article_repo.get_articles_for_review
#     returns ALL articles for the review regardless of source), but the
#     consensus logic bug (b) above caused many articles to appear as
#     "unresolved conflict" even after the Editor had resolved them.
# ─────────────────────────────────────────────────────────────────────────────

# ui/ai_analysis_panel.py v3 -  27th March
#
# Fix for PICO / Quality / Data Pooling showing empty tables:
#   Root cause: The "Run All" button caused Streamlit to rerun immediately.
#   On that rerun, each expander checked `if run_all or run_one or cached`.
#   run_all was True, cached was None (LLM hadn't run yet), so the code
#   entered the display block, found no result, and appended nothing to
#   rows_for_table — yet the table still rendered (empty).
#   The LLM calls inside the spinners DID run, but because st.button state
#   only persists for ONE render cycle, on the NEXT rerun run_all was False
#   again, so the display loop only showed cached results — which were correctly
#   populated by then. However, the summary table was built from the FIRST
#   (empty) pass, not the post-LLM pass.
#
#   Fix: move ALL table-building to a separate pass that only reads from
#   session_state cache AFTER the per-article processing loop. This way:
#   - First pass: LLM runs, results stored in session_state
#   - Subsequent renders: results read from cache and displayed correctly
#   - Table is ALWAYS built only from articles that have cached results


# ui/ai_analysis_panel.py  — v4   31st March
#
# ROOT CAUSE OF "tabs still not working":
#
# The previous fix (separate loop for table) was correct in logic but missed
# one critical detail: after the LLM runs inside a st.spinner(), the result
# is stored in session_state — but the TABLE-BUILDING pass that reads
# session_state runs in the SAME render cycle, immediately after the loop.
# At that point the cache IS populated, so the table SHOULD appear.
#
# The actual remaining bug is different: the "run_all" button's True state
# only lasts for ONE render cycle (standard Streamlit behaviour).  When a user
# clicks "Run All", Streamlit reruns the script with run_all=True.
# The loop runs, LLM is called, results go into session_state.
# Then the table-building pass reads session_state and builds the table.
# This SHOULD work.  But if the LLM call raises ANY exception (even a soft
# warning), or if the model returns empty output, _set_cached() stores None
# or an empty object — and the table row is skipped.
#
# More importantly: the quality_assessor and data_pooler were calling
# run_inference() which calls the local LLM.  If the local model is NOT loaded
# (common in development / first run), run_inference() may return an empty
# string or raise, and _parse_quality() / _parse_study_data() return a
# default object with all-zero scores.  The session_state then holds a
# "bad" cached object, and subsequent rerenders show 0s.
#
# Additional bug: the cache check `if (run_all or run_one) and not cached`
# means that if a bad result (0-score QA) is cached from a failed LLM call,
# clicking "Run All" again does NOTHING because `cached` is truthy (it's a
# QualityAssessment object, even if all-zero).
#
# FIX: 
# 1. Add a "Force re-run (clear cache)" button per-tab so users can retry.
# 2. Show the raw LLM output in an expander for debugging when quality=unknown.
# 3. The quality assessor and data pooler now log and surface errors clearly.
# 4. run_all loop now calls st.rerun() at the end so the table appears
#    on the very next render (not relying on same-cycle table pass).




# ui/ai_analysis_panel.py
#
# CHANGES in this version:
#
# 1. DOI LINK IN EVERY ARTICLE EXPANDER
#    Every included article expander now shows a clickable DOI link so the
#    reviewer can always go back to the original paper while reviewing AI output.
#    Also shows PubMed link for PubMed-sourced articles.
#
# 2. AI DISCLAIMER BANNER
#    Each analysis tab shows a persistent ⚠️ disclaimer reminding users
#    not to rely on AI output blindly. This is a standard requirement for
#    responsible AI in clinical research tools and satisfies PRISMA/EQUATOR
#    reporting expectations for AI-assisted reviews.
#
# 3. ARCHITECTURE NOTES (for future scaling)
#    - All analysis functions (_tab_summarization etc.) receive the full
#      article dict, not just abstract, so future extensions (e.g. full-text
#      analysis, citation networks) can access all fields.
#    - Cache keys use (review_id, pmid, task) so adding new tasks only
#      requires adding a new _tab_* function and cache key suffix.
#    - The AI disclaimer is a single helper function _render_ai_disclaimer()
#      so it can be updated globally from one place.

import json
import logging
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from pipeline.summarizer       import summarize_with_llm
from pipeline.pico_extractor   import extract_pico
from pipeline.quality_assessor import assess_quality
from pipeline.data_pooler      import extract_study_data, build_composite_table, StudyData
from storage.repository import (
    ArticleRepository, ScreeningRepository, AdjudicationRepository
)

logger = logging.getLogger(__name__)

article_repo      = ArticleRepository()
screening_repo    = ScreeningRepository()
adjudication_repo = AdjudicationRepository()

SYSTEM_REVIEWER_IDS = {"final_resolved", "editor", "adjudicator"}


# ── AI Disclaimer ──────────────────────────────────────────────────────────────

def _render_ai_disclaimer() -> None:
    """
    Persistent disclaimer shown at the top of every AI analysis tab.
    Required for responsible AI use in clinical/research tools.
    Scalable: update this single function to change the disclaimer globally.
    """
    st.warning(
        "⚠️ **AI Output — Human Verification Required**\n\n"
        "The results below are generated by a Large Language Model (LLM) and may "
        "contain errors, hallucinations, or misinterpretations of the source text. "
        "**Do not use AI output as a substitute for reading the original papers.** "
        "Always verify key findings, statistics, and PICO elements against the "
        "original publication before including them in your systematic review. "
        "Use the **DOI / PubMed links** in each article panel to access the source."
    )


# ── Cache helpers ──────────────────────────────────────────────────────────────

def _ck(review_id, pmid, task):
    return f"ai_{review_id}_{pmid}_{task}"

def _get_cached(review_id, pmid, task):
    return st.session_state.get(_ck(review_id, pmid, task))

def _set_cached(review_id, pmid, task, value):
    st.session_state[_ck(review_id, pmid, task)] = value

def _clear_task_cache(review_id, articles, task):
    """Remove cached results for a task so LLM re-runs on next trigger."""
    for a in articles:
        key = _ck(review_id, a["pmid"], task)
        if key in st.session_state:
            del st.session_state[key]
    if task == "quality":
        for a in articles:
            k2 = _ck(review_id, a["pmid"], "quality_label")
            if k2 in st.session_state:
                del st.session_state[k2]


# ── DOI / source link helper ───────────────────────────────────────────────────

def _article_links_md(article: dict) -> str:
    """
    Return a markdown string of links for an article.
    Shown in every AI analysis expander so users can verify AI output
    against the original paper at any time.
    """
    links = []
    doi = article.get("doi") or ""
    if doi:
        doi_clean = doi.strip()
        for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
            if doi_clean.lower().startswith(prefix.lower()):
                doi_clean = doi_clean[len(prefix):]
                break
        links.append(f"[🌐 DOI](https://doi.org/{doi_clean.strip()})")

    source = str(article.get("source") or "")
    pmid   = article.get("pmid") or ""
    if source == "pubmed" and pmid and not pmid.startswith("EPMC:"):
        links.append(f"[🔗 PubMed](https://pubmed.ncbi.nlm.nih.gov/{pmid}/)")

    url = article.get("url") or ""
    if "europepmc" in url:
        links.append(f"[🟢 Europe PMC]({url})")

    return "  ·  ".join(links) if links else ""


# ── Consensus logic ────────────────────────────────────────────────────────────

def _get_consensus_decision(review_id, pmid):
    adj = adjudication_repo.get_adjudication(review_id, pmid)
    if adj and adj.get("final_decision"):
        return adj["final_decision"]
    all_decs = screening_repo.get_all_decisions_for_article(
        review_id, pmid, "title_abstract"
    )
    primary = {k: v for k, v in all_decs.items() if k not in SYSTEM_REVIEWER_IDS}
    if not primary:
        return None
    unique = set(primary.values())
    return unique.pop() if len(unique) == 1 else None

def _get_included_articles(review_id, all_articles):
    return [a for a in all_articles
            if _get_consensus_decision(review_id, a["pmid"]) == "include"]

def _count_pending(review_id, all_articles):
    count = 0
    for a in all_articles:
        all_decs = screening_repo.get_all_decisions_for_article(
            review_id, a["pmid"], "title_abstract"
        )
        primary = {k: v for k, v in all_decs.items() if k not in SYSTEM_REVIEWER_IDS}
        if len(primary) == 0:
            count += 1
    return count


# ── Main entry point ───────────────────────────────────────────────────────────

def render_ai_analysis_panel(review_id: int) -> None:
    all_articles = article_repo.get_articles_for_review(review_id)
    if not all_articles:
        st.info("📋 No articles found. Run a search first (Search tab).")
        return

    included  = _get_included_articles(review_id, all_articles)
    n_total   = len(all_articles)
    n_incl    = len(included)
    n_pending = _count_pending(review_id, all_articles)
    db_counts = article_repo.get_screening_counts(review_id)
    n_conflict= db_counts.get("conflict", 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Articles", n_total)
    c2.metric("✅ Included",    n_incl)
    c3.metric("⏳ Pending",     n_pending)
    c4.metric("⚠️ Conflicts",   n_conflict)

    if n_pending > 0:
        st.warning(f"⚠️ **{n_pending}** article(s) not yet screened. "
                   "Complete screening first.")
    if n_conflict > 0:
        st.warning(f"⚠️ **{n_conflict}** conflict(s) unresolved. "
                   "Switch to **Editor** → Screening → Conflicts to resolve.")

    if n_incl == 0:
        st.info("No articles included yet. Go to **Screening** and mark articles as Include.")
        return

    st.success(f"✅ **{n_incl}** article(s) included — AI analysis ready.")
    st.caption("AI Analysis uses the full PubMed abstract dataset.")
    st.divider()

    tab_sum, tab_pico, tab_qual, tab_pool = st.tabs([
        "📝 Summarization", "🔬 PICO Extraction",
        "⭐ Quality Assessment", "📊 Data Pooling",
    ])

    with tab_sum:
        _tab_summarization(review_id, included)
    with tab_pico:
        _tab_pico(review_id, included)
    with tab_qual:
        _tab_quality(review_id, included)
    with tab_pool:
        _tab_data_pool(review_id, included)


# ── Summarization ──────────────────────────────────────────────────────────────

def _tab_summarization(review_id, articles):
    st.markdown("### 📝 Structured Summaries")
    st.caption("5 bullet points: Study Design · Population · Intervention · Key Findings · Conclusion.")

    # AI disclaimer
    _render_ai_disclaimer()

    c1, c2 = st.columns([2, 1])
    run_all = c1.button("▶ Summarise All", key="sum_all", type="primary")
    if c2.button("🔄 Clear All Summaries", key="sum_clear"):
        _clear_task_cache(review_id, articles, "summary")
        st.rerun()

    ran_any = False
    for article in articles:
        pmid     = article["pmid"]
        abstract = article.get("abstract", "")
        cached   = _get_cached(review_id, pmid, "summary")
        links_md = _article_links_md(article)

        with st.expander(f"**{article['title'][:90]}**", expanded=False):
            # DOI and source links always visible
            meta_line = f"PMID: `{pmid}` · {article.get('journal','')}{article.get('year','')}"
            if links_md:
                st.caption(f"{meta_line}  |  {links_md}")
            else:
                st.caption(meta_line)

            if not abstract:
                st.warning("No abstract available.")
            run_one = st.button("Summarise", key=f"sum_{pmid}")

            if (run_all or run_one) and not cached:
                with st.spinner("Summarising…"):
                    try:
                        result = summarize_with_llm(abstract)
                        _set_cached(review_id, pmid, "summary", result or "(no output)")
                        ran_any = True
                    except Exception as e:
                        _set_cached(review_id, pmid, "summary", f"Error: {e}")

            result = _get_cached(review_id, pmid, "summary")
            if result:
                st.markdown("**Summary:**")
                st.markdown(result)

    if run_all and ran_any:
        st.rerun()


# ── PICO Extraction ────────────────────────────────────────────────────────────

def _tab_pico(review_id, articles):
    st.markdown("### 🔬 PICO Extraction")
    st.caption("Population · Intervention · Comparison · Outcome · Study Design · Sample Size")

    _render_ai_disclaimer()

    c1, c2 = st.columns([2, 1])
    run_all = c1.button("▶ Extract PICO for All", key="pico_all", type="primary")
    if c2.button("🔄 Clear All PICO", key="pico_clear"):
        _clear_task_cache(review_id, articles, "pico")
        st.rerun()

    ran_any = False
    for article in articles:
        pmid     = article["pmid"]
        abstract = article.get("abstract", "")
        cached   = _get_cached(review_id, pmid, "pico")
        links_md = _article_links_md(article)

        with st.expander(f"**{article['title'][:90]}**", expanded=False):
            meta_line = f"PMID: `{pmid}`"
            if links_md:
                st.caption(f"{meta_line}  |  {links_md}")
            else:
                st.caption(meta_line)

            run_one = st.button("Extract PICO", key=f"pico_{pmid}")
            if (run_all or run_one) and not cached:
                with st.spinner("Extracting PICO…"):
                    try:
                        result = extract_pico(abstract)
                        _set_cached(review_id, pmid, "pico", result)
                        ran_any = True
                    except Exception as e:
                        logger.error("PICO extraction error PMID %s: %s", pmid, e)

            pico = _get_cached(review_id, pmid, "pico")
            if pico:
                c1_, c2_ = st.columns(2)
                c1_.markdown(f"**Population:** {pico.population or '—'}")
                c1_.markdown(f"**Intervention:** {pico.intervention or '—'}")
                c2_.markdown(f"**Comparison:** {pico.comparison or '—'}")
                c2_.markdown(f"**Outcome:** {pico.outcome or '—'}")

    # Table from cache
    rows = []
    for a in articles:
        p = _get_cached(review_id, a["pmid"], "pico")
        if p:
            doi = a.get("doi") or ""
            rows.append({
                "PMID":         a["pmid"],
                "Title":        a["title"][:60],
                "DOI":          doi,
                "Population":   p.population or "—",
                "Intervention": p.intervention or "—",
                "Comparison":   p.comparison or "—",
                "Outcome":      p.outcome or "—",
            })
    if rows:
        st.divider()
        st.markdown(f"#### PICO Table ({len(rows)}/{len(articles)} articles)")
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("📥 Export PICO (CSV)", data=df.to_csv(index=False),
                           file_name="pico_extraction.csv", mime="text/csv")

    if run_all and ran_any:
        st.rerun()


# ── Quality Assessment ─────────────────────────────────────────────────────────

def _tab_quality(review_id, articles):
    st.markdown("### ⭐ Methodological Quality Assessment")
    st.caption("5 domains × 1–3 points each (max 15). High ≥12 · Moderate 8–11 · Low <8.")

    _render_ai_disclaimer()

    with st.expander("ℹ️ Domain definitions", expanded=False):
        st.markdown("""
| Domain | 1 (Low) | 2 (Moderate) | 3 (High) |
|---|---|---|---|
| Randomisation | No control | Controlled | RCT |
| Sample Size | <30 | 30–100 | >100 |
| Outcome Reporting | Subjective only | Mixed | Objective + stats |
| Follow-up | Not reported | <3 months | ≥3 months, complete |
| Comparator | None | Historical | Concurrent |

**Troubleshooting 0/Unknown scores:** This means the LLM either is not
loaded or returned output the parser could not interpret as JSON.
Steps: (1) Check your model is loaded. (2) Click **Clear & Retry** to
re-run. (3) Try a single article first using the individual "Assess" button.
""")

    c1, c2 = st.columns([2, 1])
    run_all = c1.button("▶ Assess Quality for All", key="qual_all", type="primary")
    if c2.button("🔄 Clear & Retry", key="qual_clear"):
        _clear_task_cache(review_id, articles, "quality")
        st.rerun()

    ICON = {"high": "🟢", "moderate": "🟡", "low": "🔴", "unknown": "⬜"}
    ran_any = False

    for article in articles:
        pmid     = article["pmid"]
        abstract = article.get("abstract", "")
        cached   = _get_cached(review_id, pmid, "quality")
        ql       = _get_cached(review_id, pmid, "quality_label") or "unknown"
        links_md = _article_links_md(article)

        with st.expander(
            f"{ICON.get(ql,'⬜')} **{article['title'][:80]}**",
            expanded=False
        ):
            meta_line = f"PMID: `{pmid}`"
            if links_md:
                st.caption(f"{meta_line}  |  {links_md}")
            else:
                st.caption(meta_line)

            run_one = st.button("Assess Quality", key=f"qual_{pmid}")
            if (run_all or run_one) and not cached:
                with st.spinner("Assessing…"):
                    try:
                        qa = assess_quality(abstract)
                        _set_cached(review_id, pmid, "quality", qa)
                        _set_cached(review_id, pmid, "quality_label", qa.overall_quality)
                        ran_any = True
                    except Exception as e:
                        logger.error("Quality error PMID %s: %s", pmid, e)
                        st.error(f"Quality assessment error: {e}")

            qa = _get_cached(review_id, pmid, "quality")
            if qa:
                icon = ICON.get(qa.overall_quality, "⬜")
                st.markdown(
                    f"**Overall:** {icon} **{qa.overall_quality.capitalize()}** "
                    f"— {qa.total_score}/15"
                )
                if qa.total_score == 0 and qa.overall_quality == "unknown":
                    st.error(
                        "Score is 0/Unknown — LLM output could not be parsed. "
                        "Check that your model is loaded, then click 🔄 Clear & Retry."
                    )
                c1_, c2_, c3_, c4_, c5_ = st.columns(5)
                c1_.metric("Randomisation", f"{qa.randomisation_score}/3")
                c2_.metric("Sample Size",   f"{qa.sample_size_score}/3")
                c3_.metric("Outcomes",      f"{qa.outcome_reporting_score}/3")
                c4_.metric("Follow-up",     f"{qa.followup_score}/3")
                c5_.metric("Comparator",    f"{qa.comparator_score}/3")
                if qa.quality_notes:
                    st.caption(f"📝 {qa.quality_notes}")

    rows = []
    for a in articles:
        qa = _get_cached(review_id, a["pmid"], "quality")
        if qa:
            rows.append({
                "PMID":          a["pmid"],
                "Title":         a["title"][:55],
                "DOI":           a.get("doi") or "",
                "Score":         qa.total_score,
                "Quality":       qa.overall_quality.capitalize(),
                "Randomisation": qa.randomisation_score,
                "Sample Size":   qa.sample_size_score,
                "Outcomes":      qa.outcome_reporting_score,
                "Follow-up":     qa.followup_score,
                "Comparator":    qa.comparator_score,
                "Notes":         qa.quality_notes or "",
            })
    if rows:
        st.divider()
        st.markdown(f"#### Quality Table ({len(rows)}/{len(articles)})")
        df = pd.DataFrame(rows).sort_values("Score", ascending=False)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("📥 Export Quality (CSV)", data=df.to_csv(index=False),
                           file_name="quality_assessment.csv", mime="text/csv")

    if run_all and ran_any:
        st.rerun()


# ── Data Pooling ───────────────────────────────────────────────────────────────

def _tab_data_pool(review_id, articles):
    st.markdown("### 📊 Data Pooling for Meta-Analysis")
    st.caption("Extracts structured data from each included study for MA preparation.")

    _render_ai_disclaimer()

    with st.expander("ℹ️ Fields extracted", expanded=False):
        st.markdown(
            "Sample size · Age group · Sex · Severity/baseline · "
            "Intervention · Comparator · Follow-up · Primary outcome + result · "
            "Secondary outcomes · Adverse events · Statistical significance · "
            "Study design · Country/setting"
        )

    c1, c2 = st.columns([2, 1])
    run_all = c1.button("▶ Extract Data from All", key="pool_all", type="primary")
    if c2.button("🔄 Clear & Retry", key="pool_clear"):
        _clear_task_cache(review_id, articles, "data_pool")
        st.rerun()

    missing_abstract = 0
    ran_any          = False

    for article in articles:
        pmid     = article["pmid"]
        abstract = article.get("abstract", "")
        title    = article.get("title", "")
        cached   = _get_cached(review_id, pmid, "data_pool")
        links_md = _article_links_md(article)

        if not abstract:
            missing_abstract += 1

        with st.expander(f"**{title[:80]}**", expanded=False):
            meta_line = f"PMID: `{pmid}` · {article.get('journal','')}{article.get('year','')}"
            if links_md:
                st.caption(f"{meta_line}  |  {links_md}")
            else:
                st.caption(meta_line)

            if not abstract:
                st.warning("No abstract — extraction will produce minimal data.")
            run_one = st.button("Extract Data", key=f"pool_{pmid}")

            if (run_all or run_one) and not cached:
                with st.spinner("Extracting structured data…"):
                    try:
                        sd = extract_study_data(abstract, pmid=pmid, title=title)
                        _set_cached(review_id, pmid, "data_pool", sd)
                        ran_any = True
                    except Exception as e:
                        logger.error("Data pool error PMID %s: %s", pmid, e)
                        st.error(f"Extraction error: {e}")

            sd = _get_cached(review_id, pmid, "data_pool")
            if sd:
                row  = sd.to_table_row()
                keys = [k for k in row if k not in ("PMID", "Title")]
                cols = st.columns(2)
                for i, k in enumerate(keys):
                    cols[i % 2].markdown(f"**{k}:** {row[k] or '—'}")

    if missing_abstract > 0:
        st.warning(f"⚠️ {missing_abstract} article(s) have no abstract. "
                   "Run PMC Full-Text for richer extraction.")

    # Composite table from cache
    all_sd: List[StudyData] = []
    for a in articles:
        sd = _get_cached(review_id, a["pmid"], "data_pool")
        if sd:
            all_sd.append(sd)

    if all_sd:
        st.divider()
        st.markdown(f"#### 📋 Composite Table ({len(all_sd)}/{len(articles)} studies)")
        df = pd.DataFrame(build_composite_table(all_sd))

        # Add DOI column to composite table for easy reference
        doi_map = {a["pmid"]: (a.get("doi") or "") for a in articles}
        if "PMID" in df.columns:
            df.insert(2, "DOI", df["PMID"].map(lambda p: doi_map.get(p, "")))

        st.dataframe(df, use_container_width=True, hide_index=True)
        c1_, c2_ = st.columns(2)
        with c1_:
            st.download_button("📥 CSV", data=df.to_csv(index=False),
                               file_name=f"data_pooling_{review_id}.csv",
                               mime="text/csv", use_container_width=True)
        with c2_:
            st.download_button("📥 JSON",
                               data=json.dumps([sd.to_dict() for sd in all_sd], indent=2),
                               file_name=f"data_pooling_{review_id}.json",
                               mime="application/json", use_container_width=True)
        st.divider()
        _pool_summary(df)

    if run_all and ran_any:
        st.rerun()


def _pool_summary(df):
    st.markdown("#### Quick Summary")
    c1, c2, c3 = st.columns(3)
    if "Study Design" in df.columns:
        designs = df["Study Design"].value_counts()
        c1.markdown("**Study Designs:**")
        for d, n in designs.items():
            c1.markdown(f"- {d}: **{n}**")
    if "Primary Result" in df.columns:
        reported = (df["Primary Result"] != "Not reported").sum()
        c2.metric("Outcomes Reported", f"{reported}/{len(df)}")
    if "Comparator" in df.columns:
        c2.metric("Comparator Available",
                  (df["Comparator"] != "Not reported").sum())
    if "Country/Setting" in df.columns:
        countries = df["Country/Setting"].value_counts().head(5)
        c3.markdown("**Top Countries:**")
        for country, n in countries.items():
            c3.markdown(f"- {country}: **{n}**")














