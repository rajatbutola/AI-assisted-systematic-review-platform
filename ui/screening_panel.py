# ui/screening_panel.py - v1 25th March
# Full replacement — complete multi-reviewer screening UI with conflict detection,
# adjudication workflow, and agreement statistics.

# 27th March
# ui/screening_panel.py v2 
#
# ── FIX 2: BLINDED SCREENING ─────────────────────────────────────────────────
#
# Root cause of the bug:
#   _render_article_card() always showed ALL reviewer decisions in the
#   "primary_decisions" column grid — regardless of who was currently logged in.
#   Reviewer 2 could see Reviewer 1's icons in real time.
#
# Fix:
#   Primary reviewers (non-Editor) only see:
#     (a) Their own decision badge (top-right of the card header)
#     (b) A count of "X reviewer(s) have decided" but NOT what they decided
#     (c) Their own voting buttons
#   The Editor sees everyone's decisions (needed for conflict resolution).
#   After adjudication, the resolved decision is visible to all.
#
#   This matches the Cochrane dual-blind standard: independent reviewers work
#   without knowledge of each other's choices until the conflict-resolution stage.
# ─────────────────────────────────────────────────────────────────────────────


# FIX: Conflict shown to Reviewer 2 immediately when they make a decision.
#
# Root cause: get_conflicts() returns any article where >= 2 reviewers have
# made DIFFERENT decisions. As soon as Reviewer 2 votes differently from
# Reviewer 1, the article enters the conflict set. The article card then
# shows "Conflict" — which tells Reviewer 2 that Reviewer 1 disagreed,
# violating blinding.
#
# Fix: Primary reviewers NEVER see the conflict badge or warning.
# The conflict UI is ONLY shown to the Editor role.
# Primary reviewers see only:
#   - Their own decision badge
#   - A count "N reviewer(s) have decided" (no decisions revealed)
#   - Their own voting buttons
# The prefix on titles is also suppressed for non-Editors.
# The conflict_pmids set is still fetched (for Editor view) but is not
# exposed to primary reviewers in any form.





# ui/screening_panel.py
#
# FIXES in this version:
#
# 1. CONSISTENT ARTICLE ORDER FOR ALL REVIEWERS
#    Root cause: after st.rerun(), articles were re-fetched from the DB in
#    insertion order. Reviewer 1 (who screened all articles) ended up with a
#    different visual order than Reviewer 2 (who had not screened yet), because
#    the filter "Pending (no decision from me)" removes decided articles, making
#    the next undecided article "jump" to the top.
#
#    Fix: Default sort is "Pending first, then by title" — undecided articles
#    always appear at the top in alphabetical order, decided ones at the bottom.
#    A sort control lets the user change this. This is deterministic and
#    identical for all reviewers regardless of their screening progress.
#
# 2. Article cards use st.container() with a stable key so Streamlit does not
#    re-order them on rerun.


# ui/screening_panel.py
#
# FIXES in this version:
#
# 1. CONSISTENT ARTICLE ORDER FOR ALL REVIEWERS
#    Root cause: after st.rerun(), articles were re-fetched from the DB in
#    insertion order. Reviewer 1 (who screened all articles) ended up with a
#    different visual order than Reviewer 2 (who had not screened yet), because
#    the filter "Pending (no decision from me)" removes decided articles, making
#    the next undecided article "jump" to the top.
#
#    Fix: Default sort is "Pending first, then by title" — undecided articles
#    always appear at the top in alphabetical order, decided ones at the bottom.
#    A sort control lets the user change this. This is deterministic and
#    identical for all reviewers regardless of their screening progress.
#
# 2. Article cards use st.container() with a stable key so Streamlit does not
#    re-order them on rerun.


# ui/screening_panel.py — v3
#
# Stage 1: Title/Abstract screening with per-card PICO extraction + keyword highlighting
# Stage 2: Full-text screening with per-card AI summary
# Submit-button pattern: nothing saved to DB until Submit clicked
# Per-reviewer decision summary table with CSV download

import re
import json
import logging
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional

from storage.repository import (
    ArticleRepository, ScreeningRepository, AdjudicationRepository,
)
from pipeline.pico_extractor import extract_pico
from pipeline.summarizer     import summarize_with_llm

logger = logging.getLogger(__name__)

article_repo      = ArticleRepository()
screening_repo    = ScreeningRepository()
adjudication_repo = AdjudicationRepository()

# ── Constants ──────────────────────────────────────────────────────────────────
DECISION_EMOJI = {"include":"🟢","exclude":"🔴","unsure":"🟡",None:"⬜"}
DECISION_LABEL = {"include":"Include","exclude":"Exclude","unsure":"Unsure",None:"Pending"}
SYSTEM_REVIEWER_IDS = {"final_resolved","editor","adjudicator"}

REVIEWER_DISPLAY = {
    "rev_reviewer_1":"Reviewer 1","rev_reviewer_2":"Reviewer 2",
    "rev_reviewer_3":"Reviewer 3","rev_editor":"Editor",
}

EXCLUDE_REASONS = {
    "Wrong population":"wrong_population","Wrong intervention":"wrong_intervention",
    "Wrong comparator":"wrong_comparator","Wrong outcome":"wrong_outcome",
    "Wrong study design":"wrong_study_design","Animal study":"animal_study",
    "Conference abstract":"conference_abstract","Duplicate":"duplicate",
    "Language barrier":"language_barrier","Sample size too small":"small_sample",
    "No control group":"no_control","Other":"other",
}

UNSURE_REASONS = {
    "Unclear population":"unclear_population","Unclear outcome":"unclear_outcome",
    "Needs full text":"needs_full_text","Borderline design":"borderline_design",
    "Possibly duplicate":"possibly_duplicate","Other":"other",
}

FULLTEXT_EXCLUDE_REASONS = {
    "Wrong outcome measure":"wrong_outcome_measure",
    "Insufficient follow-up":"insufficient_followup",
    "Data not extractable":"data_not_extractable",
    "Protocol deviation":"protocol_deviation",
    "No control group":"no_control","Wrong population":"wrong_population",
    "Wrong intervention":"wrong_intervention","Animal study":"animal_study",
    "Other":"other",
}

# ── Reason helpers ─────────────────────────────────────────────────────────────
def _parse_reason(reason_str: str) -> tuple:
    if not reason_str:
        return [], ""
    parts = reason_str.split("||", 1)
    tags  = [t.strip() for t in parts[0].split(",") if t.strip()] if parts[0].strip() else []
    note  = parts[1].strip() if len(parts) > 1 else ""
    return tags, note

def _build_reason(tags: list, note: str) -> str:
    s = ",".join(tags)
    return f"{s}||{note.strip()}" if note.strip() else s

def _render_reason_display(reason_str: str, decision: str) -> None:
    tags, note = _parse_reason(reason_str)
    if not tags and not note:
        return
    all_reasons = {**EXCLUDE_REASONS, **UNSURE_REASONS, **FULLTEXT_EXCLUDE_REASONS}
    _tag_bg = {"exclude":"#FFE4E6","unsure":"#FEF3C7","include":"#D1FAE5"}
    _tag_fg = {"exclude":"#9F1239","unsure":"#92400E","include":"#065F46"}
    bg = _tag_bg.get(decision,"#F0EEE9")
    fg = _tag_fg.get(decision,"#64748B")
    parts = []
    for tag in tags:
        label = next((k for k,v in all_reasons.items() if v==tag), tag)
        parts.append(
            f'<span style="display:inline-flex;align-items:center;font-size:0.7rem;'
            f'font-weight:600;padding:0.15rem 0.55rem;border-radius:999px;'
            f'background:{bg};color:{fg};border:1px solid {fg}33;margin:0.1rem;">'
            f'{label}</span>'
        )
    if parts:
        st.markdown(
            '<div style="display:flex;flex-wrap:wrap;gap:0.3rem;margin-top:0.4rem;">'
            +"".join(parts)+"</div>", unsafe_allow_html=True
        )
    if note:
        st.caption(f"📝 {note}")

def _reviewer_label(rev_id: str) -> str:
    return REVIEWER_DISPLAY.get(rev_id, rev_id.replace("rev_","").replace("_"," ").title())

# ── AI cache helpers ────────────────────────────────────────────────────────────
def _ck(review_id, pmid, task):
    return f"sc_{review_id}_{pmid}_{task}"

def _get_cached(review_id, pmid, task):
    return st.session_state.get(_ck(review_id, pmid, task))

def _set_cached(review_id, pmid, task, value):
    st.session_state[_ck(review_id, pmid, task)] = value

# ── PICO keyword highlighter ───────────────────────────────────────────────────
def _highlight_abstract(text: str, pico_terms: list) -> str:
    """Wrap PICO keywords in yellow highlight spans (case-insensitive)."""
    if not text or not pico_terms:
        return text or ""
    # Escape HTML first
    import html as html_mod
    result = html_mod.escape(text)
    for term in sorted(set(pico_terms), key=len, reverse=True):
        if not term or len(term) < 3:
            continue
        pattern = re.compile(re.escape(term), re.IGNORECASE)
        result = pattern.sub(
            lambda m: (
                f'<mark style="background:#FEF08A;color:#78350F;'
                f'border-radius:2px;padding:0 2px;">{m.group()}</mark>'
            ),
            result
        )
    return result

def _get_pico_terms() -> list:
    """Extract PICO search terms from session state for highlighting."""
    terms = []
    for key in ["population","intervention","comparison","outcome"]:
        val = st.session_state.get(key, "") or ""
        if val.strip():
            terms.extend([w.strip() for w in val.split() if len(w.strip()) >= 3])
    return terms

# ── Conflict helpers ────────────────────────────────────────────────────────────
def _classify_conflict(all_decisions: dict) -> str:
    primary = {k: v for k, v in all_decisions.items() if k not in SYSTEM_REVIEWER_IDS}
    vals = list(primary.values())
    if "include" in vals and "exclude" in vals:
        return "Include vs Exclude"
    if "include" in vals and "unsure" in vals:
        return "Include vs Unsure"
    return "Exclude vs Unsure"

def _sort_articles(articles, sort_key, my_decisions):
    if sort_key == "Pending first, then A→Z":
        return sorted(articles, key=lambda a: (0 if my_decisions.get(a["pmid"]) is None else 1,
                                               (a.get("title") or "").lower()))
    if sort_key == "Decided first, then A→Z":
        return sorted(articles, key=lambda a: (1 if my_decisions.get(a["pmid"]) is None else 0,
                                               (a.get("title") or "").lower()))
    if sort_key == "A → Z (title)":
        return sorted(articles, key=lambda a: (a.get("title") or "").lower())
    if sort_key == "Z → A (title)":
        return sorted(articles, key=lambda a: (a.get("title") or "").lower(), reverse=True)
    if sort_key == "Newest first":
        return sorted(articles, key=lambda a: int(a.get("year") or 0), reverse=True)
    if sort_key == "Oldest first":
        return sorted(articles, key=lambda a: int(a.get("year") or 9999))
    return articles

# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
def render_screening_panel(review_id: int) -> None:
    current_reviewer_id = st.session_state.get("current_reviewer_id","rev_reviewer_1")
    is_arbiter = "editor" in current_reviewer_id.lower()

    if not is_arbiter:
        st.info(
            f"🔒 **Blinded screening** — you are **{_reviewer_label(current_reviewer_id)}**. "
            "Switch to **Editor** to view all decisions and resolve conflicts."
        )

    subtabs = st.tabs([
        "📋 Stage 1 — Title/Abstract",
        "📄 Stage 2 — Full Text",
        "⚠️ Conflicts",
        "📊 Agreement & Summary",
    ])

    with subtabs[0]:
        _render_stage1(review_id, current_reviewer_id, is_arbiter)
    with subtabs[1]:
        _render_stage2(review_id, current_reviewer_id, is_arbiter)
    with subtabs[2]:
        _render_conflict_tab(review_id, current_reviewer_id, is_arbiter)
    with subtabs[3]:
        _render_agreement_stats(review_id)


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1 — TITLE / ABSTRACT SCREENING
# ══════════════════════════════════════════════════════════════════════════════
def _render_stage1(review_id: int, current_reviewer_id: str, is_arbiter: bool) -> None:
    articles = article_repo.get_articles_for_review(review_id, current_reviewer_id,
                                                     stage="title_abstract")
    if not articles:
        st.info("No articles to screen. Run a search first.")
        return

    my_decisions = {a["pmid"]: a.get("decision") for a in articles}
    decided_count = sum(1 for d in my_decisions.values() if d is not None)
    total_count   = len(articles)

    # Controls
    ctrl1, ctrl2, ctrl3 = st.columns([3, 3, 1])
    with ctrl1:
        filter_view = st.radio(
            "Show:", ["All","Pending","Decided","Conflicts (Editor)"],
            index=0, horizontal=True, key="s1_filter",
        )
    with ctrl2:
        sort_key = st.selectbox(
            "Sort by",
            ["Pending first, then A→Z","Decided first, then A→Z",
             "A → Z (title)","Newest first","Oldest first"],
            key="s1_sort",
        )
    with ctrl3:
        st.metric("Progress", f"{decided_count}/{total_count}")
    if total_count > 0:
        st.progress(decided_count/total_count)
    st.divider()

    conflict_pmids: set = set()
    if is_arbiter or filter_view == "Conflicts (Editor)":
        conflict_pmids = {c["pmid"] for c in
                          screening_repo.get_conflicts(review_id,"title_abstract")}

    filtered = []
    for a in articles:
        pmid   = a["pmid"]
        my_dec = my_decisions.get(pmid)
        if filter_view == "Pending" and my_dec is not None: continue
        if filter_view == "Decided" and my_dec is None:    continue
        if filter_view == "Conflicts (Editor)" and pmid not in conflict_pmids: continue
        filtered.append(a)

    if not filtered:
        st.info(f"No articles match: {filter_view}")
        return

    filtered = _sort_articles(filtered, sort_key, my_decisions)
    st.caption(f"Showing {len(filtered)} of {total_count} articles")
    pico_terms = _get_pico_terms()

    for article in filtered:
        _render_s1_card(article, review_id, current_reviewer_id, is_arbiter,
                        conflict_pmids, pico_terms)


def _render_s1_card(article, review_id, current_reviewer_id, is_arbiter,
                    conflict_pmids, pico_terms):
    pmid = article["pmid"]
    all_decisions = screening_repo.get_all_decisions_for_article(
        review_id, pmid, "title_abstract")
    adjudication  = adjudication_repo.get_adjudication(review_id, pmid)
    my_decision   = all_decisions.get(current_reviewer_id)
    has_conflict  = is_arbiter and (pmid in conflict_pmids)
    is_adjudicated= adjudication is not None

    with st.container():
        # ── Header row ────────────────────────────────────────────────
        h_col, s_col = st.columns([5, 1])
        with h_col:
            source_val = str(article.get("source","")).lower()
            _src_cls = {"pubmed":"sr-source-pubmed","europe_pmc":"sr-source-epmc",
                        "core":"sr-source-core"}.get(source_val,"sr-source-default")
            _src_lbl = {"pubmed":"PubMed","europe_pmc":"Europe PMC","core":"CORE"}.get(
                source_val, source_val.upper() or "Unknown")
            badge = ""
            if is_adjudicated and is_arbiter:
                badge='<span style="font-size:0.72rem;font-weight:600;padding:0.1rem 0.45rem;border-radius:4px;background:#D1FAE5;color:#065F46;margin-right:0.4rem;">✓ Resolved</span>'
            elif has_conflict and is_arbiter:
                badge='<span style="font-size:0.72rem;font-weight:600;padding:0.1rem 0.45rem;border-radius:4px;background:#FEF3C7;color:#92400E;margin-right:0.4rem;">⚡ Conflict</span>'
            st.markdown(
                f'{badge}<span style="font-weight:600;font-size:0.9rem;color:#0F172A;">'
                f'{article["title"]}</span>', unsafe_allow_html=True)
            st.markdown(
                f'<div style="display:flex;flex-wrap:wrap;gap:0.35rem;margin-top:0.25rem;">'
                f'<span class="sr-pill">{article.get("journal","") or "Unknown"}</span>'
                f'<span class="sr-pill">{article.get("year","") or ""}</span>'
                f'<span class="sr-source-badge {_src_cls}">{_src_lbl}</span>'
                f'</div>', unsafe_allow_html=True)
        with s_col:
            dec = my_decision
            _bg = {"include":"#D1FAE5","exclude":"#FFE4E6","unsure":"#FEF3C7"}.get(dec,"#F0EEE9")
            _fg = {"include":"#065F46","exclude":"#9F1239","unsure":"#92400E"}.get(dec,"#64748B")
            st.markdown(
                f'<div style="text-align:center;background:{_bg};border-radius:8px;'
                f'padding:0.5rem;border:1px solid {_fg}33;">'
                f'<div style="font-size:1.1rem;">{DECISION_EMOJI.get(dec,"○")}</div>'
                f'<div style="font-size:0.7rem;font-weight:600;color:{_fg};'
                f'text-transform:uppercase;">{DECISION_LABEL.get(dec,"Pending")}</div>'
                f'</div>', unsafe_allow_html=True)

        # ── Blinded / arbiter decision grid ───────────────────────────
        primary = {k:v for k,v in all_decisions.items() if k not in SYSTEM_REVIEWER_IDS}
        if is_arbiter and primary:
            dec_cols = st.columns(max(len(primary),1))
            for col,(rev_id,dec) in zip(dec_cols,primary.items()):
                col.markdown(
                    f"<div style='text-align:center'><small>{_reviewer_label(rev_id)}</small><br>"
                    f"<span style='font-size:1.4em'>{DECISION_EMOJI.get(dec,'⬜')}</span><br>"
                    f"<small><b>{DECISION_LABEL.get(dec,'Pending')}</b></small></div>",
                    unsafe_allow_html=True)
        elif not is_arbiter:
            others = sum(1 for k,v in primary.items()
                         if k!=current_reviewer_id and v is not None)
            total_o= sum(1 for k in primary if k!=current_reviewer_id)
            if total_o>0:
                st.caption(f"🔒 {others}/{total_o} other reviewer(s) screened (decisions hidden).")

        # ── Adjudication notice ────────────────────────────────────────
        if is_adjudicated and is_arbiter:
            final  = adjudication["final_decision"]
            st.success(
                f"✅ **Resolved:** {DECISION_EMOJI.get(final,'')} {final.capitalize()}"
                + (f" · *{adjudication['notes']}*" if adjudication.get("notes") else ""))
        elif has_conflict and is_arbiter:
            st.warning(f"⚠️ **Conflict:** {_classify_conflict(all_decisions)}. Resolve in Conflicts tab.")

        # ── Abstract with PICO keyword highlighting ────────────────────
        abstract = article.get("abstract") or ""
        with st.expander("Abstract", expanded=True):
            if pico_terms and abstract:
                highlighted = _highlight_abstract(abstract, pico_terms)
                st.markdown(
                    f'<div style="font-size:0.88rem;line-height:1.65;">{highlighted}</div>',
                    unsafe_allow_html=True)
            else:
                st.write(abstract or "No abstract.")

        # ── Per-card PICO extractor ────────────────────────────────────
        cached_pico = _get_cached(review_id, pmid, "s1_pico")
        pc1, pc2 = st.columns([1, 4])
        if pc1.button("🔬 Extract PICO", key=f"s1pico_{pmid}", type="secondary"):
            with st.spinner("Extracting PICO…"):
                try:
                    result = extract_pico(abstract)
                    _set_cached(review_id, pmid, "s1_pico", result)
                    cached_pico = result
                except Exception as e:
                    st.error(f"PICO error: {e}")
        if cached_pico:
            with pc2.container():
                p = cached_pico
                st.markdown(
                    f'<div style="background:#F0FDF4;border:1px solid #BBF7D0;'
                    f'border-radius:8px;padding:0.6rem 0.9rem;font-size:0.82rem;">'
                    f'<b>P:</b> {p.population or "—"} &nbsp;|&nbsp; '
                    f'<b>I:</b> {p.intervention or "—"} &nbsp;|&nbsp; '
                    f'<b>C:</b> {p.comparison or "—"} &nbsp;|&nbsp; '
                    f'<b>O:</b> {p.outcome or "—"}</div>',
                    unsafe_allow_html=True)

        # ── BELOW ABSTRACT: Reason → Note → Decision → Submit ─────────
        if not is_arbiter:
            _saved_row = screening_repo.get_decision_with_reason(
                review_id, pmid, "title_abstract", current_reviewer_id)
            _saved_reason = _saved_row.get("reason","") if _saved_row else ""
            _saved_tags, _saved_note = _parse_reason(_saved_reason)

            _pend_key = f"s1_pend_{pmid}_{current_reviewer_id}"
            _pending  = st.session_state.get(_pend_key)
            _effective= _pending if _pending is not None else my_decision

            # Reason tags for exclude/unsure
            if _effective in ("exclude","unsure"):
                _rmap = EXCLUDE_REASONS if _effective=="exclude" else UNSURE_REASONS
                st.markdown('<div style="font-size:0.72rem;font-weight:600;'
                    'text-transform:uppercase;color:#6B7280;margin:0.5rem 0 0.3rem;">'
                    'Reason (select all that apply)</div>', unsafe_allow_html=True)
                tcols = st.columns(3)
                _sel = list(_saved_tags)
                for i,(lbl,key) in enumerate(_rmap.items()):
                    chk = tcols[i%3].checkbox(lbl, value=(key in _sel),
                                              key=f"s1tag_{pmid}_{current_reviewer_id}_{key}")
                    if chk and key not in _sel: _sel.append(key)
                    elif not chk and key in _sel: _sel.remove(key)
            else:
                _sel = list(_saved_tags)

            # Note
            _note = st.text_input("📝 Note (optional)", value=_saved_note,
                key=f"s1note_{pmid}_{current_reviewer_id}",
                placeholder="Read abstract → extract PICO → add note → decide → Submit")

            # Decision + Submit buttons
            st.markdown('<div style="font-size:0.72rem;font-weight:600;'
                'text-transform:uppercase;color:#6B7280;margin:0.5rem 0 0.25rem;">'
                'Decision</div>', unsafe_allow_html=True)
            b1,b2,b3,bsub = st.columns([2,2,2,3])

            if b1.button("✅ Include", key=f"s1inc_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="include" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="include" else "include"
                st.rerun()
            if b2.button("❌ Exclude", key=f"s1exc_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="exclude" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="exclude" else "exclude"
                st.rerun()
            if b3.button("❓ Unsure", key=f"s1uns_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="unsure" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="unsure" else "unsure"
                st.rerun()

            _reason_to_save = _build_reason(_sel, _note)
            _can_submit = _effective is not None
            if bsub.button(
                "💾 Submit" if _pending is not None else "💾 Update",
                key=f"s1sub_{pmid}_{current_reviewer_id}",
                type="primary" if _can_submit else "secondary",
                use_container_width=True, disabled=not _can_submit):
                if _effective:
                    screening_repo.save_decision(
                        review_id, pmid, "title_abstract", _effective,
                        reason=_reason_to_save, reviewer_id=current_reviewer_id)
                    st.session_state.pop(_pend_key, None)
                    st.rerun()

            if my_decision and _saved_reason and _pending is None:
                _render_reason_display(_saved_reason, my_decision)

        elif is_arbiter and primary:
            for rev_id,dec in primary.items():
                row = screening_repo.get_decision_with_reason(
                    review_id, pmid, "title_abstract", rev_id)
                if row and row.get("reason"):
                    st.caption(f"**{_reviewer_label(rev_id)}** reasoning:")
                    _render_reason_display(row["reason"], dec)

        st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2 — FULL TEXT SCREENING
# ══════════════════════════════════════════════════════════════════════════════
def _render_stage2(review_id: int, current_reviewer_id: str, is_arbiter: bool) -> None:
    # Articles that passed Stage 1 with full texts retrieved
    ft_articles = article_repo.get_articles_with_fulltext(review_id)
    # Also show Stage 1 included articles even without full text
    included_pmids = article_repo.get_stage1_included_pmids(review_id)

    if not included_pmids:
        st.info("⏳ No articles have been included at Stage 1 yet. "
                "Complete Title/Abstract screening first.")
        return

    # Get all Stage 1 included articles (with or without full text)
    all_articles = article_repo.get_articles_for_review(
        review_id, current_reviewer_id, stage="title_abstract")
    s1_included = [a for a in all_articles if a["pmid"] in included_pmids]

    ft_pmids = {a["pmid"] for a in ft_articles}
    ft_map   = {a["pmid"]: a.get("full_text","") for a in ft_articles}

    # Stage 2 decisions
    my_s2_decisions = {}
    for a in s1_included:
        pmid = a["pmid"]
        row  = screening_repo.get_decision_with_reason(
            review_id, pmid, "full_text", current_reviewer_id)
        my_s2_decisions[pmid] = row.get("decision") if row else None

    decided2 = sum(1 for d in my_s2_decisions.values() if d is not None)
    total2   = len(s1_included)

    # Summary metrics
    mc1,mc2,mc3 = st.columns(3)
    mc1.metric("Passed Stage 1", total2)
    mc2.metric("Have Full Text", len(ft_pmids))
    mc3.metric("Stage 2 Progress", f"{decided2}/{total2}")
    if total2 > 0:
        st.progress(decided2/total2)
    st.divider()

    if not s1_included:
        st.info("No included articles found.")
        return

    # Controls
    ctrl1, ctrl2 = st.columns([3,3])
    with ctrl1:
        f2 = st.radio("Show:", ["All","Pending","Decided"],
                      index=0, horizontal=True, key="s2_filter")
    with ctrl2:
        s2_sort = st.selectbox("Sort by",
            ["Pending first, then A→Z","A → Z (title)","Newest first"],
            key="s2_sort")

    filtered2 = []
    for a in s1_included:
        d = my_s2_decisions.get(a["pmid"])
        if f2=="Pending" and d is not None: continue
        if f2=="Decided" and d is None: continue
        filtered2.append(a)

    filtered2 = _sort_articles(filtered2, s2_sort, my_s2_decisions)
    st.caption(f"Showing {len(filtered2)} of {total2} articles — "
               f"{len(ft_pmids)} have full text retrieved")

    for article in filtered2:
        _render_s2_card(article, review_id, current_reviewer_id, is_arbiter,
                        ft_map, my_s2_decisions)


def _render_s2_card(article, review_id, current_reviewer_id, is_arbiter,
                    ft_map, my_s2_decisions):
    pmid = article["pmid"]
    my_decision = my_s2_decisions.get(pmid)
    full_text   = ft_map.get(pmid, "")
    has_ft      = bool(full_text)

    with st.container():
        h_col, s_col = st.columns([5,1])
        with h_col:
            source_val = str(article.get("source","")).lower()
            _src_cls = {"pubmed":"sr-source-pubmed","europe_pmc":"sr-source-epmc",
                        "core":"sr-source-core"}.get(source_val,"sr-source-default")
            _src_lbl = {"pubmed":"PubMed","europe_pmc":"Europe PMC","core":"CORE"}.get(
                source_val, source_val.upper() or "Unknown")
            ft_badge = ('<span style="font-size:0.7rem;font-weight:600;padding:0.1rem 0.45rem;'
                        'border-radius:4px;background:#D1FAE5;color:#065F46;margin-right:0.4rem;">'
                        '📄 Full Text</span>' if has_ft else
                        '<span style="font-size:0.7rem;font-weight:600;padding:0.1rem 0.45rem;'
                        'border-radius:4px;background:#FEF3C7;color:#92400E;margin-right:0.4rem;">'
                        '📋 Abstract only</span>')
            st.markdown(
                f'{ft_badge}<span style="font-weight:600;font-size:0.9rem;color:#0F172A;">'
                f'{article["title"]}</span>', unsafe_allow_html=True)
            st.markdown(
                f'<div style="display:flex;flex-wrap:wrap;gap:0.35rem;margin-top:0.25rem;">'
                f'<span class="sr-pill">{article.get("journal","") or "Unknown"}</span>'
                f'<span class="sr-pill">{article.get("year","") or ""}</span>'
                f'<span class="sr-source-badge {_src_cls}">{_src_lbl}</span>'
                f'</div>', unsafe_allow_html=True)
        with s_col:
            _bg={"include":"#D1FAE5","exclude":"#FFE4E6","unsure":"#FEF3C7"}.get(my_decision,"#F0EEE9")
            _fg={"include":"#065F46","exclude":"#9F1239","unsure":"#92400E"}.get(my_decision,"#64748B")
            st.markdown(
                f'<div style="text-align:center;background:{_bg};border-radius:8px;'
                f'padding:0.5rem;border:1px solid {_fg}33;">'
                f'<div style="font-size:1.1rem;">{DECISION_EMOJI.get(my_decision,"○")}</div>'
                f'<div style="font-size:0.7rem;font-weight:600;color:{_fg};'
                f'text-transform:uppercase;">{DECISION_LABEL.get(my_decision,"Pending")}</div>'
                f'</div>', unsafe_allow_html=True)

        # ── Full text or abstract ──────────────────────────────────────
        display_text = full_text if has_ft else (article.get("abstract") or "")
        label = "Full Text" if has_ft else "Abstract (no full text retrieved)"
        with st.expander(label, expanded=True):
            if has_ft:
                # Show first ~3000 chars of full text
                preview = display_text[:3000]
                if len(display_text) > 3000:
                    preview += "\n\n*[Full text truncated for display — complete text used for AI summary]*"
                st.markdown(preview)
            else:
                st.write(display_text or "No text available.")
                st.caption("💡 Run Search → Full Texts to retrieve the full paper.")

        # ── Per-card AI Summariser ─────────────────────────────────────
        cached_sum = _get_cached(review_id, pmid, "s2_summary")
        sc1, sc2 = st.columns([1,4])
        if sc1.button("📝 Summarise", key=f"s2sum_{pmid}", type="secondary"):
            text_to_summarise = (full_text[:4000] if has_ft
                                 else article.get("abstract",""))
            if not text_to_summarise:
                st.warning("No text to summarise.")
            else:
                with st.spinner("Generating AI summary…"):
                    try:
                        result = summarize_with_llm(text_to_summarise)
                        _set_cached(review_id, pmid, "s2_summary", result or "(no output)")
                        cached_sum = result
                    except Exception as e:
                        st.error(f"Summary error: {e}")
        if cached_sum:
            with sc2.container():
                st.markdown(
                    f'<div style="background:#EFF6FF;border:1px solid #BFDBFE;'
                    f'border-radius:8px;padding:0.7rem 1rem;font-size:0.85rem;">'
                    f'<b>AI Summary</b><br>{cached_sum}</div>',
                    unsafe_allow_html=True)

        # ── Reason → Note → Decision → Submit ─────────────────────────
        if not is_arbiter:
            _saved_row = screening_repo.get_decision_with_reason(
                review_id, pmid, "full_text", current_reviewer_id)
            _saved_reason = _saved_row.get("reason","") if _saved_row else ""
            _saved_tags, _saved_note = _parse_reason(_saved_reason)

            _pend_key = f"s2_pend_{pmid}_{current_reviewer_id}"
            _pending  = st.session_state.get(_pend_key)
            _effective= _pending if _pending is not None else my_decision

            if _effective in ("exclude","unsure"):
                _rmap = FULLTEXT_EXCLUDE_REASONS if _effective=="exclude" else UNSURE_REASONS
                st.markdown('<div style="font-size:0.72rem;font-weight:600;'
                    'text-transform:uppercase;color:#6B7280;margin:0.5rem 0 0.3rem;">'
                    'Exclusion reason</div>', unsafe_allow_html=True)
                tcols = st.columns(3)
                _sel = list(_saved_tags)
                for i,(lbl,key) in enumerate(_rmap.items()):
                    chk = tcols[i%3].checkbox(lbl, value=(key in _sel),
                                              key=f"s2tag_{pmid}_{current_reviewer_id}_{key}")
                    if chk and key not in _sel: _sel.append(key)
                    elif not chk and key in _sel: _sel.remove(key)
            else:
                _sel = list(_saved_tags)

            _note = st.text_input("📝 Note (optional)", value=_saved_note,
                key=f"s2note_{pmid}_{current_reviewer_id}",
                placeholder="Read full text → summarise → note → decide → Submit")

            st.markdown('<div style="font-size:0.72rem;font-weight:600;'
                'text-transform:uppercase;color:#6B7280;margin:0.5rem 0 0.25rem;">'
                'Stage 2 Decision</div>', unsafe_allow_html=True)
            b1,b2,b3,bsub = st.columns([2,2,2,3])

            if b1.button("✅ Include", key=f"s2inc_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="include" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="include" else "include"
                st.rerun()
            if b2.button("❌ Exclude", key=f"s2exc_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="exclude" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="exclude" else "exclude"
                st.rerun()
            if b3.button("❓ Unsure", key=f"s2uns_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="unsure" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="unsure" else "unsure"
                st.rerun()

            _reason_to_save = _build_reason(_sel, _note)
            _can_submit = _effective is not None
            if bsub.button(
                "💾 Submit" if _pending is not None else "💾 Update",
                key=f"s2sub_{pmid}_{current_reviewer_id}",
                type="primary" if _can_submit else "secondary",
                use_container_width=True, disabled=not _can_submit):
                if _effective:
                    screening_repo.save_decision(
                        review_id, pmid, "full_text", _effective,
                        reason=_reason_to_save, reviewer_id=current_reviewer_id)
                    st.session_state.pop(_pend_key, None)
                    st.rerun()

            if my_decision and _saved_reason and _pending is None:
                _render_reason_display(_saved_reason, my_decision)

        st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# CONFLICTS TAB
# ══════════════════════════════════════════════════════════════════════════════
def _render_conflict_tab(review_id: int, current_reviewer_id: str, is_arbiter: bool) -> None:
    if not is_arbiter:
        st.info("🔒 Conflicts tab is only accessible to the **Editor**.")
        return

    stage = st.radio("Stage:", ["Stage 1 — Title/Abstract","Stage 2 — Full Text"],
                     horizontal=True, key="conflict_stage")
    stage_key = "title_abstract" if "1" in stage else "full_text"

    conflicts = screening_repo.get_conflicts(review_id, stage_key)
    if not conflicts:
        st.success(f"✅ No conflicts at {stage}.")
        _render_adjudicated_summary(review_id, stage_key)
        return

    st.markdown(f"### ⚠️ {len(conflicts)} article(s) require adjudication")
    for c in conflicts:
        pmid  = c["pmid"]
        title = c.get("title","")[:80]
        all_dec = screening_repo.get_all_decisions_for_article(review_id, pmid, stage_key)
        primary = {k:v for k,v in all_dec.items() if k not in SYSTEM_REVIEWER_IDS}
        adj     = adjudication_repo.get_adjudication(review_id, pmid)

        with st.expander(f"⚠️ {title}", expanded=False):
            dec_cols = st.columns(max(len(primary),1))
            for col,(rev_id,dec) in zip(dec_cols,primary.items()):
                row = screening_repo.get_decision_with_reason(
                    review_id, pmid, stage_key, rev_id)
                col.markdown(
                    f"<div style='text-align:center'><small>{_reviewer_label(rev_id)}</small><br>"
                    f"<span style='font-size:1.4em'>{DECISION_EMOJI.get(dec,'⬜')}</span><br>"
                    f"<small><b>{DECISION_LABEL.get(dec,'Pending')}</b></small></div>",
                    unsafe_allow_html=True)
                if row and row.get("reason"):
                    _render_reason_display(row["reason"], dec)

            if adj:
                final = adj["final_decision"]
                st.success(f"✅ **Resolved:** {DECISION_EMOJI.get(final,'')} {final.capitalize()}"
                           + (f" · *{adj['notes']}*" if adj.get("notes") else ""))
            else:
                st.warning("Not yet resolved.")
                adj_dec = st.selectbox("Final decision:", ["include","exclude","unsure"],
                                       key=f"adj_dec_{pmid}_{stage_key}")
                adj_notes = st.text_area("Notes:", key=f"adj_notes_{pmid}_{stage_key}",
                                         height=80)
                if st.button("✅ Resolve", key=f"adj_submit_{pmid}_{stage_key}",
                             type="primary"):
                    adjudication_repo.save_adjudication(
                        review_id, pmid, adj_dec, current_reviewer_id,
                        notes=adj_notes, stage=stage_key)
                    st.rerun()
        st.divider()

    _render_adjudicated_summary(review_id, stage_key)


def _render_adjudicated_summary(review_id: int, stage: str = "title_abstract") -> None:
    adjs = adjudication_repo.get_all_adjudications(review_id, stage)
    if not adjs:
        return
    st.markdown(f"#### ✅ Resolved ({len(adjs)})")
    rows = []
    for adj in adjs:
        dec = adj["final_decision"]
        rows.append({
            "Title":  adj.get("title", adj["pmid"])[:60],
            "Final":  f"{DECISION_EMOJI.get(dec,'')} {dec.capitalize()}",
            "By":     _reviewer_label(adj["adjudicator_id"]),
            "Notes":  (adj.get("notes") or "")[:80],
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# AGREEMENT & SUMMARY TAB
# ══════════════════════════════════════════════════════════════════════════════
def _render_agreement_stats(review_id: int) -> None:
    stats = screening_repo.get_agreements(review_id, "title_abstract")
    st.markdown("### Inter-Rater Agreement — Stage 1")
    st.caption("Required for Methods section reporting in published systematic reviews.")

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Dual-Screened", stats["dual_screened"])
    c2.metric("Agreements",    stats["agreements"])
    c3.metric("Conflicts",     stats["conflicts"])
    c4.metric("Agreement %",   f"{stats['agreement_pct']}%")

    pct = stats["agreement_pct"]
    if stats["dual_screened"] > 0:
        if pct >= 90:   st.success(f"✅ {pct}% — Excellent (≥90%)")
        elif pct >= 80: st.warning(f"⚠️ {pct}% — Acceptable (80–90%)")
        else:           st.error(f"❌ {pct}% — Poor (<80%). Calibration recommended.")

    st.divider()
    st.markdown("#### Reporting Template")
    adj  = adjudication_repo.count_by_decision(review_id)
    adj_n= sum(adj.values())
    st.code(
        f"Two independent reviewers screened all {stats['dual_screened']} records by title "
        f"and abstract. Blinded screening yielded {stats['agreement_pct']}% agreement "
        f"({stats['agreements']} records). Disagreements ({stats['conflicts']} records) were "
        f"resolved by a third reviewer ({adj_n} adjudicated).",
        language=None)

    st.divider()

    # ── Per-reviewer aggregate table ──────────────────────────────────
    st.markdown("#### Per-Reviewer Decision Summary")
    reviewer_ids = ["rev_reviewer_1","rev_reviewer_2","rev_reviewer_3","rev_editor"]
    articles     = article_repo.get_articles_for_review(review_id)
    agg_rows = []
    for stage_key, stage_label in [("title_abstract","Stage 1"),("full_text","Stage 2")]:
        for rev_id in reviewer_ids:
            counts_r = {"include":0,"exclude":0,"unsure":0}
            for art in articles:
                dec = screening_repo.get_decision(
                    review_id, art["pmid"], stage_key, rev_id)
                if dec in counts_r: counts_r[dec] += 1
            tot = sum(counts_r.values())
            if tot > 0:
                agg_rows.append({
                    "Reviewer":   _reviewer_label(rev_id),
                    "Stage":      stage_label,
                    "Include 🟢": counts_r["include"],
                    "Exclude 🔴": counts_r["exclude"],
                    "Unsure 🟡":  counts_r["unsure"],
                    "Total":      tot,
                    "% Decided":  f"{round(100*tot/max(len(articles),1))}%",
                })
    if agg_rows:
        st.dataframe(pd.DataFrame(agg_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("No decisions recorded yet.")

    st.divider()

    # ── Detailed Decision Log — Editor only (blinding protection) ─────
    current_reviewer_id = st.session_state.get("current_reviewer_id","rev_reviewer_1")
    is_arbiter = "editor" in current_reviewer_id.lower()

    if not is_arbiter:
        st.info("🔒 The Detailed Decision Log is only available to the **Editor** "
                "to protect blinded screening. Switch to Editor role in the sidebar.")
        return

    st.markdown("#### Detailed Decision Log (Editor only)")
    active = [r for r in reviewer_ids if any(
        screening_repo.get_decision(review_id, art["pmid"], "title_abstract", r)
        for art in articles)]

    if not active:
        st.caption("No decisions recorded yet.")
        return

    rev_sel = st.selectbox("Reviewer:", active, format_func=_reviewer_label,
                           key="sum_rev_sel")
    stage_sel = st.radio("Stage:", ["Stage 1 — Title/Abstract","Stage 2 — Full Text"],
                         horizontal=True, key="sum_stage_sel")
    stage_key = "title_abstract" if "1" in stage_sel else "full_text"

    all_reasons = {**EXCLUDE_REASONS, **UNSURE_REASONS, **FULLTEXT_EXCLUDE_REASONS}
    detail_rows = []
    for art in articles:
        row = screening_repo.get_decision_with_reason(
            review_id, art["pmid"], stage_key, rev_sel)
        if row:
            dec  = row.get("decision","")
            tags, note = _parse_reason(row.get("reason",""))
            tag_labels = ", ".join(
                next((k for k,v in all_reasons.items() if v==t), t)
                for t in tags) if tags else ""
            detail_rows.append({
                "PMID":     art["pmid"],
                "Title":    (art.get("title") or "")[:70],
                "Journal":  art.get("journal",""),
                "Year":     art.get("year",""),
                "Decision": f"{DECISION_EMOJI.get(dec,'')} {dec.capitalize()}" if dec else "Pending",
                "Reasons":  tag_labels,
                "Note":     note,
                "Decided":  row.get("decided_at",""),
            })

    if detail_rows:
        df = pd.DataFrame(detail_rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button(
            f"📥 Download {_reviewer_label(rev_sel)} — {stage_sel} (CSV)",
            data=df.to_csv(index=False),
            file_name=f"screening_{rev_sel}_{stage_key}_{review_id}.csv",
            mime="text/csv", use_container_width=True)
    else:
        st.caption(f"No {stage_sel} decisions from {_reviewer_label(rev_sel)} yet.")