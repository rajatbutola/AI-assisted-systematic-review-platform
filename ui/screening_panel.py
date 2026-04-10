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

import streamlit as st

from typing import Dict, List, Optional

from storage.repository import (
    ArticleRepository,
    ScreeningRepository,
    AdjudicationRepository,
)

import pandas as pd

article_repo      = ArticleRepository()
screening_repo    = ScreeningRepository()
adjudication_repo = AdjudicationRepository()

DECISION_EMOJI = {
    "include": "🟢",
    "exclude": "🔴",
    "unsure":  "🟡",
    None:      "⬜",
}

DECISION_LABEL = {
    "include": "Include",
    "exclude": "Exclude",
    "unsure":  "Unsure",
    None:      "Pending",
}

SYSTEM_REVIEWER_IDS = {"final_resolved", "editor", "adjudicator"}

REVIEWER_DISPLAY = {
    "rev_reviewer_1": "Reviewer 1",
    "rev_reviewer_2": "Reviewer 2",
    "rev_reviewer_3": "Reviewer 3",
    "rev_editor":     "Editor",
}


def _reviewer_label(reviewer_id: str) -> str:
    return REVIEWER_DISPLAY.get(reviewer_id, reviewer_id)


def _classify_conflict(decisions: Dict[str, str]) -> str:
    primary = {k: v for k, v in decisions.items() if k not in SYSTEM_REVIEWER_IDS}
    unique  = sorted(set(primary.values()))
    return " vs ".join(unique) if len(unique) > 1 else ""


def _sort_articles(articles: list, sort_key: str, my_decisions: dict) -> list:
    """
    Sort article list deterministically.
    All reviewers see the same sort order for a given sort_key, so the
    list never jumps around after a decision is saved.
    """
    if sort_key == "Pending first, then A→Z":
        def _key(a):
            is_pending = my_decisions.get(a["pmid"]) is None
            return (0 if is_pending else 1, (a.get("title") or "").lower())
        return sorted(articles, key=_key)

    elif sort_key == "Decided first, then A→Z":
        def _key(a):
            is_pending = my_decisions.get(a["pmid"]) is None
            return (1 if is_pending else 0, (a.get("title") or "").lower())
        return sorted(articles, key=_key)

    elif sort_key == "A → Z (title)":
        return sorted(articles, key=lambda a: (a.get("title") or "").lower())

    elif sort_key == "Z → A (title)":
        return sorted(articles, key=lambda a: (a.get("title") or "").lower(), reverse=True)

    elif sort_key == "Newest first":
        return sorted(articles,
                      key=lambda a: int(a.get("year") or 0),
                      reverse=True)

    elif sort_key == "Oldest first":
        return sorted(articles,
                      key=lambda a: int(a.get("year") or 9999))

    # Default: original DB order
    return articles


def render_screening_panel(review_id: int) -> None:
    current_reviewer_id = st.session_state.get("current_reviewer_id", "rev_reviewer_1")
    is_arbiter          = "editor" in current_reviewer_id.lower()

    if not is_arbiter:
        st.info(
            f"🔒 **Blinded screening** — you are **{_reviewer_label(current_reviewer_id)}**. "
            "You can only see your own decisions. "
            "Switch to **Editor** in the sidebar to view all decisions and resolve conflicts."
        )

    subtabs = st.tabs(["📋 All Articles", "⚠️ Conflicts", "📊 Agreement Stats"])

    with subtabs[0]:
        _render_article_list(review_id, current_reviewer_id, is_arbiter)
    with subtabs[1]:
        _render_conflict_tab(review_id, current_reviewer_id, is_arbiter)
    with subtabs[2]:
        _render_agreement_stats(review_id)


def _render_article_list(review_id: int, current_reviewer_id: str,
                         is_arbiter: bool) -> None:
    articles = article_repo.get_articles_for_review(review_id)
    if not articles:
        st.info("No articles to screen. Run a search first.")
        return

    # Build my-decisions map once (O(1) lookup per article)
    my_decisions = {
        a["pmid"]: screening_repo.get_decision(
            review_id, a["pmid"], "title_abstract", current_reviewer_id
        )
        for a in articles
    }

    decided_count = sum(1 for d in my_decisions.values() if d is not None)
    total_count   = len(articles)

    # ── Controls row ──────────────────────────────────────────────────
    ctrl1, ctrl2, ctrl3 = st.columns([3, 3, 1])
    with ctrl1:
        filter_view = st.radio(
            "Show articles:",
            ["All", "Pending (no decision from me)", "Decided by me",
             "Conflicted (Editor only)"],
            horizontal=True, key="screen_filter",
        )
    with ctrl2:
        sort_key = st.selectbox(
            "Sort by",
            ["Pending first, then A→Z", "Decided first, then A→Z",
             "A → Z (title)", "Z → A (title)", "Newest first", "Oldest first"],
            key="screen_sort",
        )
    with ctrl3:
        st.metric("My Progress", f"{decided_count} / {total_count}")
    if total_count > 0:
        st.progress(decided_count / total_count,
                    text=f"{decided_count} of {total_count} screened by you")

    st.divider()

    # Conflict lookup (only needed for Editor)
    conflict_pmids: set = set()
    if is_arbiter or filter_view == "Conflicted (Editor only)":
        conflict_pmids = {
            c["pmid"] for c in screening_repo.get_conflicts(review_id, "title_abstract")
        }

    # Apply filter
    filtered = []
    for article in articles:
        pmid   = article["pmid"]
        my_dec = my_decisions.get(pmid)
        if filter_view == "Pending (no decision from me)" and my_dec is not None:
            continue
        if filter_view == "Decided by me" and my_dec is None:
            continue
        if filter_view == "Conflicted (Editor only)" and pmid not in conflict_pmids:
            continue
        filtered.append(article)

    if not filtered:
        if filter_view == "Conflicted (Editor only)" and not is_arbiter:
            st.info("Switch to **Editor** role to see conflicted articles.")
        else:
            st.info(f"No articles match filter: {filter_view}")
        return

    # Apply sort — deterministic for ALL reviewers
    filtered = _sort_articles(filtered, sort_key, my_decisions)

    st.caption(f"Showing {len(filtered)} of {total_count} articles")

    for article in filtered:
        _render_article_card(
            article, review_id, current_reviewer_id,
            is_arbiter, conflict_pmids,
        )


def _render_article_card(
    article: Dict,
    review_id: int,
    current_reviewer_id: str,
    is_arbiter: bool,
    conflict_pmids: set,
) -> None:
    pmid = article["pmid"]

    all_decisions = screening_repo.get_all_decisions_for_article(
        review_id, pmid, "title_abstract"
    )
    adjudication = adjudication_repo.get_adjudication(review_id, pmid)
    my_decision  = all_decisions.get(current_reviewer_id)

    has_conflict   = is_arbiter and (pmid in conflict_pmids)
    is_adjudicated = adjudication is not None

    with st.container():
        header_col, status_col = st.columns([5, 1])
        with header_col:
            title_display = article["title"]
            if is_adjudicated and is_arbiter:
                title_display = "✅ " + title_display
            elif has_conflict and is_arbiter:
                title_display = "⚠️ " + title_display
            st.markdown(f"**{title_display}**")
            st.caption(
                f"{article.get('journal', '')} ({article.get('year', '')}) · "
                f"Source: {article.get('source', 'N/A')}"
            )

        with status_col:
            my_emoji = DECISION_EMOJI.get(my_decision, "⬜")
            my_label = DECISION_LABEL.get(my_decision, "Pending")
            st.markdown(
                f"**My decision:**<br>{my_emoji} {my_label}",
                unsafe_allow_html=True
            )

        with st.expander("Abstract", expanded=(my_decision is None)):
            st.write(article.get("abstract") or "No abstract.")

        # ── Decision visibility (blinded for primary reviewers) ───────
        primary_decisions = {
            k: v for k, v in all_decisions.items()
            if k not in SYSTEM_REVIEWER_IDS
        }

        if is_arbiter:
            if primary_decisions:
                dec_cols = st.columns(max(len(primary_decisions), 1))
                for col, (rev_id, dec) in zip(dec_cols, primary_decisions.items()):
                    emoji = DECISION_EMOJI.get(dec, "⬜")
                    label = DECISION_LABEL.get(dec, "Pending")
                    col.markdown(
                        f"<div style='text-align:center'>"
                        f"<small>{_reviewer_label(rev_id)}</small><br>"
                        f"<span style='font-size:1.4em'>{emoji}</span><br>"
                        f"<small><b>{label}</b></small></div>",
                        unsafe_allow_html=True,
                    )
        else:
            others_decided = sum(
                1 for rev_id, dec in primary_decisions.items()
                if rev_id != current_reviewer_id and dec is not None
            )
            total_others = sum(
                1 for rev_id in primary_decisions
                if rev_id != current_reviewer_id
            )
            if total_others > 0:
                st.caption(
                    f"🔒 {others_decided}/{total_others} other reviewer(s) have screened "
                    "this article (decisions hidden until Editor review)."
                )

        # ── Adjudication notice (Editor only) ─────────────────────────
        if is_adjudicated and is_arbiter:
            final   = adjudication["final_decision"]
            adj_who = _reviewer_label(adjudication.get("adjudicator_id", ""))
            st.success(
                f"✅ **Resolved by {adj_who}:** "
                f"{DECISION_EMOJI.get(final, '')} {final.capitalize()}"
                + (f" · *{adjudication['notes']}*" if adjudication.get("notes") else "")
            )
        elif has_conflict and is_arbiter:
            conflict_type = _classify_conflict(all_decisions)
            st.warning(
                f"⚠️ **Conflict:** {conflict_type}. Go to **Conflicts** tab to resolve."
            )

        # ── Voting buttons (primary reviewers only) ───────────────────
        if not is_arbiter:
            btn1, btn2, btn3 = st.columns(3)
            if btn1.button(
                "✅ Include", key=f"inc_{pmid}_{current_reviewer_id}",
                type="primary" if my_decision == "include" else "secondary",
                use_container_width=True,
            ):
                screening_repo.save_decision(
                    review_id, pmid, "title_abstract", "include",
                    reviewer_id=current_reviewer_id,
                )
                st.rerun()

            if btn2.button(
                "❌ Exclude", key=f"exc_{pmid}_{current_reviewer_id}",
                type="primary" if my_decision == "exclude" else "secondary",
                use_container_width=True,
            ):
                screening_repo.save_decision(
                    review_id, pmid, "title_abstract", "exclude",
                    reviewer_id=current_reviewer_id,
                )
                st.rerun()

            if btn3.button(
                "❓ Unsure", key=f"uns_{pmid}_{current_reviewer_id}",
                type="primary" if my_decision == "unsure" else "secondary",
                use_container_width=True,
            ):
                screening_repo.save_decision(
                    review_id, pmid, "title_abstract", "unsure",
                    reviewer_id=current_reviewer_id,
                )
                st.rerun()

        st.divider()


def _render_conflict_tab(review_id: int, current_reviewer_id: str,
                         is_arbiter: bool) -> None:
    if not is_arbiter:
        st.info(
            "🔒 The Conflicts tab is only accessible to the **Editor**. "
            "Switch to Editor in the sidebar to view and resolve conflicts."
        )
        return

    conflicts = screening_repo.get_conflicts(review_id, "title_abstract")
    if not conflicts:
        st.success("✅ No conflicts — all screened articles have reviewer consensus.")
        _render_adjudicated_summary(review_id)
        return

    st.markdown(f"### ⚠️ {len(conflicts)} article(s) require adjudication")

    for conflict in conflicts:
        pmid        = conflict["pmid"]
        title       = conflict["title"]
        dec_summary = conflict["decisions_summary"]

        parsed_decisions = {}
        for part in dec_summary.split(" | "):
            if ":" in part:
                rev, dec = part.split(":", 1)
                parsed_decisions[rev.strip()] = dec.strip()

        with st.expander(
            f"⚠️ {title[:80]}{'...' if len(title) > 80 else ''}",
            expanded=True,
        ):
            st.markdown("**Reviewer decisions:**")
            for rev_id, dec in parsed_decisions.items():
                emoji = DECISION_EMOJI.get(dec, "⬜")
                st.markdown(f"- **{_reviewer_label(rev_id)}**: {emoji} {dec.capitalize()}")

            conflict_type = _classify_conflict(parsed_decisions)
            if conflict_type:
                st.caption(f"Conflict type: {conflict_type}")

            article_rows = article_repo.get_articles_for_review(review_id)
            abstract = next(
                (a["abstract"] for a in article_rows if a["pmid"] == pmid), ""
            )
            if abstract:
                st.markdown("**Abstract:**")
                st.write(abstract[:600] + ("..." if len(abstract) > 600 else ""))

            st.divider()
            adj_col1, adj_col2 = st.columns([2, 3])
            with adj_col1:
                final_dec = st.selectbox(
                    "Final Decision", ["include", "exclude", "unsure"],
                    key=f"adj_dec_{pmid}",
                    help="Your decision is binding and replaces all reviewer votes.",
                )
            with adj_col2:
                adj_notes = st.text_area(
                    "Reasoning / Notes (recommended for audit trail)",
                    key=f"adj_notes_{pmid}", height=80,
                )

            if st.button(
                f"💾 Save Final Decision: {final_dec.capitalize()}",
                key=f"save_adj_{pmid}", type="primary",
            ):
                adjudication_repo.save_adjudication(
                    review_id=review_id, pmid=pmid,
                    final_decision=final_dec,
                    adjudicator_id=current_reviewer_id,
                    conflict_type=conflict_type,
                    notes=adj_notes,
                    stage="title_abstract",
                )
                st.success(f"✅ Decision saved: **{final_dec.capitalize()}** for {title[:50]}")
                st.rerun()

        st.divider()

    _render_adjudicated_summary(review_id)


def _render_adjudicated_summary(review_id: int) -> None:
    adjudications = adjudication_repo.get_all_adjudications(review_id)
    if not adjudications:
        return

    st.markdown(f"#### ✅ Resolved Decisions ({len(adjudications)})")
    rows = []
    for adj in adjudications:
        dec = adj["final_decision"]
        rows.append({
            "Title":    adj.get("title", adj["pmid"])[:60],
            "Final":    f"{DECISION_EMOJI.get(dec, '')} {dec.capitalize()}",
            "By":       _reviewer_label(adj["adjudicator_id"]),
            "Conflict": adj.get("conflict_type", ""),
            "Notes":    (adj.get("notes") or "")[:80],
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_agreement_stats(review_id: int) -> None:
    stats = screening_repo.get_agreements(review_id, "title_abstract")
    st.markdown("### Inter-Rater Agreement")
    st.caption("Required for Methods section reporting in published systematic reviews.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Dual-Screened", stats["dual_screened"])
    c2.metric("Agreements",    stats["agreements"])
    c3.metric("Conflicts",     stats["conflicts"])
    c4.metric("Agreement Rate", f"{stats['agreement_pct']}%")

    if stats.get("adjudicated", 0) > 0 or stats.get("unresolved_conflicts", 0) > 0:
        r1, r2 = st.columns(2)
        r1.metric("✅ Resolved",   stats.get("adjudicated", 0))
        r2.metric("⚠️ Unresolved", stats.get("unresolved_conflicts", 0))

    if stats["dual_screened"] > 0:
        pct = stats["agreement_pct"]
        if pct >= 90:
            st.success(f"✅ {pct}% agreement — Excellent (≥90%)")
        elif pct >= 80:
            st.warning(f"⚠️ {pct}% agreement — Acceptable (80–90%).")
        else:
            st.error(f"❌ {pct}% agreement — Poor (<80%). Calibration meeting recommended.")

    st.divider()
    st.markdown("#### Reporting Template")
    pct        = stats["agreement_pct"]
    total      = stats["dual_screened"]
    total_conf = stats["conflicts"]
    adj        = adjudication_repo.count_by_decision(review_id)
    adj_n      = sum(adj.values())
    reporting_text = (
        f"Two independent reviewers screened all {total} records by title and abstract. "
        f"Reviewers were blinded to each other's decisions. "
        f"Agreement was reached on {stats['agreements']} records ({pct}%). "
        f"Disagreements arose for {total_conf} records, resolved by a third reviewer "
        f"(Editor) who made the final inclusion decision ({adj_n} records resolved)."
    )
    st.code(reporting_text, language=None)

    st.markdown("#### Per-Reviewer Decision Summary")
    reviewer_ids = ["rev_reviewer_1", "rev_reviewer_2", "rev_reviewer_3", "rev_editor"]
    rows     = []
    articles = article_repo.get_articles_for_review(review_id)
    for rev_id in reviewer_ids:
        rev_counts = {"include": 0, "exclude": 0, "unsure": 0}
        for art in articles:
            dec = screening_repo.get_decision(
                review_id, art["pmid"], "title_abstract", rev_id
            )
            if dec in rev_counts:
                rev_counts[dec] += 1
        total_by_rev = sum(rev_counts.values())
        if total_by_rev > 0:
            rows.append({
                "Reviewer":   _reviewer_label(rev_id),
                "Include 🟢": rev_counts["include"],
                "Exclude 🔴": rev_counts["exclude"],
                "Unsure 🟡":  rev_counts["unsure"],
                "Total":      total_by_rev,
            })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption("No reviewer decisions recorded yet.")

















