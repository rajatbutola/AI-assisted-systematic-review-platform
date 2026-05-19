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
import html as _html
import json
import logging
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional

from storage.repository import (
    ArticleRepository, ScreeningRepository, AdjudicationRepository,
    AIAnalysisRepository,
)
from pipeline.pico_extractor import extract_pico
from pipeline.summarizer     import summarize_with_llm
from utils.i18n import t

def _dlabel(decision: str) -> str:
    """Return translated decision label."""
    return {
        "include": t("decision_include"),
        "exclude": t("decision_exclude"),
        "unsure":  t("decision_unsure"),
        None:      t("decision_pending"),
    }.get(decision, t("decision_pending"))

logger = logging.getLogger(__name__)
 
article_repo      = ArticleRepository()
screening_repo    = ScreeningRepository()
adjudication_repo = AdjudicationRepository()
ai_repo           = AIAnalysisRepository()

def _extract_pdf_text(uploaded_file) -> tuple:
    """
    Extract text from an uploaded PDF using pdfplumber.
    Returns (full_text: str, n_pages: int).
    Falls back gracefully if pdfplumber is not installed.
    """
    try:
        import pdfplumber
        import io
        pdf_bytes = uploaded_file.read()
        pages_text = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            n_pages = len(pdf.pages)
            for page_no, page in enumerate(pdf.pages, 1):
                text = page.extract_text() or ""
                if text.strip():
                    # Prefix each page with a marker for future page-level anchoring
                    pages_text.append(f"[PAGE {page_no}]\n{text}")
        full_text = "\n\n".join(pages_text)
        return full_text, n_pages
    except ImportError:
        return "", 0
    except Exception as e:
        logger.error("PDF extraction error: %s", e)
        return "", 0


# ── Constants ──────────────────────────────────────────────────────────────────
DECISION_EMOJI = {"include":"🟢","exclude":"🔴","unsure":"🟡",None:"⬜"}
DECISION_LABEL = {"include":"Include","exclude":"Exclude","unsure":"Unsure",None:"Pending"}
SYSTEM_REVIEWER_IDS = {"final_resolved","editor","adjudicator"}
 
REVIEWER_DISPLAY = {
    "rev_reviewer_1":"Reviewer 1","rev_reviewer_2":"Reviewer 2",
    "rev_reviewer_3":"Reviewer 3","rev_editor":"Editor",
}
 
# Internal code → always English (used for DB storage)
_EXCLUDE_CODES = {
    "Wrong population":    "wrong_population",
    "Wrong intervention":  "wrong_intervention",
    "Wrong comparator":    "wrong_comparator",
    "Wrong outcome":       "wrong_outcome",
    "Wrong study design":  "wrong_study_design",
    "Animal study":        "animal_study",
    "Conference abstract": "conference_abstract",
    "Duplicate":           "duplicate",
    "Language barrier":    "language_barrier",
    "Sample size too small":"small_sample",
    "No control group":    "no_control",
    "Other":               "other",
}
_UNSURE_CODES = {
    "Unclear population":  "unclear_population",
    "Unclear outcome":     "unclear_outcome",
    "Needs full text":     "needs_full_text",
    "Borderline design":   "borderline_design",
    "Possibly duplicate":  "possibly_duplicate",
    "Other":               "other",
}
_FULLTEXT_EXCLUDE_CODES = {
    "Wrong outcome measure":"wrong_outcome_measure",
    "Insufficient follow-up":"insufficient_followup",
    "Data not extractable": "data_not_extractable",
    "Protocol deviation":   "protocol_deviation",
    "No control group":     "no_control",
    "Wrong population":     "wrong_population",
    "Wrong intervention":   "wrong_intervention",
    "Animal study":         "animal_study",
    "Other":                "other",
}

# Translation key mapping — English label → i18n key
_REASON_T_KEYS = {
    "Wrong population":    "reason_wrong_population",
    "Wrong intervention":  "reason_wrong_intervention",
    "Wrong comparator":    "reason_wrong_comparator",
    "Wrong outcome":       "reason_wrong_outcome",
    "Wrong study design":  "reason_wrong_study_design",
    "Animal study":        "reason_animal_study",
    "Conference abstract": "reason_conference_abstract",
    "Duplicate":           "reason_duplicate",
    "Language barrier":    "reason_language_barrier",
    "Sample size too small":"reason_small_sample",
    "No control group":    "reason_no_control",
    "Other":               "reason_other",
    "Unclear population":  "reason_unclear_population",
    "Unclear outcome":     "reason_unclear_outcome",
    "Needs full text":     "reason_needs_full_text",
    "Borderline design":   "reason_borderline_design",
    "Possibly duplicate":  "reason_possibly_duplicate",
    "Wrong outcome measure":"reason_wrong_outcome_measure",
    "Insufficient follow-up":"reason_insufficient_followup",
    "Data not extractable": "reason_data_not_extractable",
    "Protocol deviation":   "reason_protocol_deviation",
}

def _translated_reasons(codes_dict: dict) -> dict:
    """Return {translated_label: internal_code} for current language."""
    return {t(_REASON_T_KEYS.get(lbl, lbl)): code
            for lbl, code in codes_dict.items()}

# Public dicts — translated labels mapping to internal codes
def EXCLUDE_REASONS():        return _translated_reasons(_EXCLUDE_CODES)
def UNSURE_REASONS():         return _translated_reasons(_UNSURE_CODES)
def FULLTEXT_EXCLUDE_REASONS():return _translated_reasons(_FULLTEXT_EXCLUDE_CODES)
 
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
    all_reasons = {**(EXCLUDE_REASONS()), **UNSURE_REASONS(), **FULLTEXT_EXCLUDE_REASONS()}
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

def _db_task_sc(review_id, task):
    return f"r{review_id}_{task}"

def _deserialise_sc(task, raw):
    if raw is None:
        return None
    try:
        if task in ("s1_pico", "s2_pico"):
            from pipeline.pico_extractor import PICOExtraction
            return PICOExtraction(**{
                k: raw.get(k) or ""
                for k in ("population", "intervention", "comparison", "outcome")
            })
    except Exception:
        pass
    return None

def _get_cached(review_id, pmid, task):
    # 1. Session state — fast path
    val = st.session_state.get(_ck(review_id, pmid, task))
    if val is not None:
        return val
    # 2. DB fallback — survives refresh and reconnect
    try:
        raw = ai_repo.get_analysis(pmid, _db_task_sc(review_id, task))
        if raw is not None:
            logger.debug("DB cache hit: pmid=%s task=%s", pmid, task)
            obj = _deserialise_sc(task, raw)
            if obj is not None:
                st.session_state[_ck(review_id, pmid, task)] = obj
            return obj
        else:
            logger.debug("DB cache miss: pmid=%s task=%s", pmid, task)
    except Exception as e:
        logger.warning("DB cache read failed: %s", e)
    return None

def _set_cached(review_id, pmid, task, value):
    st.session_state[_ck(review_id, pmid, task)] = value
    try:
        if task in ("s1_pico", "s2_pico"):
            data = {
                "population":   getattr(value, "population",   "") or "",
                "intervention": getattr(value, "intervention", "") or "",
                "comparison":   getattr(value, "comparison",   "") or "",
                "outcome":      getattr(value, "outcome",      "") or "",
            }
        else:
            from dataclasses import asdict
            data = (asdict(value)
                    if hasattr(value, "__dataclass_fields__") else value)
        ai_repo.save_analysis(pmid, _db_task_sc(review_id, task), data)
    except Exception as e:
        logger.warning("Screening cache DB write failed (task=%s): %s", task, e)

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

# _PICO_PALETTE = {
#     "P": ("#BFDBFE", "#1E40AF", "Population"),
#     "I": ("#BBF7D0", "#065F46", "Intervention"),
#     "C": ("#FED7AA", "#92400E", "Comparison"),
#     "O": ("#E9D5FF", "#6B21A8", "Outcome"),
# }

_PICO_PALETTE = {
    "P": ("#FECACA", "#991B1B", "Population"),    # Red
    "I": ("#FED7AA", "#92400E", "Intervention"),   # Orange
    "C": ("#FEF08A", "#713F12", "Comparison"),     # Yellow
    "O": ("#BBF7D0", "#065F46", "Outcome"),        # Green
}




def _pico_color_legend() -> str:
    badges = " ".join(
        f'<span style="background:{bg};color:{fg};border-radius:3px;'
        f'padding:1px 7px;font-size:0.72rem;font-weight:600;">'
        f'{ltr}: {name}</span>'
        for ltr, (bg, fg, name) in _PICO_PALETTE.items()
    )
    return (
        f'<div style="margin-bottom:0.5rem;display:flex;'
        f'gap:0.3rem;flex-wrap:wrap;">{badges}</div>'
    )

def _compute_pico_relevance_score(review_id: int, cached_pico) -> Optional[int]:
    """
    0–100 score. 25 points per non-empty extracted PICO element.
    P=25, I=25, C=25, O=25.
    All 4 extracted → 100%. 3 → 75%. 2 → 50%. 1 → 25%.
    """
    if cached_pico is None:
        return None

    WEIGHTS = {
        "population":   25,
        "intervention": 25,
        "comparison":   25,
        "outcome":      25,
    }

    return sum(
        w for k, w in WEIGHTS.items()
        if (getattr(cached_pico, k, "") or "").strip()
    )

def _compute_pico_consistency(s1_pico, s2_pico) -> dict:
    """
    Compare Stage 1 abstract PICO vs Stage 2 full-text PICO.
    Uses recall-based word overlap: checks how many abstract PICO
    words appear in the full-text extraction (lenient — handles
    cases where full text gives more detail on the same concept).
    Returns per-element scores and an overall consistency flag.
    """
    ELEMENTS = ["population", "intervention", "comparison", "outcome"]
    results  = {}
    n_flags  = 0

    for el in ELEMENTS:
        s1 = (getattr(s1_pico, el, "") or "").strip().lower()
        s2 = (getattr(s2_pico, el, "") or "").strip().lower()

        s1_words = {w for w in re.split(r'\W+', s1) if len(w) >= 3}
        s2_words = {w for w in re.split(r'\W+', s2) if len(w) >= 3}

        if not s1_words and not s2_words:
            score = 1.0                          # both empty → consistent
        elif not s1_words or not s2_words:
            score = 0.0                          # one empty → inconsistent
        else:
            score = len(s1_words & s2_words) / len(s1_words)

        flagged = score < 0.25 and bool(s1_words)
        if flagged:
            n_flags += 1

        results[el] = {
            "s1":      getattr(s1_pico, el, "") or "",
            "s2":      getattr(s2_pico, el, "") or "",
            "score":   score,
            "flagged": flagged,
        }

    return {"elements": results, "n_flags": n_flags,
            "consistent": n_flags == 0}


_HIGHLIGHT_STOPWORDS = frozenset({
    # 5+ char common English words
    "their", "there", "these", "those", "which", "while", "where",
    "about", "after", "among", "other", "using", "being", "given",
    "found", "shown", "based", "first", "three", "years", "lower",
    "upper", "prior", "since", "never", "every", "often", "still",
    "could", "would", "should", "might", "shall", "until", "under",
    "above", "below", "along", "between", "before", "during", "within",
    "through", "against", "without", "despite", "following", "including",
    # Generic clinical/trial words (too common to be meaningful highlights)
    "patient", "patients", "disease", "cancer", "tumor", "tumour",
    "clinical", "treatment", "treated", "therapy", "therapies",
    "analysis", "results", "outcome", "overall", "primary", "secondary",
    "response", "survival", "related", "associated", "compared",
    "reported", "observed", "significant", "significantly",
    "received", "receiving", "administered", "enrolled", "included",
    "study", "studies", "trial", "trials", "group", "groups",
    "cohort", "median", "months", "weeks", "cases", "levels",
    "higher", "lower", "rates", "total", "cells",
})

def _highlight_pico_extraction(abstract: str, pico) -> tuple:
    """
    Colour-code extracted PICO elements in the abstract.

    Three strategies, all consistent: FIRST occurrence only.

      1. Exact phrase match — find() returns first occurrence naturally.

      2. Comma-split — splits "OS, EFS, relapse, and NRM" into individual
         sub-terms and exact-matches each one separately. Handles short
         acronyms (OS, EFS, NRM) with no length restriction.

      3. Word-level last resort — significant words (≥5 chars, not in
         stopwords), first occurrence only. Only reached when strategies
         1 and 2 both fail entirely.

    Returns (html_str, any_highlighted: bool).
    """
    import html as _html

    if not abstract:
        return _html.escape(abstract or ""), False

    elements = {
        "P": (getattr(pico, "population",   "") or "").strip(),
        "I": (getattr(pico, "intervention", "") or "").strip(),
        "C": (getattr(pico, "comparison",   "") or "").strip(),
        "O": (getattr(pico, "outcome",      "") or "").strip(),
    }

    abstract_lower = abstract.lower()
    # (start, end, label, priority)  priority: 0=exact, 1=sub-term, 2=word
    raw_spans: list = []

    for label, text in elements.items():
        if not text or len(text) < 2:
            continue
        text_lower = text.lower()

        # ── Strategy 1: exact phrase (first occurrence) ────────────────
        idx = abstract_lower.find(text_lower)
        if idx >= 0:
            raw_spans.append((idx, idx + len(text), label, 0))
            continue

        # ── Strategy 2: comma-split sub-terms (each exact-matched) ─────
        # "OS, EFS, relapse, and NRM" → ["OS","EFS","relapse","NRM"]
        sub_terms = [
            s.strip().strip(".,;-()[]\"'")
            for s in re.split(
                r',|\band\b|\bor\b|\bwho\b|\bwhich\b|\bthat\b'
                r'|\bafter\b|\bbefore\b|\bwhen\b|\bwhile\b|\bsince\b',
                text, flags=re.IGNORECASE
            )
            if s.strip().strip(".,;-()[]\"'")
        ]
        # Only use sub-term strategy when there are genuinely multiple terms
        found_sub = False
        if len(sub_terms) > 1 or (len(sub_terms) == 1
                                   and sub_terms[0].lower() != text_lower):
            for sub in sub_terms:
                if len(sub) < 2:
                    continue
                sub_lower = sub.lower()
                # Use word boundary for short terms to avoid partial matches
                if len(sub) <= 4:
                    m = re.search(
                        r'\b' + re.escape(sub_lower) + r'\b',
                        abstract_lower
                    )
                    if m:
                        raw_spans.append((m.start(), m.start() + len(sub),
                                          label, 1))
                        found_sub = True
                else:
                    idx2 = abstract_lower.find(sub_lower)
                    if idx2 >= 0:
                        raw_spans.append((idx2, idx2 + len(sub), label, 1))
                        found_sub = True

        if found_sub:
            continue

        # ── Strategy 3: word-level, first occurrence only ──────────────
        sig_words = [
            w.strip(".,;:()[]'\"/-") for w in text.split()
            if len(w.strip(".,;:()[]'\"/-")) >= 5
            and w.strip(".,;:()[]'\"/-").lower() not in _HIGHLIGHT_STOPWORDS
        ]
        for word in sig_words:
            idx3 = abstract_lower.find(word.lower())   # first only — no loop
            if idx3 >= 0:
                raw_spans.append((idx3, idx3 + len(word), label, 2))

    if not raw_spans:
        return _html.escape(abstract), False

    # ── Resolve overlaps ──────────────────────────────────────────────
    # earlier start → higher priority → longer span
    raw_spans.sort(key=lambda s: (s[0], s[3], -(s[1] - s[0])))

    merged: list = []
    last_end = -1
    for start, end, label, _ in raw_spans:
        if start >= last_end:
            merged.append((start, end, label))
            last_end = end

    # ── Build HTML ────────────────────────────────────────────────────
    parts: list = []
    cursor = 0
    for start, end, label in merged:
        if start > cursor:
            parts.append(_html.escape(abstract[cursor:start]))
        bg, fg, name = _PICO_PALETTE[label]
        parts.append(
            f'<mark style="background:{bg};color:{fg};border-radius:3px;'
            f'padding:0 3px;font-weight:500;" title="{name}">'
            f'{_html.escape(abstract[start:end])}</mark>'
        )
        cursor = end
    if cursor < len(abstract):
        parts.append(_html.escape(abstract[cursor:]))

    return "".join(parts), True


def _get_pico_terms(review_id=None) -> list:
    """Extract PICO search terms from session state for highlighting."""
    _saved = st.session_state.get(f"pico_form_{review_id}", {}) if review_id else {}
    terms = []
    for key in ["population","intervention","comparison","outcome"]:
        val = (_saved.get(key, "") or "").strip()
        if val:
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
 
def _sort_articles(articles, sort_key, my_decisions, review_id=None):
    if sort_key in ("Relevance score (high → low)", "Relevance score (low → high)"):
        def _rel_key(a):
            pico = _get_cached(review_id, a["pmid"], "s1_pico") if review_id else None
            s = _compute_pico_relevance_score(review_id, pico)
            return s if s is not None else -1
        reverse = sort_key == "Relevance score (high → low)"
        return sorted(articles, key=_rel_key, reverse=reverse)
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
            t("blinded_msg").format(reviewer=_reviewer_label(current_reviewer_id))
        )
 
    subtabs = st.tabs([
        t("tab_s1"),
        t("tab_s2"),
        t("tab_conflicts"),
        t("tab_agreement"),
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
        st.info(t("no_articles"))
        return
 
    my_decisions = {a["pmid"]: a.get("decision") for a in articles}
    decided_count = sum(1 for d in my_decisions.values() if d is not None)
    total_count   = len(articles)
 
    # Controls
    ctrl1, ctrl2, ctrl3 = st.columns([3, 3, 1])
    with ctrl1:
        _S1_FILTER_KEYS = ["All", "Pending", "Decided", "Conflicts (Editor)"]
        _S1_FILTER_TK   = ["all", "pending", "decided", "conflicts_editor"]
        _fi = st.radio(
            t("show"),
            options=range(len(_S1_FILTER_KEYS)),
            format_func=lambda i: t(_S1_FILTER_TK[i]),
            index=0, horizontal=True, key="s1_filter",
        )
        filter_view = _S1_FILTER_KEYS[_fi]
    with ctrl2:
        _S1_SORT_KEYS = [
            "Pending first, then A→Z", "Decided first, then A→Z",
            "A → Z (title)", "Newest first", "Oldest first",
            "Relevance score (high → low)", "Relevance score (low → high)",
        ]
        _S1_SORT_TK = [
            "sort_pending_az", "sort_decided_az", "sort_a_z",
            "sort_newest", "sort_oldest", "sort_rel_high", "sort_rel_low",
        ]
        _si = st.selectbox(
            t("sort_by"),
            options=range(len(_S1_SORT_KEYS)),
            format_func=lambda i: t(_S1_SORT_TK[i]),
            key="s1_sort",
        )
        sort_key = _S1_SORT_KEYS[_si]

    with ctrl3:
        st.metric(t("progress_metric"), f"{decided_count}/{total_count}")

    # ── Bulk PICO extraction ───────────────────────────────────────────────────
    _all_articles_for_pico = article_repo.get_articles_for_review(
        review_id, current_reviewer_id, stage="title_abstract")
    _unextracted = [
        a for a in _all_articles_for_pico
        if not _get_cached(review_id, a["pmid"], "s1_pico")
        and (a.get("abstract") or "").strip()
    ]
    _extracted_n = len(_all_articles_for_pico) - len(_unextracted)

    ba1, ba2 = st.columns([2, 3])
    with ba1:
        _all_lbl = (
            f"{t('bulk_extract_all')} ({len(_unextracted)} remaining)"
            if _unextracted else t("already_extracted")
        )
        if st.button(_all_lbl, key=f"s1pico_all_{review_id}",
                     type="primary", disabled=not _unextracted):
            _prog = st.progress(0, text="Starting PICO extraction…")
            _total = len(_unextracted)
            for _i, _art in enumerate(_unextracted):
                _pmid = _art["pmid"]
                _prog.progress(
                    _i / _total,
                    text=f"🔬 Extracting PICO — {_i + 1} of {_total} articles…"
                )
                try:
                    _res = extract_pico(_art.get("abstract", ""))
                    _set_cached(review_id, _pmid, "s1_pico", _res)
                except Exception as _e:
                    logger.error("Bulk PICO failed pmid=%s: %s", _pmid, _e)
            _prog.progress(1.0, text=f"✅ PICO extracted for {_total} articles.")
            st.rerun()
    with ba2:
        if _extracted_n > 0:
            st.caption(
                t("pico_already_extracted").format(
                    n=_extracted_n,
                    total=len(_all_articles_for_pico)
                )
            )
    # ── End bulk extraction ───────────────────────────────────────────────────
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
        st.info(f"{t('no_articles_match')} {t(_S1_FILTER_TK[_fi])}")
        return
 
    filtered = _sort_articles(filtered, sort_key, my_decisions, review_id=review_id)
    st.caption(f"{t('showing')} {len(filtered)} {t('of')} {total_count} {t('articles')}")
    pico_terms = _get_pico_terms(review_id=review_id)

    # ── PICO extraction queue ──────────────────────────────────────────────────
    _pq_key = f"s1_pico_queue_{review_id}"
    st.session_state.setdefault(_pq_key, [])
    _pq     = st.session_state[_pq_key]

    if _pq:
        _next_pmid  = _pq[0]
        _next_art   = next((a for a in articles if a["pmid"] == _next_pmid), None)
        _already    = _get_cached(review_id, _next_pmid, "s1_pico")

        if _next_art and not _already:
            n = len(_pq)
            with st.spinner(
                f"🔬 Extracting PICO — "
                f"{'1 article' if n == 1 else f'{n} remaining in queue'}…"
            ):
                try:
                    result = extract_pico(_next_art.get("abstract", ""))
                    _set_cached(review_id, _next_pmid, "s1_pico", result)
                except Exception as e:
                    logger.error("PICO queue extraction failed pmid=%s: %s",
                                 _next_pmid, e)

        st.session_state[_pq_key].pop(0)   # done (or already cached) — advance
        st.rerun()                          # immediately process next item
    # ── End queue ─────────────────────────────────────────────────────────────

    for article in filtered:
        _render_s1_card(article, review_id, current_reviewer_id, is_arbiter,
                        conflict_pmids, pico_terms)
 
 
def _render_s1_card(article, review_id, current_reviewer_id, is_arbiter,
                    conflict_pmids, pico_terms):
    pmid        = article["pmid"]
    cached_pico = _get_cached(review_id, pmid, "s1_pico")   # needed by badge + abstract

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
            _rel_score = _compute_pico_relevance_score(review_id, cached_pico)
            if _rel_score is not None:
                _rb, _rf = (
                    ("#D1FAE5", "#065F46") if _rel_score >= 70 else
                    ("#FEF3C7", "#92400E") if _rel_score >= 40 else
                    ("#FFE4E6", "#9F1239")
                )
                _rel_badge = (
                    f'<span style="background:{_rb};color:{_rf};'
                    f'border-radius:4px;padding:1px 8px;'
                    f'font-size:0.75rem;font-weight:700;" '
                    f'title="PICO relevance score">'
                    f'{_rel_score}%</span>'
                )
            else:
                _rel_badge = (
                    '<span style="background:#F1F5F9;color:#64748B;'
                    'border-radius:4px;padding:1px 8px;'
                    'font-size:0.75rem;font-weight:600;" '
                    'title="Extract PICO to see relevance score">'
                    '?%</span>'
                )

            st.markdown(
                f'<div style="display:flex;flex-wrap:wrap;gap:0.35rem;margin-top:0.25rem;">'
                f'<span class="sr-pill">{article.get("journal","") or "Unknown"}</span>'
                f'<span class="sr-pill">{article.get("year","") or ""}</span>'
                f'<span class="sr-source-badge {_src_cls}">{_src_lbl}</span>'
                f'{_rel_badge}'
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
                f'text-transform:uppercase;">{_dlabel(dec)}</div>'
                f'</div>', unsafe_allow_html=True)
 
        # ── Blinded / arbiter decision grid ───────────────────────────
        primary = {k:v for k,v in all_decisions.items() if k not in SYSTEM_REVIEWER_IDS}
        if is_arbiter and primary:
            dec_cols = st.columns(max(len(primary),1))
            for col,(rev_id,dec) in zip(dec_cols,primary.items()):
                col.markdown(
                    f"<div style='text-align:center'><small>{_reviewer_label(rev_id)}</small><br>"
                    f"<span style='font-size:1.4em'>{DECISION_EMOJI.get(dec,'⬜')}</span><br>"
                    f"<small><b>{_dlabel(dec)}</b></small></div>",
                    unsafe_allow_html=True)
        elif not is_arbiter:
            others = sum(1 for k,v in primary.items()
                         if k!=current_reviewer_id and v is not None)
            total_o= sum(1 for k in primary if k!=current_reviewer_id)
            if total_o>0:
                st.caption(t("other_reviewers_screened").format(
                    n=others, total=total_o))
 
        # ── Adjudication notice ────────────────────────────────────────
        if is_adjudicated and is_arbiter:
            final  = adjudication["final_decision"]
            st.success(
                f"{t('resolved_label')} {DECISION_EMOJI.get(final,'')} {_dlabel(final)}"
                + (f" · *{adjudication['notes']}*" if adjudication.get("notes") else ""))
        elif has_conflict and is_arbiter:
            st.warning(f"{t('conflict_label')} {_classify_conflict(all_decisions)}. {t('resolve_in_tab')}")
 
        # ── Abstract (layered highlighting: PICO extraction > search terms > plain)
        abstract     = article.get("abstract") or ""


        with st.expander(t("abstract"), expanded=True):
            if cached_pico and abstract:
                pico_html, any_hit = _highlight_pico_extraction(abstract, cached_pico)
                if any_hit:
                    st.markdown(_pico_color_legend(), unsafe_allow_html=True)
                    st.markdown(
                        f'<div style="font-size:0.88rem;line-height:1.65;">'
                        f'{pico_html}</div>',
                        unsafe_allow_html=True)
                else:
                    # LLM paraphrased heavily — fall back to search-term highlight
                    body = (_highlight_abstract(abstract, pico_terms)
                            if pico_terms else _html.escape(abstract))
                    st.markdown(
                        f'<div style="font-size:0.88rem;line-height:1.65;">'
                        f'{body}</div>',
                        unsafe_allow_html=True)
                    st.caption(
                        "ℹ️ PICO phrases not found verbatim — "
                        "LLM may have paraphrased. Check the banner below.")
            elif pico_terms and abstract:
                st.markdown(
                    f'<div style="font-size:0.88rem;line-height:1.65;">'
                    f'{_highlight_abstract(abstract, pico_terms)}</div>',
                    unsafe_allow_html=True)
            else:
                st.write(abstract or "No abstract.")

        # ── Per-card PICO extractor (queue-based) ─────────────────────
        _pq_key   = f"s1_pico_queue_{review_id}"
        _queue    = st.session_state.get(_pq_key, [])
        _in_queue = pmid in _queue
        _q_pos    = (_queue.index(pmid) + 1) if _in_queue else 0

        pc1, pc2 = st.columns([1, 4])
        if _in_queue:
            pc1.button(f"⏳ #{_q_pos} in queue", key=f"s1pico_{pmid}",
                       type="secondary", disabled=True)
        else:
            _lbl = t("btn_re_extract") if cached_pico else t("btn_extract_pico")
            if pc1.button(_lbl, key=f"s1pico_{pmid}", type="secondary"):
                st.session_state.setdefault(_pq_key, [])
                if pmid not in st.session_state[_pq_key]:
                    st.session_state[_pq_key].append(pmid)
                st.rerun()

        if cached_pico:
            with pc2.container():
                p = cached_pico
                st.markdown(
                    f'<div style="background:#F8FAFF;border:1px solid #BFDBFE;'
                    f'border-radius:8px;padding:0.6rem 0.9rem;font-size:0.82rem;">'
                    f'<b style="color:#1E40AF;">P:</b> {p.population or "—"}'
                    f' &nbsp;|&nbsp; '
                    f'<b style="color:#065F46;">I:</b> {p.intervention or "—"}'
                    f' &nbsp;|&nbsp; '
                    f'<b style="color:#92400E;">C:</b> {p.comparison or "—"}'
                    f' &nbsp;|&nbsp; '
                    f'<b style="color:#6B21A8;">O:</b> {p.outcome or "—"}'
                    f'</div>',
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
                _rmap = EXCLUDE_REASONS() if _effective=="exclude" else UNSURE_REASONS()
                st.markdown(f'<div style="font-size:0.72rem;font-weight:600;'
                    f'text-transform:uppercase;color:#6B7280;margin:0.5rem 0 0.3rem;">'
                    f'{t("reason")}</div>', unsafe_allow_html=True)
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
            _note = st.text_input(t("note_optional"), value=_saved_note,
                key=f"s1note_{pmid}_{current_reviewer_id}",
                placeholder=t("placeholder_s2_note"))
 
            # Decision + Submit buttons
            st.markdown(f'<div style="font-size:0.72rem;font-weight:600;'
                f'text-transform:uppercase;color:#6B7280;margin:0.5rem 0 0.25rem;">'
                f'{t("decision")}</div>', unsafe_allow_html=True)
            b1,b2,b3,bsub = st.columns([2,2,2,3])
 
            if b1.button(t("btn_include"), key=f"s1inc_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="include" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="include" else "include"
                st.rerun()
            if b2.button(t("btn_exclude"), key=f"s1exc_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="exclude" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="exclude" else "exclude"
                st.rerun()
            if b3.button(t("btn_unsure"),  key=f"s1uns_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="unsure" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="unsure" else "unsure"
                st.rerun()
 
            _reason_to_save = _build_reason(_sel, _note)
            _can_submit = _effective is not None
            if bsub.button(
                t("btn_submit") if _pending is not None else t("btn_update"),
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
                    st.caption(f"**{_reviewer_label(rev_id)}** {t('reviewer_reasoning')}")
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
    mc1.metric(t("passed_stage1"), total2)
    mc2.metric(t("have_full_text"), len(ft_pmids))
    mc3.metric(t("stage2_progress"), f"{decided2}/{total2}")
    if total2 > 0:
        st.progress(decided2/total2)
    st.divider()
 
    if not s1_included:
        st.info("No included articles found.")
        return
 
    # Controls
    ctrl1, ctrl2 = st.columns([3,3])
    with ctrl1:
        _S2_FILTER_KEYS = ["All", "Pending", "Decided"]
        _S2_FILTER_TK   = ["all", "pending", "decided"]
        _fi2 = st.radio(
            t("show"),
            options=range(len(_S2_FILTER_KEYS)),
            format_func=lambda i: t(_S2_FILTER_TK[i]),
            index=0, horizontal=True, key="s2_filter",
        )
        f2 = _S2_FILTER_KEYS[_fi2]
    with ctrl2:
        _S2_SORT_KEYS = ["Pending first, then A→Z", "A → Z (title)", "Newest first"]
        _S2_SORT_TK   = ["sort_pending_az", "sort_a_z", "sort_newest"]
        _si2 = st.selectbox(
            t("sort_by"),
            options=range(len(_S2_SORT_KEYS)),
            format_func=lambda i: t(_S2_SORT_TK[i]),
            key="s2_sort",
        )
        s2_sort = _S2_SORT_KEYS[_si2]
 
    filtered2 = []
    for a in s1_included:
        d = my_s2_decisions.get(a["pmid"])
        if f2=="Pending" and d is not None: continue
        if f2=="Decided" and d is None: continue
        filtered2.append(a)
 
    filtered2 = _sort_articles(filtered2, s2_sort, my_s2_decisions)
    st.caption(
        f"{t('showing')} {len(filtered2)} {t('of')} {total2} {t('articles')} — "
        f"{len(ft_pmids)} {t('have_retrieved')}"
    )

    # ── Bulk actions ───────────────────────────────────────────────────────────
    _needs_pico = [
        a for a in s1_included
        if _get_cached(review_id, a["pmid"], "s1_pico")
        and not _get_cached(review_id, a["pmid"], "s2_pico")
        and (ft_map.get(a["pmid"]) or a.get("abstract", "")).strip()
    ]
    _needs_sum = [
        a for a in s1_included
        if not _get_cached(review_id, a["pmid"], "s2_summary")
        and (ft_map.get(a["pmid"]) or a.get("abstract", "")).strip()
    ]

    bb1, bb2 = st.columns(2)
    with bb1:
        _pico_lbl = (
            f"{t('bulk_check_all')} ({len(_needs_pico)} remaining)"
            if _needs_pico else t("all_checked")
        )
        if st.button(_pico_lbl, key=f"s2pico_all_{review_id}",
                     type="primary", disabled=not _needs_pico):
            _prog = st.progress(0, text="Starting PICO consistency checks…")
            _total_p = len(_needs_pico)
            for _i, _art in enumerate(_needs_pico):
                _pmid = _art["pmid"]
                _prog.progress(
                    _i / _total_p,
                    text=f"🔍 Checking PICO — {_i + 1} of {_total_p} articles…"
                )
                try:
                    _src = (ft_map.get(_pmid) or _art.get("abstract", ""))
                    _s2 = extract_pico(_src[:3000])
                    _set_cached(review_id, _pmid, "s2_pico", _s2)
                except Exception as _e:
                    logger.error("Bulk PICO consistency failed pmid=%s: %s",
                                 _pmid, _e)
            _prog.progress(1.0,
                text=f"✅ PICO consistency checked for {_total_p} articles.")
            st.rerun()

    with bb2:
        _sum_lbl = (
            f"{t('bulk_summarise_all')} ({len(_needs_sum)} remaining)"
            if _needs_sum else t("all_summarised")
        )
        if st.button(_sum_lbl, key=f"s2sum_all_{review_id}",
                     type="primary", disabled=not _needs_sum):
            _prog2 = st.progress(0, text="Starting summarisation…")
            _total_s = len(_needs_sum)
            for _i, _art in enumerate(_needs_sum):
                _pmid = _art["pmid"]
                _prog2.progress(
                    _i / _total_s,
                    text=f"📝 Summarising — {_i + 1} of {_total_s} articles…"
                )
                try:
                    _text = (ft_map.get(_pmid) or _art.get("abstract", ""))
                    _res  = summarize_with_llm(_text[:4000])
                    _set_cached(review_id, _pmid, "s2_summary",
                                _res or "(no output)")
                except Exception as _e:
                    logger.error("Bulk summarise failed pmid=%s: %s", _pmid, _e)
            _prog2.progress(1.0,
                text=f"✅ Summarised {_total_s} articles.")
            st.rerun()
    # ── End bulk actions ───────────────────────────────────────────────────────

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
            ft_badge = (
                f'<span style="font-size:0.7rem;font-weight:600;padding:0.1rem 0.45rem;'
                f'border-radius:4px;background:#D1FAE5;color:#065F46;margin-right:0.4rem;">'
                f'{t("ft_badge")}</span>' if has_ft else
                f'<span style="font-size:0.7rem;font-weight:600;padding:0.1rem 0.45rem;'
                f'border-radius:4px;background:#FEF3C7;color:#92400E;margin-right:0.4rem;">'
                f'{t("abstract_only_badge")}</span>'
            )
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
        # ── PDF upload (manual full text) ─────────────────────────────
        if not has_ft:
            with st.expander(t("upload_pdf_title"), expanded=False):
                st.caption(t("upload_pdf_desc"))
                uploaded = st.file_uploader(
                    "Choose PDF", type=["pdf"],
                    key=f"pdf_upload_{pmid}_{review_id}",
                    label_visibility="collapsed"
                )
                if uploaded is not None:
                    with st.spinner("Extracting text from PDF…"):
                        extracted_text, n_pages = _extract_pdf_text(uploaded)
                    if extracted_text and len(extracted_text.strip()) > 100:
                        # Save to DB
                        try:
                            from models.schemas import Article as _Article, \
                                ArticleSource as _AS, ResearchDomain as _RD
                            _src = article.get("source","pubmed")
                            try:
                                _src_enum = _AS(_src)
                            except ValueError:
                                _src_enum = _AS.EUROPE_PMC
                            _art_obj = _Article(
                                pmid=pmid,
                                title=article.get("title",""),
                                abstract=article.get("abstract",""),
                                authors=article.get("authors",[]),
                                journal=article.get("journal",""),
                                year=article.get("year",""),
                                doi=article.get("doi",""),
                                source=_src_enum,
                                domain=_RD.MEDICAL,
                                full_text=extracted_text,
                            )
                            article_repo.save_full_texts([_art_obj])
                            # Update ft_map so summariser uses it immediately
                            full_text = extracted_text
                            has_ft    = True
                            ft_map[pmid] = extracted_text
                            st.success(
                                t("upload_pdf_success").format(
                                    pages=n_pages,
                                    chars=f"{len(extracted_text):,}"
                                )
                            )
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not save to DB: {e}")
                    else:
                        st.error(t("upload_pdf_error"))

        # ── Full text or abstract ──────────────────────────────────────
        display_text = full_text if has_ft else (article.get("abstract") or "")
        label = (t("ft_label_chars").format(n=f"{len(full_text):,}")
                 if has_ft else t("abstract_no_ft"))
        with st.expander(label, expanded=True):
            if has_ft:
                preview = display_text[:3000]
                if len(display_text) > 3000:
                    preview += (
                        "\n\n*[Display truncated — full text used for AI summary]*"
                    )
                st.markdown(preview)
            else:
                st.write(display_text or t("no_text_available"))
                st.caption(t("retrieve_hint"))

        # ── PICO Consistency Check ─────────────────────────────────────
        s1_pico = _get_cached(review_id, pmid, "s1_pico")
        s2_pico = _get_cached(review_id, pmid, "s2_pico")

        if s1_pico:
            _PICO_LABELS = {
                "population":   "Population",
                "intervention": "Intervention",
                "comparison":   "Comparison",
                "outcome":      "Outcome",
            }
            pc1, pc2 = st.columns([1, 4])
            _btn_lbl = (
                t("btn_recheck_pico") if s2_pico else t("btn_check_pico")
            )
            if pc1.button(_btn_lbl, key=f"s2pico_{pmid}",
                          type="secondary"):
                _src_text = (full_text[:3000] if has_ft
                             else article.get("abstract", ""))
                if not _src_text:
                    st.warning("No text available for PICO extraction.")
                else:
                    _label = ("full text" if has_ft else "abstract")
                    with st.spinner(
                        f"Extracting PICO from {_label} for comparison…"
                    ):
                        try:
                            s2_pico = extract_pico(_src_text)
                            _set_cached(review_id, pmid, "s2_pico", s2_pico)
                        except Exception as _e:
                            st.error(f"PICO extraction error: {_e}")

            if s2_pico:
                _cx = _compute_pico_consistency(s1_pico, s2_pico)

                with pc2.container():
                    if _cx["consistent"]:
                        st.markdown(
                            f'<div style="background:#D1FAE5;border:1px solid '
                            f'#6EE7B7;border-radius:8px;padding:0.45rem 0.9rem;'
                            f'font-size:0.82rem;">'
                            f'{t("pico_consistent")}'
                            f'</div>',
                            unsafe_allow_html=True)
                    else:
                        _flagged_names = [
                            _PICO_LABELS[el]
                            for el, v in _cx["elements"].items()
                            if v["flagged"]
                        ]
                        _verb = (t("pico_diff_single") if len(_flagged_names) == 1
                                 else t("pico_diff_multi"))
                        st.markdown(
                            f'<div style="background:#FEF3C7;border:1px solid '
                            f'#FCD34D;border-radius:8px;padding:0.45rem 0.9rem;'
                            f'font-size:0.82rem;">'
                            f'{t("pico_discrepancy").format(elements=", ".join(_flagged_names), verb=_verb)}'
                            f'</div>',
                            unsafe_allow_html=True)

                with st.expander(t("pico_comparison"), expanded=_cx["n_flags"] > 0):
                    for el, v in _cx["elements"].items():
                        _icon = "⚠️" if v["flagged"] else "✅"
                        _score_pct = round(v["score"] * 100)
                        st.markdown(
                            f'**{_icon} {_PICO_LABELS[el]}** '
                            f'<span style="font-size:0.75rem;color:#6B7280;">'
                            f'({_score_pct}% {t("overlap_pct")})</span>',
                            unsafe_allow_html=True)
                        c_s1, c_s2 = st.columns(2)
                        c_s1.caption(t("abstract_col"))
                        c_s1.markdown(v["s1"] or t("not_extracted"))
                        c_s2.caption(t("full_text_col"))
                        c_s2.markdown(v["s2"] or t("not_extracted"))
                        st.divider()
        elif not s1_pico:
            st.caption(t("extract_pico_s1_hint"))  

        # ── Per-card AI Summariser ─────────────────────────────────────
        cached_sum = _get_cached(review_id, pmid, "s2_summary")
        
        sc1, sc2 = st.columns([1,4])
        if sc1.button(t("btn_summarise"), key=f"s2sum_{pmid}", type="secondary"):
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
                    f'{t("ai_summary_label")}<br>{cached_sum}</div>',
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
                _rmap = FULLTEXT_EXCLUDE_REASONS() if _effective=="exclude" else UNSURE_REASONS()
                st.markdown(f'<div style="font-size:0.72rem;font-weight:600;'
                    f'text-transform:uppercase;color:#6B7280;margin:0.5rem 0 0.3rem;">'
                    f'{t("exclusion_reason")}</div>', unsafe_allow_html=True)
                tcols = st.columns(3)
                _sel = list(_saved_tags)
                for i,(lbl,key) in enumerate(_rmap.items()):
                    chk = tcols[i%3].checkbox(lbl, value=(key in _sel),
                                              key=f"s2tag_{pmid}_{current_reviewer_id}_{key}")
                    if chk and key not in _sel: _sel.append(key)
                    elif not chk and key in _sel: _sel.remove(key)
            else:
                _sel = list(_saved_tags)
 
            _note = st.text_input(t("note_optional"), value=_saved_note,
                key=f"s2note_{pmid}_{current_reviewer_id}",
                placeholder=t("placeholder_s2_note"))
 
            st.markdown(f'<div style="font-size:0.72rem;font-weight:600;'
                f'text-transform:uppercase;color:#6B7280;margin:0.5rem 0 0.25rem;">'
                f'{t("stage2_decision")}</div>', unsafe_allow_html=True)
            b1,b2,b3,bsub = st.columns([2,2,2,3])
 
            if b1.button(t("btn_include"), key=f"s2inc_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="include" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="include" else "include"
                st.rerun()
            if b2.button(t("btn_exclude"), key=f"s2exc_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="exclude" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="exclude" else "exclude"
                st.rerun()
            if b3.button(t("btn_unsure"),  key=f"s2uns_{pmid}_{current_reviewer_id}",
                type="primary" if _effective=="unsure" else "secondary", use_container_width=True):
                st.session_state[_pend_key] = None if _pending=="unsure" else "unsure"
                st.rerun()
 
            _reason_to_save = _build_reason(_sel, _note)
            _can_submit = _effective is not None
            if bsub.button(
                t("btn_submit") if _pending is not None else t("btn_update"),
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
        st.info(t("conflicts_editor_only"))
        return
 
    _STAGE_KEYS  = ["title_abstract", "full_text"]
    _stage_idx = st.radio(
        t("stage_select"),
        options=range(2),
        format_func=lambda i: [t("stage1_tab_abstract"), t("stage2_full_text")][i],
        horizontal=True, key="conflict_stage",
    )
    stage     = ["Stage 1 — Title/Abstract", "Stage 2 — Full Text"][_stage_idx]
    stage_key = _STAGE_KEYS[_stage_idx]

    conflicts = screening_repo.get_conflicts(review_id, stage_key)

    if not conflicts:
        st.success(f"✅ No decision conflicts at {stage}.")
        _render_adjudicated_summary(review_id, stage_key)
    else:
        st.markdown(f"### ⚠️ {len(conflicts)} article(s) require adjudication")
        for c in conflicts:
            pmid  = c["pmid"]
            title = c.get("title","")[:80]
            all_dec = screening_repo.get_all_decisions_for_article(
                review_id, pmid, stage_key)
            primary = {k:v for k,v in all_dec.items()
                       if k not in SYSTEM_REVIEWER_IDS}
            adj = adjudication_repo.get_adjudication(
                review_id, pmid, stage=stage_key)

            with st.expander(f"⚠️ {title}", expanded=False):
                dec_cols = st.columns(max(len(primary),1))
                for col,(rev_id,dec) in zip(dec_cols,primary.items()):
                    row = screening_repo.get_decision_with_reason(
                        review_id, pmid, stage_key, rev_id)
                    col.markdown(
                        f"<div style='text-align:center'>"
                        f"<small>{_reviewer_label(rev_id)}</small><br>"
                        f"<span style='font-size:1.4em'>"
                        f"{DECISION_EMOJI.get(dec,'⬜')}</span><br>"
                        f"<small><b>{_dlabel(dec)}"
                        f"</b></small></div>",
                        unsafe_allow_html=True)
                    if row and row.get("reason"):
                        _render_reason_display(row["reason"], dec)

                if adj:
                    final = adj["final_decision"]
                    st.success(
                        f"✅ **Resolved:** "
                        f"{DECISION_EMOJI.get(final,'')} {final.capitalize()}"
                        + (f" · *{adj['notes']}*" if adj.get("notes") else ""))
                else:
                    st.warning("Not yet resolved.")
                    adj_dec = st.selectbox(
                        "Final decision:", ["include","exclude"],
                        key=f"adj_dec_{pmid}_{stage_key}")
                    adj_notes = st.text_area(
                        "Notes:", key=f"adj_notes_{pmid}_{stage_key}",
                        height=80)
                    _rmap_adj = (FULLTEXT_EXCLUDE_REASONS()
                                 if stage_key == "full_text"
                                 else EXCLUDE_REASONS())
                    _adj_opts = ["— (not applicable)"] + list(_rmap_adj.keys())
                    _adj_lbl = st.radio(
                        "Canonical exclusion reason (if excluding):",
                        _adj_opts, index=0,
                        key=f"adj_reason_{pmid}_{stage_key}")
                    _final_reason = (_rmap_adj[_adj_lbl]
                                     if _adj_lbl in _rmap_adj else "")
                    if st.button(
                        "✅ Resolve",
                        key=f"adj_submit_{pmid}_{stage_key}",
                        type="primary"):
                        adjudication_repo.save_adjudication(
                            review_id, pmid, adj_dec, current_reviewer_id,
                            notes=adj_notes, stage=stage_key,
                            final_reason=_final_reason)
                        st.rerun()
            st.divider()

        _render_adjudicated_summary(review_id, stage_key)

    # ── Always shown — reason conflicts persist after decision conflicts resolved
    reason_conflicts = screening_repo.get_reason_conflicts(review_id, stage_key)
    if reason_conflicts:
        st.divider()
        st.warning(
            f"📝 **{len(reason_conflicts)} reason conflict(s)** — "
            "both reviewers excluded with different reasons. "
            "Set a canonical reason for the PRISMA breakdown."
        )
        _rmap_rc = (FULLTEXT_EXCLUDE_REASONS() if stage_key == "full_text"
                    else EXCLUDE_REASONS())
        for rc in reason_conflicts:
            pmid_rc  = rc["pmid"]
            title_rc = rc.get("title","")[:80]
            with st.expander(f"📝 {title_rc}", expanded=True):
                st.caption(
                    f"Reviewer reasons: {rc.get('reasons_summary','')}")
                _rc_lbl = st.radio(
                    "Select canonical reason:",
                    list(_rmap_rc.keys()), index=0,
                    key=f"rc_reason_{pmid_rc}_{stage_key}")
                if st.button(
                    "💾 Set canonical reason",
                    key=f"rc_sub_{pmid_rc}_{stage_key}",
                    type="primary"):
                    adjudication_repo.save_adjudication(
                        review_id, pmid_rc, "exclude",
                        current_reviewer_id,
                        conflict_type="reason_conflict",
                        notes="Canonical reason set by Editor",
                        stage=stage_key,
                        final_reason=_rmap_rc[_rc_lbl])
                    st.rerun()
 
 
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
    st.markdown(f"### {t('inter_rater')}")
    st.caption(t("inter_rater_caption"))
 
    c1,c2,c3,c4 = st.columns(4)
    c1.metric(t("dual_screened"), stats["dual_screened"])
    c2.metric(t("agreements"),    stats["agreements"])
    c3.metric(t("conflicts"),     stats["conflicts"])
    c4.metric(t("agreement_pct"), f"{stats['agreement_pct']}%")
 
    pct = stats["agreement_pct"]
    if stats["dual_screened"] > 0:
        if pct >= 90:   st.success(f"✅ {pct}% — Excellent (≥90%)")
        elif pct >= 80: st.warning(f"⚠️ {pct}% — Acceptable (80–90%)")
        else:           st.error(f"❌ {pct}% — Poor (<80%). Calibration recommended.")
 
    st.divider()
    st.markdown(t("reporting_template"))
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
    st.markdown(f"#### {t('per_reviewer')}")
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
                    t("reviewer_col"):                  _reviewer_label(rev_id),
                    t("stage_col"):                     stage_label,
                    f"{t('decision_include')} 🟢":      counts_r["include"],
                    f"{t('decision_exclude')} 🔴":      counts_r["exclude"],
                    f"{t('decision_unsure')} 🟡":       counts_r["unsure"],
                    t("total_col"):                     tot,
                    t("decided_pct"):                   f"{round(100*tot/max(len(articles),1))}%",
                })
    if agg_rows:
        st.dataframe(pd.DataFrame(agg_rows), use_container_width=True, hide_index=True)
    else:
        st.caption(t("no_decisions_yet"))
 
    st.divider()
 
    # ── Detailed Decision Log — Editor only (blinding protection) ─────
    current_reviewer_id = st.session_state.get("current_reviewer_id","rev_reviewer_1")
    is_arbiter = "editor" in current_reviewer_id.lower()
 
    if not is_arbiter:
        st.info(t("log_protected"))
        return
 
    st.markdown(t("detailed_log"))
    active = [r for r in reviewer_ids if any(
        screening_repo.get_decision(review_id, art["pmid"], "title_abstract", r)
        for art in articles)]
 
    if not active:
        st.caption(t("no_decisions_yet"))
        return
 
    rev_sel = st.selectbox("Reviewer:", active, format_func=_reviewer_label,
                           key="sum_rev_sel")
    stage_sel = st.radio("Stage:", ["Stage 1 — Title/Abstract","Stage 2 — Full Text"],
                         horizontal=True, key="sum_stage_sel")
    stage_key = "title_abstract" if "1" in stage_sel else "full_text"
 
    all_reasons = {**EXCLUDE_REASONS(), **UNSURE_REASONS(), **FULLTEXT_EXCLUDE_REASONS()}
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
            t("download_csv").format(
                reviewer=_reviewer_label(rev_sel), stage=stage_sel),
            data=df.to_csv(index=False),
            file_name=f"screening_{rev_sel}_{stage_key}_{review_id}.csv",
            mime="text/csv", use_container_width=True)
    else:
        st.caption(f"No {stage_sel} decisions from {_reviewer_label(rev_sel)} yet.")


