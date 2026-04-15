# storage/repository.py
# Full replacement — adds AdjudicationRepository and PrismaSettingsRepository,
# fixes get_screening_counts to work correctly with multi-reviewer data.

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from models.schemas import Article, PICOQuery
from storage.database import get_connection

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
class ReviewRepository:

    def create_review(self, title: str, description: str = "",
                      pico: Optional[PICOQuery] = None) -> int:
        pico_json = pico.model_dump_json() if pico else None
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO reviews (title, description, pico_json) VALUES (?, ?, ?)",
                (title, description, pico_json)
            )
            return cursor.lastrowid

    def list_reviews(self) -> List[Dict]:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT id, title, status, created_at FROM reviews ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_review(self, review_id: int) -> Optional[Dict]:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM reviews WHERE id = ?", (review_id,)
            ).fetchone()
        return dict(row) if row else None

    def update_review_status(self, review_id: int, status: str) -> None:
        with get_connection() as conn:
            conn.execute(
                "UPDATE reviews SET status = ?, updated_at = ? WHERE id = ?",
                (status, datetime.now(), review_id)
            )

# ─────────────────────────────────────────────────────────────────────────────
class ArticleRepository:

    def save_articles(self, articles: List[Article], review_id: int,
                      search_id: int) -> Dict[str, int]:
        """
        Persist articles to DB with cross-source duplicate detection.
 
        Three-key deduplication before inserting:
          1. pmid  — same pmid already in DB (same-source duplicate)
          2. doi   — same DOI, different pmid (cross-source: CORE vs PubMed/EPMC)
          3. title — normalised title match (last resort for DOI-less articles)
 
        When a cross-source duplicate is detected, we link the EXISTING article's
        pmid to this review instead of inserting a new row. This keeps the articles
        table clean (one row per real paper) and prevents duplicates in Screening.
        """
        if not articles:
            return {"saved": 0, "duplicates": 0}
 
        import re as _re
 
        def _norm_doi(raw: str) -> str:
            d = (raw or "").strip().lower()
            for pfx in ("https://doi.org/", "http://doi.org/",
                        "https://dx.doi.org/", "doi:"):
                if d.startswith(pfx):
                    d = d[len(pfx):]
                    break
            return d.strip()
 
        def _norm_title(t: str) -> str:
            t = _re.sub(r"[^a-z0-9 ]", "", (t or "").lower())
            t = _re.sub(r"\s+", " ", t).strip()
            return t[:60]
 
        with get_connection() as conn:
            before = conn.execute(
                "SELECT COUNT(*) FROM review_articles WHERE review_id = ?",
                (review_id,)
            ).fetchone()[0]
 
            # Build lookup maps from what's already in the DB for this review
            existing_rows = conn.execute("""
                SELECT a.pmid, a.doi, a.title
                FROM articles a
                JOIN review_articles ra ON a.pmid = ra.pmid
                WHERE ra.review_id = ?
            """, (review_id,)).fetchall()
 
            db_pmid_set:  dict = {}   # pmid → canonical pmid
            db_doi_map:   dict = {}   # norm_doi → canonical pmid
            db_title_map: dict = {}   # norm_title → canonical pmid
 
            for row in existing_rows:
                epid  = row["pmid"]
                edoi  = _norm_doi(row["doi"] or "")
                etitle= _norm_title(row["title"] or "")
                db_pmid_set[epid] = epid
                if edoi:
                    db_doi_map[edoi]     = epid
                if etitle:
                    db_title_map[etitle] = epid
 
            # Also build in-batch maps (articles in current batch may dedup each other)
            batch_doi_map:   dict = {}   # norm_doi → first article in this batch
            batch_title_map: dict = {}   # norm_title → first article
 
            articles_to_insert = []   # will go into articles table
            review_links       = []   # (review_id, pmid, search_id) to link
 
            for art in articles:
                pmid  = (art.pmid or "").strip()
                doi   = _norm_doi(art.doi or "")
                # CORE articles store DOI as pmid field
                if not doi and pmid.startswith("10."):
                    doi = pmid
                title = _norm_title(art.title or "")
 
                # ── Check DB for existing match ────────────────────────────
                canonical_pmid = None
                if pmid in db_pmid_set:
                    canonical_pmid = pmid          # exact pmid match in DB
                elif doi and doi in db_doi_map:
                    canonical_pmid = db_doi_map[doi]   # DOI match in DB
                elif title and title in db_title_map:
                    canonical_pmid = db_title_map[title]   # title match in DB
 
                # ── Check within current batch ────────────────────────────
                if canonical_pmid is None:
                    if doi and doi in batch_doi_map:
                        canonical_pmid = batch_doi_map[doi]
                    elif title and title in batch_title_map:
                        canonical_pmid = batch_title_map[title]
 
                if canonical_pmid and canonical_pmid != pmid:
                    # Cross-source duplicate: link the canonical article to review
                    review_links.append((review_id, canonical_pmid, search_id))
                    logger.debug(
                        "save_articles: cross-source dedup — pmid=%r → canonical=%r "
                        "(doi=%r title=%r)",
                        pmid, canonical_pmid, doi, title[:40]
                    )
                    continue   # skip inserting this duplicate
 
                # ── New article: insert and register in lookup maps ────────
                articles_to_insert.append(art)
                review_links.append((review_id, pmid, search_id))
 
                # Update DB lookup maps so later articles in this batch see it
                db_pmid_set[pmid] = pmid
                if doi:
                    db_doi_map[doi]       = pmid
                    batch_doi_map[doi]    = pmid
                if title:
                    db_title_map[title]   = pmid
                    batch_title_map[title] = pmid
 
            # ── Batch insert unique articles ───────────────────────────────
            if articles_to_insert:
                conn.executemany("""
                    INSERT INTO articles
                    (pmid, title, abstract, authors, journal, year,
                     doi, source, domain, url, venue, citation_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pmid) DO UPDATE SET
                        title          = excluded.title,
                        abstract       = excluded.abstract,
                        source         = excluded.source,
                        domain         = excluded.domain,
                        venue          = excluded.venue,
                        doi            = excluded.doi,
                        url            = excluded.url,
                        citation_count = excluded.citation_count
                """, [
                    (
                        a.pmid, a.title, a.abstract,
                        json.dumps(a.authors), a.journal, a.year,
                        a.doi,
                        a.source.value if hasattr(a.source, "value") else str(a.source),
                        a.domain.value if hasattr(a.domain, "value") else str(a.domain),
                        a.url, a.venue, a.citation_count,
                    )
                    for a in articles_to_insert
                ])
 
            # ── Link all articles (canonical) to this review ───────────────
            if review_links:
                conn.executemany("""
                    INSERT OR IGNORE INTO review_articles (review_id, pmid, search_id)
                    VALUES (?, ?, ?)
                """, review_links)
 
            after = conn.execute(
                "SELECT COUNT(*) FROM review_articles WHERE review_id = ?",
                (review_id,)
            ).fetchone()[0]
 
            saved      = after - before
            duplicates = len(articles) - saved
            logger.info(
                "save_articles: review_id=%d input=%d unique=%d "
                "cross-source-dupes=%d saved=%d",
                review_id, len(articles), len(articles_to_insert),
                len(articles) - len(articles_to_insert), saved
            )
            return {"saved": saved, "duplicates": duplicates}

# ── ADD these two methods to ArticleRepository in storage/repository.py ──────
#
# Paste them inside the ArticleRepository class, after save_articles().
# They handle storing and retrieving PMC full text from the separate
# full_texts table created by Migration 005.

    def save_full_texts(self, articles: List[Article]) -> int:
        """
        Persist full_text from Article objects into the full_texts table.

        Called after save_articles() when articles come from PMCClient.
        Only saves articles that actually have full_text content.
        Returns count of full texts saved.
        """
        to_save = [a for a in articles if a.full_text and a.full_text.strip()]
        if not to_save:
            return 0

        with get_connection() as conn:
            conn.executemany("""
                INSERT INTO full_texts (pmid, full_text, word_count)
                VALUES (?, ?, ?)
                ON CONFLICT(pmid) DO UPDATE SET
                    full_text  = excluded.full_text,
                    word_count = excluded.word_count,
                    fetched_at = CURRENT_TIMESTAMP
            """, [
                (a.pmid, a.full_text, len(a.full_text.split()))
                for a in to_save
            ])

            # Mark has_full_text = 1 on the articles row
            conn.executemany(
                "UPDATE articles SET has_full_text = 1 WHERE pmid = ?",
                [(a.pmid,) for a in to_save]
            )

        logger.info("save_full_texts: saved %d full texts", len(to_save))
        return len(to_save)

    def get_full_text(self, pmid: str) -> Optional[str]:
        """
        Retrieve full text for one article. Returns None if not available.
        Called by the AI pipeline when full-text analysis is needed.
        """
        with get_connection() as conn:
            row = conn.execute(
                "SELECT full_text FROM full_texts WHERE pmid = ?",
                (pmid,)
            ).fetchone()
        return row["full_text"] if row else None

    def has_full_text(self, pmid: str) -> bool:
        """Quick check without loading the full text."""
        with get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM full_texts WHERE pmid = ?",
                (pmid,)
            ).fetchone()
        return row is not None    
    
    def get_articles_for_review(
        self,
        review_id: int,
        reviewer_id: str = "rev_reviewer_1",   # ← was Optional[str] = None
        stage: str = "title_abstract"
        ) -> List[Dict]:
        """
        Return articles for a review with the given reviewer's screening decision.

        BUG FIX: The LEFT JOIN on screening_decisions had no reviewer_id filter.
        With multiple reviewers, each article has one screening_decisions row per
        reviewer. Without filtering, the JOIN produces N output rows per article
        (where N = number of reviewers who screened it), causing duplicate PMIDs
        in the result list and a StreamlitDuplicateElementKey crash in the UI.

        Fix: always filter the LEFT JOIN to a single reviewer_id. The caller
        passes the current reviewer from session_state.
        """
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT
                    a.pmid,
                    a.title,
                    a.abstract,
                    a.authors,
                    a.journal,
                    a.year,
                    a.source,
                    a.domain,
                    a.venue,
                    a.url,
                    a.citation_count,
                    sd.decision,
                    sd.reason,
                    sd.decided_at
                FROM articles a
                JOIN review_articles ra
                    ON a.pmid = ra.pmid AND ra.review_id = ?
                LEFT JOIN screening_decisions sd
                    ON  sd.pmid        = a.pmid
                    AND sd.review_id   = ?
                    AND sd.stage       = ?
                    AND sd.reviewer_id = ?          -- ← this line was missing
                ORDER BY sd.decision NULLS FIRST, a.pmid
            """, (review_id, review_id, stage, reviewer_id)).fetchall()

        result = []
        for row in rows:
            d = dict(row)
            try:
                d["authors"] = json.loads(d["authors"]) if d["authors"] else []
            except (json.JSONDecodeError, TypeError):
                d["authors"] = []
            result.append(d)
        return result

    def get_screening_counts(
            self,
            review_id: int,
            stage: str = "title_abstract",
            reviewer_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Return PRISMA-ready counts.

        When reviewer_id is None (the default used by the PRISMA diagram and
        dashboard), counts are based on the ADJUDICATED decision if one exists,
        otherwise the consensus decision across all primary reviewers, otherwise
        pending.

        When reviewer_id is provided, counts reflect only that reviewer's decisions.

        Multi-reviewer counting strategy:
          - Consensus (all reviewers agree) → that decision counts
          - Conflict (reviewers disagree, no adjudication) → counts as 'conflict'
            which maps to pending for PRISMA purposes
          - Adjudicated → adjudicator's final decision counts
        """
        with get_connection() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM review_articles WHERE review_id = ?",
                (review_id,)
            ).fetchone()[0]

            if reviewer_id:
                # Single-reviewer view
                rows = conn.execute("""
                    SELECT decision, COUNT(*) AS n
                    FROM screening_decisions
                    WHERE review_id = ? AND stage = ? AND reviewer_id = ?
                    GROUP BY decision
                """, (review_id, stage, reviewer_id)).fetchall()
                decision_counts = {row["decision"]: row["n"] for row in rows}

            else:
                # ── Adjudicated decisions (highest authority) ─────────────────
                adj_rows = conn.execute("""
                    SELECT final_decision, COUNT(*) AS n
                    FROM adjudications
                    WHERE review_id = ? AND stage = ?
                    GROUP BY final_decision
                """, (review_id, stage)).fetchall()
                adj_counts = {row["final_decision"]: row["n"] for row in adj_rows}
                adj_pmids = set(conn.execute(
                    "SELECT pmid FROM adjudications WHERE review_id = ? AND stage = ?",
                    (review_id, stage)
                ).fetchall().__iter__().__next__.__self__
                    if False else [
                    r["pmid"] for r in conn.execute(
                        "SELECT pmid FROM adjudications WHERE review_id = ? AND stage = ?",
                        (review_id, stage)
                    ).fetchall()
                ])

                # ── Consensus among non-adjudicated articles ──────────────────
                # For each pmid not yet adjudicated, check if all reviewers agree
                non_adj_rows = conn.execute("""
                    SELECT pmid, decision, COUNT(DISTINCT reviewer_id) AS reviewer_count
                    FROM screening_decisions
                    WHERE review_id = ? AND stage = ?
                      AND reviewer_id NOT IN ('final_resolved', 'editor', 'adjudicator')
                    GROUP BY pmid, decision
                """, (review_id, stage)).fetchall()

                # Group by pmid to find consensus
                from collections import defaultdict
                pmid_decisions: Dict[str, Dict[str, int]] = defaultdict(dict)
                for row in non_adj_rows:
                    if row["pmid"] not in adj_pmids:
                        pmid_decisions[row["pmid"]][row["decision"]] = row["reviewer_count"]

                consensus_counts = {"include": 0, "exclude": 0, "unsure": 0, "conflict": 0}
                for pmid, dec_map in pmid_decisions.items():
                    if len(dec_map) == 1:
                        # All reviewers agree
                        decision = list(dec_map.keys())[0]
                        consensus_counts[decision] = consensus_counts.get(decision, 0) + 1
                    elif len(dec_map) > 1:
                        consensus_counts["conflict"] += 1

                # Merge adjudicated + consensus
                decision_counts = {
                    "include": adj_counts.get("include", 0) + consensus_counts["include"],
                    "exclude": adj_counts.get("exclude", 0) + consensus_counts["exclude"],
                    "unsure":  adj_counts.get("unsure", 0)  + consensus_counts["unsure"],
                    "conflict": consensus_counts["conflict"],
                }

        counts = {
            "total":    total,
            "included": decision_counts.get("include", 0),
            "excluded": decision_counts.get("exclude", 0),
            "unsure":   decision_counts.get("unsure", 0),
            "conflict": decision_counts.get("conflict", 0),
            "pending":  0,
        }
        screened = counts["included"] + counts["excluded"] + counts["unsure"]
        counts["pending"] = max(total - screened - counts["conflict"], 0)
        return counts

# ─────────────────────────────────────────────────────────────────────────────
class ScreeningRepository:

    VALID_DECISIONS = {"include", "exclude", "unsure"}

    def save_decision(
            self,
            review_id: int,
            pmid: str,
            stage: str,
            decision: str,
            reason: str = "",
            reviewer_id: Optional[str] = None
    ) -> None:
        decision = decision.lower().strip()
        if decision not in self.VALID_DECISIONS:
            raise ValueError(
                f"Invalid decision '{decision}'. Must be one of {self.VALID_DECISIONS}"
            )
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO screening_decisions
                    (review_id, pmid, stage, decision, reason, reviewer_id)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(review_id, pmid, stage, reviewer_id) DO UPDATE SET
                    decision   = excluded.decision,
                    reason     = excluded.reason,
                    decided_at = CURRENT_TIMESTAMP
            """, (review_id, pmid, stage, decision, reason, reviewer_id))

    def get_all_decisions_for_article(
            self,
            review_id: int,
            pmid: str,
            stage: str = "title_abstract"
    ) -> Dict[str, str]:
        """Return {reviewer_id: decision} for all reviewers on one article."""
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT reviewer_id, decision
                FROM screening_decisions
                WHERE review_id = ? AND pmid = ? AND stage = ?
                ORDER BY decided_at
            """, (review_id, pmid, stage)).fetchall()
        return {row["reviewer_id"]: row["decision"] for row in rows}

    def get_decision(
            self,
            review_id: int,
            pmid: str,
            stage: str,
            reviewer_id: Optional[str] = None
    ) -> Optional[str]:
        with get_connection() as conn:
            row = conn.execute("""
                SELECT decision FROM screening_decisions
                WHERE review_id = ? AND pmid = ? AND stage = ? AND reviewer_id = ?
            """, (review_id, pmid, stage, reviewer_id)).fetchone()
        return row["decision"] if row else None

    def get_conflicts(
            self,
            review_id: int,
            stage: str = "title_abstract"
    ) -> List[Dict]:
        """
        Return articles where two or more reviewers have different decisions.
        Excludes articles already adjudicated.
        """
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT
                    sd.pmid,
                    a.title,
                    GROUP_CONCAT(sd.reviewer_id || ':' || sd.decision, ' | ') AS decisions_summary,
                    COUNT(DISTINCT sd.decision) AS unique_decision_count,
                    COUNT(DISTINCT sd.reviewer_id) AS reviewer_count
                FROM screening_decisions sd
                JOIN articles a ON sd.pmid = a.pmid
                WHERE sd.review_id = ?
                  AND sd.stage     = ?
                  AND sd.reviewer_id NOT IN ('final_resolved', 'editor', 'adjudicator')
                  AND sd.pmid NOT IN (
                      SELECT pmid FROM adjudications
                      WHERE review_id = ? AND stage = ?
                  )
                GROUP BY sd.pmid, a.title
                HAVING COUNT(DISTINCT sd.decision) > 1
                ORDER BY a.title
            """, (review_id, stage, review_id, stage)).fetchall()
        return [dict(r) for r in rows]

    def get_agreements(
        self,
        review_id: int,
        stage: str = "title_abstract"
    ) -> Dict[str, float]:
        """
        Compute inter-rater agreement statistics.

        FIX: The original method called get_conflicts() which only returns
        UNRESOLVED conflicts. Once the Arbiter adjudicates an article, it
        disappears from get_conflicts(), making the historical conflict count
        drop to 0 — which is wrong for reporting.

        The correct count is: total conflicts = unresolved conflicts NOW
        + adjudicated conflicts (resolved). Both represent articles where
        reviewers originally disagreed.
        """
        with get_connection() as conn:
            dual_screened = conn.execute("""
                SELECT COUNT(DISTINCT pmid) FROM (
                    SELECT pmid
                    FROM screening_decisions
                    WHERE review_id = ? AND stage = ?
                    AND reviewer_id NOT IN ('final_resolved', 'arbiter', 'adjudicator')
                    GROUP BY pmid
                    HAVING COUNT(DISTINCT reviewer_id) >= 2
                )
            """, (review_id, stage)).fetchone()[0]

            # Adjudicated articles were conflicted by definition — count them
            adjudicated_count = conn.execute("""
                SELECT COUNT(*) FROM adjudications
                WHERE review_id = ? AND stage = ?
            """, (review_id, stage)).fetchone()[0]

        # Unresolved conflicts still open right now
        unresolved_conflicts = len(self.get_conflicts(review_id, stage))

        # Total historical conflicts = still open + already resolved by adjudication
        total_conflicts = unresolved_conflicts + adjudicated_count

        agreements = max(dual_screened - total_conflicts, 0)
        pct = round((agreements / dual_screened * 100), 1) if dual_screened > 0 else 0.0

        return {
            "dual_screened":       dual_screened,
            "agreements":          agreements,
            "conflicts":           total_conflicts,        # historical total
            "unresolved_conflicts": unresolved_conflicts,  # currently open
            "adjudicated":         adjudicated_count,      # resolved by arbiter
            "agreement_pct":       pct,
        }

# ─────────────────────────────────────────────────────────────────────────────
class AdjudicationRepository:
    """
    Stores and retrieves final adjudicated decisions.

    In a systematic review, when two primary reviewers disagree, a third
    party (the Editor or Lead Reviewer) makes a binding final decision.
    That decision is stored here, separate from the original reviewer decisions,
    preserving the full audit trail while providing a single authoritative answer
    for PRISMA counting purposes.
    """

    VALID_DECISIONS = {"include", "exclude", "unsure"}

    def save_adjudication(
            self,
            review_id: int,
            pmid: str,
            final_decision: str,
            adjudicator_id: str,
            conflict_type: str = "",
            notes: str = "",
            stage: str = "title_abstract"
    ) -> None:
        """Record the editor's final decision for a conflicted article."""
        final_decision = final_decision.lower().strip()
        if final_decision not in self.VALID_DECISIONS:
            raise ValueError(
                f"Invalid final_decision '{final_decision}'. "
                f"Must be one of {self.VALID_DECISIONS}"
            )
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO adjudications
                    (review_id, pmid, stage, conflict_type,
                     final_decision, adjudicator_id, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(review_id, pmid, stage) DO UPDATE SET
                    final_decision = excluded.final_decision,
                    adjudicator_id = excluded.adjudicator_id,
                    conflict_type  = excluded.conflict_type,
                    notes          = excluded.notes,
                    adjudicated_at = CURRENT_TIMESTAMP
            """, (review_id, pmid, stage, conflict_type,
                  final_decision, adjudicator_id, notes))

    def get_adjudication(
            self,
            review_id: int,
            pmid: str,
            stage: str = "title_abstract"
    ) -> Optional[Dict]:
        """Return the adjudicated decision for one article, or None."""
        with get_connection() as conn:
            row = conn.execute("""
                SELECT * FROM adjudications
                WHERE review_id = ? AND pmid = ? AND stage = ?
            """, (review_id, pmid, stage)).fetchone()
        return dict(row) if row else None

    def get_all_adjudications(
            self,
            review_id: int,
            stage: str = "title_abstract"
    ) -> List[Dict]:
        """Return all adjudicated decisions for a review."""
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT adj.*, a.title
                FROM adjudications adj
                JOIN articles a ON adj.pmid = a.pmid
                WHERE adj.review_id = ? AND adj.stage = ?
                ORDER BY adj.adjudicated_at DESC
            """, (review_id, stage)).fetchall()
        return [dict(r) for r in rows]

    def count_by_decision(
            self,
            review_id: int,
            stage: str = "title_abstract"
    ) -> Dict[str, int]:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT final_decision, COUNT(*) AS n
                FROM adjudications
                WHERE review_id = ? AND stage = ?
                GROUP BY final_decision
            """, (review_id, stage)).fetchall()
        return {row["final_decision"]: row["n"] for row in rows}

# ─────────────────────────────────────────────────────────────────────────────
class PrismaSettingsRepository:
    """
    Persist per-review PRISMA diagram customisation settings.
    Settings are stored as JSON so no schema changes are needed when
    new customisation options are added.
    """

    DEFAULT_SETTINGS = {
        "box_colors": {
            "identification": "#0B2545",
            "screening":      "#1B4F8A",
            "eligibility":    "#028090",
            "included":       "#2D6A4F",
            "excluded":       "#FEF3F2",
            "unsure":         "#FEF3F2",
        },
        "font_colors": {
            "identification": "#FFFFFF",
            "screening":      "#FFFFFF",
            "eligibility":    "#FFFFFF",
            "included":       "#FFFFFF",
            "excluded":       "#C0392B",
            "unsure":         "#C0392B",
        },
        "box_width":  0.38,
        "box_height": 0.10,
        "font_size":  11,
        "show_unsure_box": True,
        "custom_labels": {},   # {box_key: override_text}
    }

    def _is_valid_hex_color(value: str) -> bool:
        """Return True if value is a valid hex colour string for st.color_picker."""
        import re
        return bool(re.match(r'^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})$', str(value)))

    def get_settings(self, review_id: int) -> Dict:
        """Return settings for a review, falling back to defaults."""
        with get_connection() as conn:
            row = conn.execute(
                "SELECT settings_json FROM prisma_settings WHERE review_id = ?",
                (review_id,)
            ).fetchone()
        if not row:
            return dict(self.DEFAULT_SETTINGS)
        try:
            stored = json.loads(row["settings_json"])
            # Deep merge: stored values override defaults
            merged = dict(self.DEFAULT_SETTINGS)
            for k, v in stored.items():
                if isinstance(v, dict) and k in merged and isinstance(merged[k], dict):
                    merged[k] = {**merged[k], **v}
                else:
                    merged[k] = v
            # ── Self-heal invalid color values ─────────────────────────────
            for color_dict_key in ("box_colors", "font_colors"):
                for k, v in merged.get(color_dict_key, {}).items():
                    if not self._is_valid_hex_color(v):
                        merged[color_dict_key][k] = self.DEFAULT_SETTINGS[color_dict_key][k]
            return merged
        except (json.JSONDecodeError, TypeError):
            return dict(self.DEFAULT_SETTINGS)

    def save_settings(self, review_id: int, settings: Dict) -> None:
        """Persist settings for a review."""
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO prisma_settings (review_id, settings_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(review_id) DO UPDATE SET
                    settings_json = excluded.settings_json,
                    updated_at    = CURRENT_TIMESTAMP
            """, (review_id, json.dumps(settings)))

    def reset_settings(self, review_id: int) -> None:
        """Reset to defaults by deleting the stored settings row."""
        with get_connection() as conn:
            conn.execute(
                "DELETE FROM prisma_settings WHERE review_id = ?",
                (review_id,)
            )

# ─────────────────────────────────────────────────────────────────────────────
class AIAnalysisRepository:

    def save_analysis(self, pmid: str, task: str,
                      output: Any, model_name: str = "tinyllama") -> None:
        output_json = json.dumps(output) if not isinstance(output, str) else output
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO ai_analyses (pmid, task, output, model_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(pmid, task) DO UPDATE SET
                    output     = excluded.output,
                    model_name = excluded.model_name,
                    created_at = CURRENT_TIMESTAMP
            """, (pmid, task, output_json, model_name))

    def get_analysis(self, pmid: str, task: str) -> Optional[Any]:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT output FROM ai_analyses WHERE pmid = ? AND task = ?",
                (pmid, task)
            ).fetchone()
        if not row:
            return None
        try:
            return json.loads(row["output"])
        except (json.JSONDecodeError, TypeError):
            return row["output"]

    def has_analysis(self, pmid: str, task: str) -> bool:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM ai_analyses WHERE pmid = ? AND task = ?",
                (pmid, task)
            ).fetchone()
        return row is not None
