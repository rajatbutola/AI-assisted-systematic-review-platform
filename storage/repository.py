# storage/repository.py
# Full replacement — adds AdjudicationRepository and PrismaSettingsRepository,
# fixes get_screening_counts to work correctly with multi-reviewer data.


# storage/repository.py 17th April
#
# Changes from original (Migration 007):
#   ScreeningRepository.save_decision — now writes to
#   screening_decision_history in addition to screening_decisions.
#   All other methods are unchanged.








# storage/repository.py
#
# Changes from original (Migration 007):
#   ScreeningRepository.save_decision — now writes to
#   screening_decision_history in addition to screening_decisions.
#   All other methods are unchanged.

import json
import logging
from collections import defaultdict
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

        When a cross-source duplicate is detected, we link the EXISTING
        article's pmid to this review instead of inserting a new row.
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

            db_pmid_set:  dict = {}
            db_doi_map:   dict = {}
            db_title_map: dict = {}

            for row in existing_rows:
                epid   = row["pmid"]
                edoi   = _norm_doi(row["doi"] or "")
                etitle = _norm_title(row["title"] or "")
                db_pmid_set[epid] = epid
                if edoi:   db_doi_map[edoi]     = epid
                if etitle: db_title_map[etitle] = epid

            batch_doi_map:   dict = {}
            batch_title_map: dict = {}
            articles_to_insert = []
            review_links       = []

            for art in articles:
                pmid  = (art.pmid or "").strip()
                doi   = _norm_doi(art.doi or "")
                if not doi and pmid.startswith("10."):
                    doi = pmid
                title = _norm_title(art.title or "")

                canonical_pmid = None
                if pmid in db_pmid_set:
                    canonical_pmid = pmid
                elif doi and doi in db_doi_map:
                    canonical_pmid = db_doi_map[doi]
                elif title and title in db_title_map:
                    canonical_pmid = db_title_map[title]

                if canonical_pmid is None:
                    if doi and doi in batch_doi_map:
                        canonical_pmid = batch_doi_map[doi]
                    elif title and title in batch_title_map:
                        canonical_pmid = batch_title_map[title]

                if canonical_pmid and canonical_pmid != pmid:
                    review_links.append((review_id, canonical_pmid, search_id))
                    logger.debug(
                        "save_articles: cross-source dedup pmid=%r → canonical=%r",
                        pmid, canonical_pmid
                    )
                    continue

                articles_to_insert.append(art)
                review_links.append((review_id, pmid, search_id))
                db_pmid_set[pmid] = pmid
                if doi:   db_doi_map[doi]    = pmid; batch_doi_map[doi]    = pmid
                if title: db_title_map[title]= pmid; batch_title_map[title]= pmid

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

    def save_full_texts(self, articles: List[Article]) -> int:
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
            conn.executemany(
                "UPDATE articles SET has_full_text = 1 WHERE pmid = ?",
                [(a.pmid,) for a in to_save]
            )
        logger.info("save_full_texts: saved %d full texts.", len(to_save))
        return len(to_save)

    def get_full_text(self, pmid: str) -> Optional[str]:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT full_text FROM full_texts WHERE pmid = ?", (pmid,)
            ).fetchone()
            return row["full_text"] if row else None

    def has_full_text(self, pmid: str) -> bool:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM full_texts WHERE pmid = ?", (pmid,)
            ).fetchone()
            return row is not None

    def get_stage1_included_pmids(self, review_id: int) -> set:
        """PMIDs that have consensus 'include' at title_abstract stage.
        Used to populate Stage 2 full-text screening."""
        with get_connection() as conn:
            # Adjudicated first
            adj = {r["pmid"] for r in conn.execute(
                "SELECT pmid FROM adjudications WHERE review_id=? AND stage=? AND final_decision=?",
                (review_id, "title_abstract", "include")
            ).fetchall()}
            # Consensus include among primary reviewers (excluding system reviewers)
            rows = conn.execute("""
                SELECT pmid, decision, COUNT(DISTINCT reviewer_id) AS n
                FROM screening_decisions
                WHERE review_id=? AND stage=?
                AND reviewer_id NOT IN ('final_resolved','editor','adjudicator')
                GROUP BY pmid, decision
                HAVING COUNT(DISTINCT decision)=1 AND decision='include'
            """, (review_id, "title_abstract")).fetchall()
            consensus = {r["pmid"] for r in rows}
        return adj | consensus

    def get_articles_with_fulltext(self, review_id: int) -> List[Dict]:
        """Return articles that have full text retrieved AND passed Stage 1."""
        included_pmids = self.get_stage1_included_pmids(review_id)
        if not included_pmids:
            return []
        with get_connection() as conn:
            placeholders = ",".join("?" * len(included_pmids))
            rows = conn.execute(f"""
                SELECT a.pmid, a.title, a.abstract, a.authors,
                       a.journal, a.year, a.source, a.doi, a.url,
                       ft.full_text, ft.word_count
                FROM articles a
                JOIN review_articles ra ON a.pmid=ra.pmid AND ra.review_id=?
                JOIN full_texts ft ON a.pmid=ft.pmid
                WHERE a.pmid IN ({placeholders})
                ORDER BY a.title
            """, [review_id] + list(included_pmids)).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            try: d["authors"] = json.loads(d["authors"]) if d["authors"] else []
            except: d["authors"] = []
            result.append(d)
        return result

    def get_stage2_counts(self, review_id: int) -> Dict[str, int]:
        """Counts for Stage 2 (full_text stage) screening decisions."""
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT decision, COUNT(DISTINCT pmid) AS n
                FROM screening_decisions
                WHERE review_id=? AND stage='full_text'
                AND reviewer_id NOT IN ('final_resolved','editor','adjudicator')
                GROUP BY decision
            """, (review_id,)).fetchall()
        counts = {r["decision"]: r["n"] for r in rows}
        return {
            "s2_included": counts.get("include", 0),
            "s2_excluded": counts.get("exclude", 0),
            "s2_unsure":   counts.get("unsure", 0),
        }

    def get_articles_for_review(
        self,
        review_id: int,
        reviewer_id: str = "rev_reviewer_1",
        stage: str = "title_abstract"
    ) -> List[Dict]:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT
                    a.pmid, a.title, a.abstract, a.authors,
                    a.journal, a.year, a.source, a.domain,
                    a.venue, a.url, a.citation_count, a.doi,
                    ra.created_at AS added_to_review_at,
                    sd.decision, sd.reason, sd.decided_at
                FROM articles a
                JOIN review_articles ra
                    ON a.pmid = ra.pmid AND ra.review_id = ?
                LEFT JOIN screening_decisions sd
                    ON sd.pmid = a.pmid
                    AND sd.review_id = ?
                    AND sd.stage = ?
                    AND sd.reviewer_id = ?
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
        with get_connection() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM review_articles WHERE review_id = ?",
                (review_id,)
            ).fetchone()[0]

            if reviewer_id:
                rows = conn.execute("""
                    SELECT decision, COUNT(*) AS n
                    FROM screening_decisions
                    WHERE review_id = ? AND stage = ? AND reviewer_id = ?
                    GROUP BY decision
                """, (review_id, stage, reviewer_id)).fetchall()
                decision_counts = {row["decision"]: row["n"] for row in rows}

            else:
                adj_rows = conn.execute("""
                    SELECT final_decision, COUNT(*) AS n
                    FROM adjudications
                    WHERE review_id = ? AND stage = ?
                    GROUP BY final_decision
                """, (review_id, stage)).fetchall()
                adj_counts = {row["final_decision"]: row["n"] for row in adj_rows}

                adj_pmids = set(
                    r["pmid"] for r in conn.execute(
                        "SELECT pmid FROM adjudications WHERE review_id = ? AND stage = ?",
                        (review_id, stage)
                    ).fetchall()
                )

                non_adj_rows = conn.execute("""
                    SELECT pmid, decision, COUNT(DISTINCT reviewer_id) AS reviewer_count
                    FROM screening_decisions
                    WHERE review_id = ? AND stage = ?
                    AND reviewer_id NOT IN ('final_resolved', 'editor', 'adjudicator')
                    GROUP BY pmid, decision
                """, (review_id, stage)).fetchall()

                pmid_decisions: Dict[str, Dict[str, int]] = defaultdict(dict)
                for row in non_adj_rows:
                    if row["pmid"] not in adj_pmids:
                        pmid_decisions[row["pmid"]][row["decision"]] = row["reviewer_count"]

                consensus_counts = {"include": 0, "exclude": 0, "unsure": 0, "conflict": 0}
                for pmid, dec_map in pmid_decisions.items():
                    if len(dec_map) == 1:
                        decision = list(dec_map.keys())[0]
                        consensus_counts[decision] = consensus_counts.get(decision, 0) + 1
                    elif len(dec_map) > 1:
                        consensus_counts["conflict"] += 1

                decision_counts = {
                    "include":  adj_counts.get("include", 0)  + consensus_counts["include"],
                    "exclude":  adj_counts.get("exclude", 0)  + consensus_counts["exclude"],
                    "unsure":   adj_counts.get("unsure", 0)   + consensus_counts["unsure"],
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
        """
        Save (or update) a screening decision.

        Migration 007-D: Also writes to screening_decision_history for
        a full audit trail. The history table is append-only — it records
        every change, not just the current state.
        """
        decision = decision.lower().strip()
        if decision not in self.VALID_DECISIONS:
            raise ValueError(
                f"Invalid decision '{decision}'. Must be one of {self.VALID_DECISIONS}"
            )

        with get_connection() as conn:
            # ── Read current decision before overwriting (for history) ─────────
            current_row = conn.execute("""
                SELECT decision FROM screening_decisions
                WHERE review_id = ? AND pmid = ? AND stage = ? AND reviewer_id = ?
            """, (review_id, pmid, stage, reviewer_id)).fetchone()

            changed_from = current_row["decision"] if current_row else None

            # ── Write current state (upsert) ───────────────────────────────────
            conn.execute("""
                INSERT INTO screening_decisions
                    (review_id, pmid, stage, decision, reason, reviewer_id)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(review_id, pmid, stage, reviewer_id) DO UPDATE SET
                    decision   = excluded.decision,
                    reason     = excluded.reason,
                    decided_at = CURRENT_TIMESTAMP
            """, (review_id, pmid, stage, decision, reason, reviewer_id))

            # ── Append to history (007-D) ──────────────────────────────────────
            # Only write history if the decision actually changed (or is new)
            if changed_from != decision:
                try:
                    conn.execute("""
                        INSERT INTO screening_decision_history
                            (review_id, pmid, stage, reviewer_id,
                             decision, changed_from, reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (review_id, pmid, stage, reviewer_id,
                          decision, changed_from, reason))
                    logger.debug(
                        "Decision history: review=%d pmid=%s reviewer=%s "
                        "%s → %s",
                        review_id, pmid, reviewer_id, changed_from, decision
                    )
                except Exception as e:
                    # History write failure must never block the main save
                    logger.warning(
                        "Could not write decision history (migration 007-D "
                        "may not have run yet): %s", e
                    )

    def get_exclusion_reason_counts(
        self,
        review_id: int,
        stage: str = "title_abstract"
    ) -> Dict[str, int]:
        """
        Return a breakdown of exclusion reason tags for all excluded articles.

        Reason strings are stored as 'tag1,tag2||free note'.
        This method counts how many excluded articles have each tag.
        Used by the PRISMA panel to populate the exclusion breakdown.

        Returns dict like: {"wrong_population": 5, "animal_study": 3, ...}
        """
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT reason FROM screening_decisions
                WHERE review_id = ? AND stage = ? AND decision = 'exclude'
                AND reviewer_id NOT IN ('final_resolved', 'editor', 'adjudicator')
            """, (review_id, stage)).fetchall()

        counts: Dict[str, int] = {}
        for row in rows:
            reason_str = row["reason"] or ""
            # Split off the free-text note
            tags_part = reason_str.split("||", 1)[0]
            for tag in tags_part.split(","):
                tag = tag.strip()
                if tag:
                    counts[tag] = counts.get(tag, 0) + 1
        return counts

    def get_decision_history(
        self,
        review_id: int,
        pmid: str,
        stage: str = "title_abstract",
        reviewer_id: Optional[str] = None
    ) -> List[Dict]:
        """
        Return full decision history for one article.
        Useful for the Editor to see how decisions evolved.
        """
        with get_connection() as conn:
            if reviewer_id:
                rows = conn.execute("""
                    SELECT reviewer_id, decision, changed_from, reason, changed_at
                    FROM screening_decision_history
                    WHERE review_id = ? AND pmid = ? AND stage = ? AND reviewer_id = ?
                    ORDER BY changed_at ASC
                """, (review_id, pmid, stage, reviewer_id)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT reviewer_id, decision, changed_from, reason, changed_at
                    FROM screening_decision_history
                    WHERE review_id = ? AND pmid = ? AND stage = ?
                    ORDER BY changed_at ASC
                """, (review_id, pmid, stage)).fetchall()
        return [dict(r) for r in rows]

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

    def get_decision_with_reason(
        self,
        review_id: int,
        pmid: str,
        stage: str,
        reviewer_id: Optional[str] = None
    ) -> Optional[Dict]:
        """Return {decision, reason, decided_at} for one reviewer on one article.
        Used by the screening panel to pre-populate reason tags and note field.
        Returns None if no decision has been made yet.
        """
        with get_connection() as conn:
            row = conn.execute("""
                SELECT decision, reason, decided_at
                FROM screening_decisions
                WHERE review_id = ? AND pmid = ? AND stage = ? AND reviewer_id = ?
            """, (review_id, pmid, stage, reviewer_id)).fetchone()
        return dict(row) if row else None

    def get_conflicts(
        self,
        review_id: int,
        stage: str = "title_abstract"
    ) -> List[Dict]:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT
                    sd.pmid,
                    a.title,
                    GROUP_CONCAT(sd.reviewer_id || ':' || sd.decision, ' | ')
                        AS decisions_summary,
                    COUNT(DISTINCT sd.decision)    AS unique_decision_count,
                    COUNT(DISTINCT sd.reviewer_id) AS reviewer_count
                FROM screening_decisions sd
                JOIN articles a ON sd.pmid = a.pmid
                WHERE sd.review_id = ?
                  AND sd.stage = ?
                  AND sd.reviewer_id NOT IN ('final_resolved','editor','adjudicator')
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
        with get_connection() as conn:
            dual_screened = conn.execute("""
                SELECT COUNT(DISTINCT pmid) FROM (
                    SELECT pmid
                    FROM screening_decisions
                    WHERE review_id = ? AND stage = ?
                    AND reviewer_id NOT IN ('final_resolved','arbiter','adjudicator')
                    GROUP BY pmid
                    HAVING COUNT(DISTINCT reviewer_id) >= 2
                )
            """, (review_id, stage)).fetchone()[0]

            adjudicated_count = conn.execute("""
                SELECT COUNT(*) FROM adjudications
                WHERE review_id = ? AND stage = ?
            """, (review_id, stage)).fetchone()[0]

        unresolved_conflicts = len(self.get_conflicts(review_id, stage))
        total_conflicts      = unresolved_conflicts + adjudicated_count
        agreements           = max(dual_screened - total_conflicts, 0)
        pct = round((agreements / dual_screened * 100), 1) if dual_screened > 0 else 0.0

        return {
            "dual_screened":      dual_screened,
            "agreements":         agreements,
            "conflicts":          total_conflicts,
            "unresolved_conflicts": unresolved_conflicts,
            "adjudicated":        adjudicated_count,
            "agreement_pct":      pct,
        }


# ─────────────────────────────────────────────────────────────────────────────

class AdjudicationRepository:
    """
    Stores and retrieves final adjudicated decisions.
    Separate from ScreeningRepository to keep the audit trail clean.
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
        "box_width":        0.38,
        "box_height":       0.10,
        "font_size":        11,
        "show_unsure_box":  True,
        "custom_labels":    {},
    }

    @staticmethod
    def _is_valid_hex_color(value: str) -> bool:
        import re
        return bool(re.match(r'^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})$', str(value)))

    def get_settings(self, review_id: int) -> Dict:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT settings_json FROM prisma_settings WHERE review_id = ?",
                (review_id,)
            ).fetchone()
        if not row:
            return dict(self.DEFAULT_SETTINGS)
        try:
            stored = json.loads(row["settings_json"])
            merged = dict(self.DEFAULT_SETTINGS)
            for k, v in stored.items():
                if isinstance(v, dict) and k in merged and isinstance(merged[k], dict):
                    merged[k] = {**merged[k], **v}
                else:
                    merged[k] = v
            # Self-heal invalid colours
            for cdk in ("box_colors", "font_colors"):
                for k, v in merged.get(cdk, {}).items():
                    if not self._is_valid_hex_color(v):
                        merged[cdk][k] = self.DEFAULT_SETTINGS[cdk][k]
            return merged
        except (json.JSONDecodeError, TypeError):
            return dict(self.DEFAULT_SETTINGS)

    def save_settings(self, review_id: int, settings: Dict) -> None:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO prisma_settings (review_id, settings_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(review_id) DO UPDATE SET
                    settings_json = excluded.settings_json,
                    updated_at    = CURRENT_TIMESTAMP
            """, (review_id, json.dumps(settings)))

    def reset_settings(self, review_id: int) -> None:
        with get_connection() as conn:
            conn.execute(
                "DELETE FROM prisma_settings WHERE review_id = ?", (review_id,)
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