import json
import sqlite3
from datetime import datetime
from typing import List, Optional, Dict, Any
from models.schemas import Article, PICOQuery, ScreeningDecision
from storage.database import get_connection

class ReviewRepository:
    """All database operations for Reviews."""

    def create_review(self, title: str, description: str = "",
                      pico: Optional[PICOQuery] = None) -> int:
        """Create a new review. Returns new review ID."""
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


class ArticleRepository:
    """All database operations for Articles."""

    def save_articles(self, articles: List[Article], review_id: int,
                      search_id: int) -> Dict[str, int]:
        """
        Save articles and link them to a review.
        Returns counts: {'saved': n, 'duplicates': n}
        """
        saved = 0
        duplicates = 0

        with get_connection() as conn:
            for article in articles:
                # Upsert article (may already exist from another review)
                conn.execute("""
                    INSERT INTO articles (pmid, title, abstract, authors, journal, year)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pmid) DO UPDATE SET
                        title = excluded.title,
                        abstract = excluded.abstract
                """, (
                    article.pmid, article.title, article.abstract,
                    json.dumps(article.authors), article.journal, article.year
                ))

                # Link to this review (check for duplicate within review)
                existing = conn.execute(
                    "SELECT 1 FROM review_articles WHERE review_id = ? AND pmid = ?",
                    (review_id, article.pmid)
                ).fetchone()

                if existing:
                    duplicates += 1
                else:
                    conn.execute(
                        """INSERT INTO review_articles (review_id, pmid, search_id)
                           VALUES (?, ?, ?)""",
                        (review_id, article.pmid, search_id)
                    )
                    saved += 1

        return {"saved": saved, "duplicates": duplicates}

    def get_articles_for_review(self, review_id: int,
                                 stage: str = "title_abstract") -> List[Dict]:
        """
        Get articles for a review with their screening status.
        stage: 'title_abstract' or 'full_text'
        """
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT
                    a.pmid, a.title, a.abstract, a.authors, a.journal, a.year,
                    sd.decision, sd.reason, sd.decided_at
                FROM articles a
                JOIN review_articles ra ON a.pmid = ra.pmid
                LEFT JOIN screening_decisions sd
                    ON a.pmid = sd.pmid
                    AND sd.review_id = ?
                    AND sd.stage = ?
                    AND sd.reviewer_id = 'user_1'
                WHERE ra.review_id = ?
                ORDER BY sd.decision NULLS FIRST, a.pmid
            """, (review_id, stage, review_id)).fetchall()

            return [dict(r) for r in rows]

    def get_screening_counts(self, review_id: int) -> Dict[str, int]:
        """Get PRISMA-ready counts for a review."""
        with get_connection() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM review_articles WHERE review_id = ?",
                (review_id,)
            ).fetchone()[0]

            decisions = conn.execute("""
                SELECT decision, COUNT(*) as n
                FROM screening_decisions
                WHERE review_id = ? AND stage = 'title_abstract'
                GROUP BY decision
            """, (review_id,)).fetchall()

            counts = {"total": total, "pending": 0,
                      "included": 0, "excluded": 0, "unsure": 0}
            screened = 0
            for row in decisions:
                counts[row["decision"]] = row["n"]
                screened += row["n"]
            counts["pending"] = total - screened
            return counts


class ScreeningRepository:
    """All database operations for Screening Decisions."""

    def save_decision(self, review_id: int, pmid: str, stage: str,
                      decision: str, reason: str = "",
                      reviewer_id: str = "user_1") -> None:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO screening_decisions
                    (review_id, pmid, stage, decision, reason, reviewer_id)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(review_id, pmid, stage, reviewer_id)
                DO UPDATE SET
                    decision = excluded.decision,
                    reason = excluded.reason,
                    decided_at = CURRENT_TIMESTAMP
            """, (review_id, pmid, stage, decision, reason, reviewer_id))

    def get_decision(self, review_id: int, pmid: str, stage: str,
                     reviewer_id: str = "user_1") -> Optional[str]:
        with get_connection() as conn:
            row = conn.execute("""
                SELECT decision FROM screening_decisions
                WHERE review_id = ? AND pmid = ? AND stage = ? AND reviewer_id = ?
            """, (review_id, pmid, stage, reviewer_id)).fetchone()
            return row["decision"] if row else None


class AIAnalysisRepository:
    """Cache AI outputs so you don't re-run inference on the same abstract."""

    def save_analysis(self, pmid: str, task: str,
                      output: Any, model_name: str = "tinyllama") -> None:
        output_json = json.dumps(output) if not isinstance(output, str) else output
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO ai_analyses (pmid, task, output, model_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(pmid, task) DO UPDATE SET
                    output = excluded.output,
                    model_name = excluded.model_name,
                    created_at = CURRENT_TIMESTAMP
            """, (pmid, task, output_json, model_name))

    def get_analysis(self, pmid: str, task: str) -> Optional[Any]:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT output FROM ai_analyses WHERE pmid = ? AND task = ?",
                (pmid, task)
            ).fetchone()
            if row:
                try:
                    return json.loads(row["output"])
                except (json.JSONDecodeError, TypeError):
                    return row["output"]
            return None

    def has_analysis(self, pmid: str, task: str) -> bool:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM ai_analyses WHERE pmid = ? AND task = ?",
                (pmid, task)
            ).fetchone()
            return row is not None