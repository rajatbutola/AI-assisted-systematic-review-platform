# storage/migrations.py
# Add Migration 005: full_texts table for PMC full-text storage.
# Full text is stored in a separate table (not in articles) because:
#   - It can be megabytes per article
#   - Most queries only need abstract
#   - Keeps the articles table fast for screening/filtering



# storage/migrations.py
#
# Central migration runner.  Called once at app startup via run_migrations().
# Each migration function is idempotent — safe to call on every startup.
#
# Migration history:
#   001  Add source/domain/url/venue/citation_count to articles
#   002  Ensure doi column on articles
#   003  Add adjudications table
#   004  Add prisma_settings table
#   005  Add full_texts table + has_full_text flag on articles
#   006  Add analysis_json column to articles
#   007  Schema hardening:
#          A — schema_meta table + composite PK documentation
#          B — performance indexes (review_articles, screening_decisions, etc.)
#          C — created_at on review_articles
#          D — screening_decision_history append-only audit table

import logging
from storage.database import get_connection
from storage.migrations_007 import _migration_007_schema_hardening

logger = logging.getLogger(__name__)


def run_migrations() -> None:
    _migration_001_add_article_source_columns()
    _migration_002_ensure_doi_column()
    _migration_003_add_adjudication_table()
    _migration_004_add_prisma_settings_table()
    _migration_005_add_full_texts_table()
    _migration_006_add_analysis_json_column()
    _migration_007_schema_hardening()          # ← new


# ── 001 ───────────────────────────────────────────────────────────────────────

def _migration_001_add_article_source_columns() -> None:
    new_columns = [
        ("source",         "TEXT DEFAULT 'pubmed'"),
        ("domain",         "TEXT DEFAULT 'medical'"),
        ("url",            "TEXT"),
        ("venue",          "TEXT"),
        ("citation_count", "INTEGER"),
    ]
    _add_columns_if_missing("articles", new_columns, migration="001")


# ── 002 ───────────────────────────────────────────────────────────────────────

def _migration_002_ensure_doi_column() -> None:
    _add_columns_if_missing("articles", [("doi", "TEXT")], migration="002")


# ── 003 ───────────────────────────────────────────────────────────────────────

def _migration_003_add_adjudication_table() -> None:
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS adjudications (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id        INTEGER NOT NULL,
                pmid             TEXT    NOT NULL,
                stage            TEXT    NOT NULL DEFAULT 'title_abstract',
                conflict_type    TEXT,
                final_decision   TEXT    NOT NULL,
                adjudicator_id   TEXT    NOT NULL,
                notes            TEXT,
                adjudicated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                FOREIGN KEY (pmid)      REFERENCES articles(pmid),
                UNIQUE (review_id, pmid, stage)
            )
        """)
    logger.info("Migration 003: adjudications table ready.")


# ── 004 ───────────────────────────────────────────────────────────────────────

def _migration_004_add_prisma_settings_table() -> None:
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prisma_settings (
                review_id     INTEGER PRIMARY KEY,
                settings_json TEXT    NOT NULL DEFAULT '{}',
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id)
            )
        """)
    logger.info("Migration 004: prisma_settings table ready.")


# ── 005 ───────────────────────────────────────────────────────────────────────

def _migration_005_add_full_texts_table() -> None:
    """
    Separate table for PMC full-text content.
    Full-text articles can be 50-500 KB each. Keeping them out of the
    articles table means every SELECT on articles stays fast.
    """
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS full_texts (
                pmid       TEXT PRIMARY KEY,
                full_text  TEXT NOT NULL,
                word_count INTEGER,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pmid) REFERENCES articles(pmid)
            )
        """)
    logger.info("Migration 005: full_texts table ready.")
    _add_columns_if_missing(
        "articles",
        [("has_full_text", "INTEGER DEFAULT 0")],
        migration="005"
    )


# ── 006 ───────────────────────────────────────────────────────────────────────

def _migration_006_add_analysis_json_column() -> None:
    """Add analysis_json column to articles for caching AI results."""
    _add_columns_if_missing(
        "articles",
        [("analysis_json", "TEXT DEFAULT NULL")],
        migration="006"
    )


# ── Shared helper ──────────────────────────────────────────────────────────────

def _add_columns_if_missing(table: str, columns: list,
                             migration: str = "?") -> None:
    with get_connection() as conn:
        existing = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for col_name, col_def in columns:
            if col_name not in existing:
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}"
                )
                logger.info(
                    "Migration %s: added column %s.%s",
                    migration, table, col_name
                )
            else:
                logger.debug(
                    "Migration %s: %s.%s already exists",
                    migration, table, col_name
                )






