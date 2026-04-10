# storage/migrations.py
# Add Migration 005: full_texts table for PMC full-text storage.
# Full text is stored in a separate table (not in articles) because:
#   - It can be megabytes per article
#   - Most queries only need abstract
#   - Keeps the articles table fast for screening/filtering

import logging
from storage.database import get_connection

logger = logging.getLogger(__name__)


def run_migrations() -> None:
    _migration_001_add_article_source_columns()
    _migration_002_ensure_doi_column()
    _migration_003_add_adjudication_table()
    _migration_004_add_prisma_settings_table()
    _migration_005_add_full_texts_table()
    _migration_006_add_analysis_json_column()


def _migration_001_add_article_source_columns() -> None:
    new_columns = [
        ("source",         "TEXT DEFAULT 'pubmed'"),
        ("domain",         "TEXT DEFAULT 'medical'"),
        ("url",            "TEXT"),
        ("venue",          "TEXT"),
        ("citation_count", "INTEGER"),
    ]
    _add_columns_if_missing("articles", new_columns, migration="001")


def _migration_002_ensure_doi_column() -> None:
    _add_columns_if_missing("articles", [("doi", "TEXT")], migration="002")


def _migration_003_add_adjudication_table() -> None:
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS adjudications (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id       INTEGER NOT NULL,
                pmid            TEXT NOT NULL,
                stage           TEXT NOT NULL DEFAULT 'title_abstract',
                conflict_type   TEXT,
                final_decision  TEXT NOT NULL,
                adjudicator_id  TEXT NOT NULL,
                notes           TEXT,
                adjudicated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                FOREIGN KEY (pmid)      REFERENCES articles(pmid),
                UNIQUE (review_id, pmid, stage)
            )
        """)
        logger.info("Migration 003: adjudications table ready")


def _migration_004_add_prisma_settings_table() -> None:
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prisma_settings (
                review_id     INTEGER PRIMARY KEY,
                settings_json TEXT NOT NULL DEFAULT '{}',
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id)
            )
        """)
        logger.info("Migration 004: prisma_settings table ready")


def _migration_005_add_full_texts_table() -> None:
    """
    Migration 005: Separate table for PMC full-text content.

    Design rationale:
    - Full-text articles can be 50-500 KB each
    - Storing in articles table would make every SELECT * expensive
    - Separate table means screening/AI tabs load fast (abstract only)
    - Full text is fetched on demand when needed for deep analysis
    - has_full_text column on articles avoids a JOIN just to check availability
    """
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS full_texts (
                pmid        TEXT PRIMARY KEY,
                full_text   TEXT NOT NULL,
                word_count  INTEGER,
                fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (pmid) REFERENCES articles(pmid)
            )
        """)
        logger.info("Migration 005: full_texts table ready")

    # Add a lightweight flag to articles so UI can show "Full text available"
    # without loading the full text itself
    _add_columns_if_missing(
        "articles",
        [("has_full_text", "INTEGER DEFAULT 0")],
        migration="005"
    )


def _add_columns_if_missing(table: str, columns: list, migration: str = "?") -> None:
    with get_connection() as conn:
        existing = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for col_name, col_def in columns:
            if col_name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
                logger.info("Migration %s: added column %s.%s", migration, table, col_name)
            else:
                logger.debug("Migration %s: %s.%s already exists", migration, table, col_name)


def _migration_006_add_analysis_json_column() -> None:
    _add_columns_if_missing(
        "articles",
        [("analysis_json", "TEXT DEFAULT NULL")],
        migration="006"
    )