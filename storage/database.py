# storage/database.py

import sqlite3
import json
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

DB_PATH = Path("sr_platform.db")

@contextmanager
def get_connection():
    """Context manager for safe database connections."""
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_database() -> None:
    """
    Create all tables if they don't exist. Safe to call on every startup.

    BUG FIXED:
        Original code called conn.executescript(...) inside get_connection().
        sqlite3.executescript() issues an implicit COMMIT before running, which
        interferes with the context manager's transaction control and can cause
        "cannot commit — no transaction is active" errors or silent rollbacks.

        Fix: use individual conn.execute() calls for each CREATE TABLE statement
        inside a normal transaction, which the context manager commits cleanly.
    """
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT NOT NULL,
                description TEXT,
                pico_json   TEXT,
                status      TEXT DEFAULT 'active',
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS searches (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id   INTEGER NOT NULL,
                database    TEXT NOT NULL DEFAULT 'pubmed',
                query       TEXT NOT NULL,
                n_results   INTEGER,
                searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                pmid        TEXT PRIMARY KEY,
                title       TEXT,
                abstract    TEXT,
                authors     TEXT,
                journal     TEXT,
                year        TEXT,
                doi         TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS review_articles (
                review_id    INTEGER NOT NULL,
                pmid         TEXT NOT NULL,
                search_id    INTEGER,
                is_duplicate INTEGER DEFAULT 0,
                PRIMARY KEY (review_id, pmid),
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                FOREIGN KEY (pmid)      REFERENCES articles(pmid)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS screening_decisions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id   INTEGER NOT NULL,
                pmid        TEXT NOT NULL,
                stage       TEXT NOT NULL,
                decision    TEXT NOT NULL,
                reason      TEXT,
                reviewer_id TEXT DEFAULT 'user_1',
                decided_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                FOREIGN KEY (pmid)      REFERENCES articles(pmid),
                UNIQUE (review_id, pmid, stage, reviewer_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_analyses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pmid        TEXT NOT NULL,
                task        TEXT NOT NULL,
                output      TEXT,
                model_name  TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (pmid, task)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_extractions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id    INTEGER NOT NULL,
                pmid         TEXT NOT NULL,
                field_name   TEXT NOT NULL,
                field_value  TEXT,
                extracted_by TEXT DEFAULT 'user_1',
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                UNIQUE (review_id, pmid, field_name)
            )
        """)

