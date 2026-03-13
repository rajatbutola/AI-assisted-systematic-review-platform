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
    conn.row_factory = sqlite3.Row  # Access columns by name
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")  # Better concurrent read performance
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_database() -> None:
    """Create all tables if they don't exist. Safe to call on every startup."""
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT NOT NULL,
                description TEXT,
                pico_json   TEXT,
                status      TEXT DEFAULT 'active',
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS searches (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id   INTEGER NOT NULL,
                database    TEXT NOT NULL DEFAULT 'pubmed',
                query       TEXT NOT NULL,
                n_results   INTEGER,
                searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id)
            );

            CREATE TABLE IF NOT EXISTS articles (
                pmid        TEXT PRIMARY KEY,
                title       TEXT,
                abstract    TEXT,
                authors     TEXT,   -- JSON array
                journal     TEXT,
                year        TEXT,
                doi         TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS review_articles (
                review_id   INTEGER NOT NULL,
                pmid        TEXT NOT NULL,
                search_id   INTEGER,
                is_duplicate INTEGER DEFAULT 0,
                PRIMARY KEY (review_id, pmid),
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                FOREIGN KEY (pmid) REFERENCES articles(pmid)
            );

            CREATE TABLE IF NOT EXISTS screening_decisions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id   INTEGER NOT NULL,
                pmid        TEXT NOT NULL,
                stage       TEXT NOT NULL,  -- 'title_abstract' | 'full_text'
                decision    TEXT NOT NULL,  -- 'include' | 'exclude' | 'unsure'
                reason      TEXT,
                reviewer_id TEXT DEFAULT 'user_1',
                decided_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                FOREIGN KEY (pmid) REFERENCES articles(pmid),
                UNIQUE (review_id, pmid, stage, reviewer_id)
            );

            CREATE TABLE IF NOT EXISTS ai_analyses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pmid        TEXT NOT NULL,
                task        TEXT NOT NULL,  -- 'summary' | 'pico' | 'score'
                output      TEXT,           -- JSON string
                model_name  TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (pmid, task)
            );

            CREATE TABLE IF NOT EXISTS data_extractions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id   INTEGER NOT NULL,
                pmid        TEXT NOT NULL,
                field_name  TEXT NOT NULL,
                field_value TEXT,
                extracted_by TEXT DEFAULT 'user_1',
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                UNIQUE (review_id, pmid, field_name)
            );
        """)