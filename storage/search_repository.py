# storage/search_repository.py 27th March, 2026




# storage/search_repository.py — v2 31st March
#
# CHANGE: Added source_name column to searches table.
# This enables the PRISMA panel to SUM n_results PER SOURCE across
# all searches under a review (instead of taking the latest value which
# overwrote the previous one).
#
# Migration: The column is added lazily on first use if missing,
# so existing databases are not broken.




# storage/search_repository.py — v3 1st April
#
# CRITICAL FIXES:
#   1. The searches table uses "searched_at" NOT "created_at" — the old
#      COALESCE(created_at, '') caused a SQL error that was silently caught,
#      making _get_prisma_sources_from_db return {} every time. This meant
#      prisma_db_{review_id} was always empty → single identification box.
#   2. The searches table has a "database" column (the original schema) that
#      holds the source name for older records. The new "source_name" column
#      is added by lazy migration. We read COALESCE(source_name, database)
#      so both old and new records work correctly.

from storage.database import get_connection


class SearchRepository:

    def _ensure_source_name_column(self):
        """Add source_name column if not present (lazy migration)."""
        with get_connection() as conn:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(searches)")]
            if "source_name" not in cols:
                conn.execute(
                    "ALTER TABLE searches ADD COLUMN source_name TEXT"
                )

    def create_search(self, review_id: int, query: str, n_results: int,
                      source_name: str = "PubMed") -> int:
        self._ensure_source_name_column()
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO searches (review_id, query, n_results, source_name)
                VALUES (?, ?, ?, ?)
                """,
                (review_id, query, n_results, source_name),
            )
            return cursor.lastrowid

    def list_searches_for_review(self, review_id: int) -> list:
        """
        Return all searches for a review.
        Uses COALESCE(source_name, database, 'PubMed') to handle both old
        records (which stored the source in the 'database' column) and new
        records (which use source_name). Uses 'searched_at' which is the
        actual column name in the schema (not 'created_at').
        """
        self._ensure_source_name_column()
        with get_connection() as conn:
            # Check whether the 'database' column exists (original schema)
            cols = [row[1] for row in conn.execute("PRAGMA table_info(searches)")]
            has_database_col = "database" in cols
            has_searched_at  = "searched_at" in cols

            src_expr = (
                "COALESCE(source_name, \"database\", 'PubMed')"
                if has_database_col
                else "COALESCE(source_name, 'PubMed')"
            )
            time_col = "searched_at" if has_searched_at else "NULL"

            rows = conn.execute(
                f"""
                SELECT id, review_id, query, n_results,
                       {src_expr} as source_name,
                       {time_col} as searched_at
                FROM searches
                WHERE review_id = ?
                ORDER BY id ASC
                """,
                (review_id,),
            ).fetchall()
            return [
                {
                    "id":          row[0],
                    "review_id":   row[1],
                    "query":       row[2],
                    "n_results":   row[3] or 0,
                    "source_name": row[4] or "PubMed",
                    "searched_at": row[5],
                }
                for row in rows
            ]

    def get_source_totals(self, review_id: int) -> dict:
        """Return {source_name: unique_article_count} from actual DB articles.
        Counts distinct articles per source joined to review_articles,
        so repeated searches and cross-search duplicates are not double-counted.
        Falls back to summing n_results if no articles are in DB yet.
        """
        from storage.database import get_connection
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT a.source, COUNT(DISTINCT a.pmid) AS n
                FROM articles a
                JOIN review_articles ra ON a.pmid = ra.pmid
                WHERE ra.review_id = ?
                GROUP BY a.source
            """, (review_id,)).fetchall()

        if rows:
            src_map = {
                "pubmed":       "PubMed",
                "europe_pmc":   "Europe PMC",
                "core":         "CORE",
                "semantic_scholar": "Semantic Scholar",
                "openalex":     "OpenAlex",
            }
            totals: dict = {}
            for row in rows:
                src_raw  = (row[0] or "pubmed").lower()
                src_name = src_map.get(src_raw, src_raw.upper())
                totals[src_name] = int(row[1])
            return totals

        # Fallback: no articles saved yet — sum raw n_results
        searches = self.list_searches_for_review(review_id)
        totals = {}
        for s in searches:
            src = s.get("source_name") or "PubMed"
            totals[src] = totals.get(src, 0) + int(s.get("n_results") or 0)
        return totals