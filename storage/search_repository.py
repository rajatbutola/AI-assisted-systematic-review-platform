from storage.database import get_connection


class SearchRepository:

    def create_search(self, review_id: int, query: str, n_results: int):
        with get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO searches (review_id, query, n_results)
                VALUES (?, ?, ?)
                """,
                (review_id, query, n_results)
            )
            return cursor.lastrowid