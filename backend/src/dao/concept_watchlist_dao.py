"""DAO for the concept watchlist."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "concept_watchlist_schema.sql"


class ConceptWatchlistDAO(PostgresDAOBase):
    """Persist and query monitored concepts."""

    def __init__(self, config: PostgresSettings) -> None:
        super().__init__(config=config)
        self._table = getattr(config, "concept_watchlist_table", "concept_watchlist")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def _qualified_table(self) -> sql.Composed:
        return sql.SQL("{schema}.{table}").format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table),
        )

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table,
            index_updated=f"{self._table}_updated_at_idx",
        )

    def list_entries(self) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT concept_name,
                               concept_code,
                               last_synced_at,
                               created_at,
                               updated_at,
                               is_watched
                        FROM {table}
                        ORDER BY is_watched DESC, updated_at DESC
                        """
                    ).format(table=self._qualified_table())
                )
                rows = cur.fetchall()
        return [
            {
                "concept_name": row[0],
                "concept_code": row[1],
                "last_synced_at": row[2],
                "created_at": row[3],
                "updated_at": row[4],
                "is_watched": row[5],
            }
            for row in rows
        ]

    def get_entry(self, concept_name: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT concept_name,
                               concept_code,
                               last_synced_at,
                               created_at,
                               updated_at,
                               is_watched
                        FROM {table}
                        WHERE concept_name = %s
                        """
                    ).format(table=self._qualified_table()),
                    (concept_name,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "concept_name": row[0],
            "concept_code": row[1],
            "last_synced_at": row[2],
            "created_at": row[3],
            "updated_at": row[4],
            "is_watched": row[5],
        }

    def upsert(
        self,
        concept_name: str,
        concept_code: str,
        *,
        last_synced_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {table} (concept_name, concept_code, last_synced_at, is_watched, created_at, updated_at)
                        VALUES (%s, %s, %s, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (concept_name) DO UPDATE
                        SET concept_code = EXCLUDED.concept_code,
                            last_synced_at = COALESCE(EXCLUDED.last_synced_at, {table}.last_synced_at),
                            is_watched = TRUE,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING concept_name, concept_code, last_synced_at, created_at, updated_at, is_watched
                        """
                    ).format(table=self._qualified_table()),
                    (concept_name, concept_code, last_synced_at),
                )
                row = cur.fetchone()
        return {
            "concept_name": row[0],
            "concept_code": row[1],
            "last_synced_at": row[2],
            "created_at": row[3],
            "updated_at": row[4],
            "is_watched": row[5],
        }

    def update_last_synced(self, concept_name: str, timestamp: datetime) -> None:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        UPDATE {table}
                        SET last_synced_at = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE concept_name = %s
                        """
                    ).format(table=self._qualified_table()),
                    (timestamp, concept_name),
                )
            conn.commit()

    def remove(self, concept_name: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        UPDATE {table}
                        SET is_watched = FALSE,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE concept_name = %s
                        RETURNING concept_name, concept_code, last_synced_at, created_at, updated_at, is_watched
                        """
                    ).format(table=self._qualified_table()),
                    (concept_name,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "concept_name": row[0],
            "concept_code": row[1],
            "last_synced_at": row[2],
            "created_at": row[3],
            "updated_at": row[4],
            "is_watched": row[5],
        }

    def delete_entry(self, concept_name: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        DELETE FROM {table}
                        WHERE concept_name = %s
                        RETURNING concept_name, concept_code, last_synced_at, created_at, updated_at, is_watched
                        """
                    ).format(table=self._qualified_table()),
                    (concept_name,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "concept_name": row[0],
            "concept_code": row[1],
            "last_synced_at": row[2],
            "created_at": row[3],
            "updated_at": row[4],
            "is_watched": row[5],
        }

    def set_watch_state(self, concept_name: str, watch: bool) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        UPDATE {table}
                        SET is_watched = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE concept_name = %s
                        RETURNING concept_name, concept_code, last_synced_at, created_at, updated_at, is_watched
                        """
                    ).format(table=self._qualified_table()),
                    (watch, concept_name),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "concept_name": row[0],
            "concept_code": row[1],
            "last_synced_at": row[2],
            "created_at": row[3],
            "updated_at": row[4],
            "is_watched": row[5],
        }


__all__ = ["ConceptWatchlistDAO"]
