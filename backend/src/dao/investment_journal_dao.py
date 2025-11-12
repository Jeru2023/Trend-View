"""
DAO for storing daily investment journal entries.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "investment_journal_schema.sql"


class InvestmentJournalDAO(PostgresDAOBase):
    """Persistence helper for investment journal entries keyed by entry_date."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "investment_journal_table", "investment_journal")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )

    def upsert_entry(self, entry_date: date, review_html: Optional[str], plan_html: Optional[str]) -> Dict[str, object]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.{table} (entry_date, review_html, plan_html)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (entry_date)
                        DO UPDATE
                        SET review_html = EXCLUDED.review_html,
                            plan_html = EXCLUDED.plan_html,
                            updated_at = NOW()
                        RETURNING entry_date, review_html, plan_html, created_at, updated_at
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (entry_date, review_html, plan_html),
                )
                row = cur.fetchone()
        return self._row_to_entry(row) if row else {}

    def get_entry(self, entry_date: date) -> Optional[Dict[str, object]]:
        query = sql.SQL(
            """
            SELECT entry_date, review_html, plan_html, created_at, updated_at
            FROM {schema}.{table}
            WHERE entry_date = %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (entry_date,))
                row = cur.fetchone()
        return self._row_to_entry(row) if row else None

    def list_entries(self, start_date: date, end_date: date) -> List[Dict[str, object]]:
        query = sql.SQL(
            """
            SELECT entry_date, review_html, plan_html, created_at, updated_at
            FROM {schema}.{table}
            WHERE entry_date BETWEEN %s AND %s
            ORDER BY entry_date ASC
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (start_date, end_date))
                rows = cur.fetchall()
        return [self._row_to_entry(row) for row in rows]

    @staticmethod
    def _row_to_entry(row: Optional[tuple]) -> Optional[Dict[str, object]]:
        if not row:
            return None
        entry_date, review_html, plan_html, created_at, updated_at = row
        return {
            "entry_date": entry_date,
            "review_html": review_html,
            "plan_html": plan_html,
            "created_at": created_at,
            "updated_at": updated_at,
        }


__all__ = ["InvestmentJournalDAO"]
