"""
Data access object for finance breakfast summaries fetched via AkShare.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "finance_breakfast_schema.sql"

FINANCE_BREAKFAST_FIELDS: Sequence[str] = (
    "title",
    "summary",
    "published_at",
    "url",
)


class FinanceBreakfastDAO(PostgresDAOBase):
    """Handles persistence for finance breakfast summaries."""

    _conflict_keys: Sequence[str] = ("title", "published_at")

    def __init__(self, config: PostgresSettings, table_name: str | None = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "finance_breakfast_table", "finance_breakfast")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=FINANCE_BREAKFAST_FIELDS,
                conflict_keys=self._conflict_keys,
                date_columns=("published_at",),
            )
        return affected

    def stats(self) -> dict[str, Optional[datetime]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*), MAX(updated_at) FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                count, last_updated = cur.fetchone()
        return {"count": count or 0, "updated_at": last_updated}

    def list_recent(self, *, limit: int = 100) -> list[dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT title, summary, published_at, url FROM {schema}.{table} "
                        "ORDER BY published_at DESC NULLS LAST, title ASC "
                        "LIMIT %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (limit,),
                )
                rows = cur.fetchall()

        results: list[dict[str, object]] = []
        for title, summary, published_at, url in rows:
            results.append(
                {
                    "title": title,
                    "summary": summary,
                    "published_at": published_at,
                    "url": url,
                }
            )
        return results


__all__ = [
    "FINANCE_BREAKFAST_FIELDS",
    "FinanceBreakfastDAO",
]
