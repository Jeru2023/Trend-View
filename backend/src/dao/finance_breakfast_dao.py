"""
Data access object for finance breakfast summaries fetched via AkShare.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "finance_breakfast_schema.sql"

FINANCE_BREAKFAST_FIELDS: Sequence[str] = (
    "title",
    "summary",
    "content",
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
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN IF NOT EXISTS content TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN IF NOT EXISTS ai_extract TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN IF NOT EXISTS ai_extract_summary TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN IF NOT EXISTS ai_extract_detail TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            available_columns = [column for column in FINANCE_BREAKFAST_FIELDS if column in dataframe.columns]
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=available_columns,
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
                        "SELECT title, summary, content, ai_extract, ai_extract_summary, ai_extract_detail, published_at, url "
                        "FROM {schema}.{table} "
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
        for title, summary, content, ai_extract, ai_extract_summary, ai_extract_detail, published_at, url in rows:
            results.append(
                {
                    "title": title,
                    "summary": summary,
                    "content": content,
                    "ai_extract": ai_extract,
                    "ai_extract_summary": ai_extract_summary,
                    "ai_extract_detail": ai_extract_detail,
                    "published_at": published_at,
                    "url": url,
                }
            )
        return results

    def latest_published_date(self) -> Optional[datetime]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT MAX(published_at) FROM {schema}.{table}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                (latest,) = cur.fetchone()
        return latest

    def titles_on_date(self, target_date: date) -> set[str]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT title FROM {schema}.{table} "
                        "WHERE DATE(published_at) = %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (target_date,),
                )
                rows = cur.fetchall()
        return {title for (title,) in rows if title}

    def list_missing_content(self, *, limit: int | None = 200) -> list[dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                base_sql = sql.SQL(
                    "SELECT title, summary, published_at, url "
                    "FROM {schema}.{table} "
                    "WHERE content IS NULL "
                    "AND url IS NOT NULL "
                    "AND url <> '' "
                    "ORDER BY published_at DESC NULLS LAST"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )

                if limit is not None and limit > 0:
                    query = sql.SQL("{} LIMIT %s").format(base_sql)
                    cur.execute(query, (limit,))
                else:
                    cur.execute(base_sql)
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

    def update_content(self, entries: Sequence[tuple[str, datetime, str]]) -> int:
        if not entries:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                template = (
                    "UPDATE {schema}.{table} AS t "
                    "SET content = data.content, updated_at = CURRENT_TIMESTAMP "
                    "FROM (VALUES %s) AS data(title, published_at, content) "
                    "WHERE t.title = data.title AND t.published_at = data.published_at"
                )
                update_sql = sql.SQL(template).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
                execute_values(cur, update_sql.as_string(conn), entries)
        return len(entries)

    def fetch_contents(self, keys: Sequence[tuple[str, datetime]]) -> dict[tuple[str, datetime], Optional[str]]:
        if not keys:
            return {}

        unique_keys = list(dict.fromkeys(keys))
        if not unique_keys:
            return {}

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                query = sql.SQL(
                    "SELECT title, published_at, content FROM {schema}.{table} "
                    "WHERE (title, published_at) IN (VALUES %s)"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
                execute_values(cur, query.as_string(conn), unique_keys)
                rows = cur.fetchall()

        return {(title, published_at): content for title, published_at, content in rows}

    def list_missing_ai_extract(self, *, limit: int | None = 30) -> list[dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                base_sql = sql.SQL(
                    "SELECT title, published_at, content "
                    "FROM {schema}.{table} "
                    "WHERE content IS NOT NULL "
                    "AND TRIM(content) <> '' "
                    "AND (ai_extract_summary IS NULL OR ai_extract_detail IS NULL) "
                    "ORDER BY published_at DESC NULLS LAST"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )

                if limit is not None and limit > 0:
                    query = sql.SQL("{} LIMIT %s").format(base_sql)
                    cur.execute(query, (limit,))
                else:
                    cur.execute(base_sql)
                rows = cur.fetchall()

        results: list[dict[str, object]] = []
        for title, published_at, content in rows:
            results.append(
                {
                    "title": title,
                    "published_at": published_at,
                    "content": content,
                }
            )
        return results

    def update_ai_extract(
        self, entries: Sequence[tuple[str, datetime, Optional[str], Optional[str], Optional[str]]]
    ) -> int:
        if not entries:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                template = (
                    "UPDATE {schema}.{table} AS t "
                    "SET ai_extract = data.ai_extract, "
                    "ai_extract_summary = data.ai_extract_summary, "
                    "ai_extract_detail = data.ai_extract_detail, "
                    "updated_at = CURRENT_TIMESTAMP "
                    "FROM (VALUES %s) AS data(title, published_at, ai_extract, ai_extract_summary, ai_extract_detail) "
                    "WHERE t.title = data.title AND t.published_at = data.published_at"
                )
                update_sql = sql.SQL(template).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
                execute_values(cur, update_sql.as_string(conn), entries)
        return len(entries)


__all__ = [
    "FINANCE_BREAKFAST_FIELDS",
    "FinanceBreakfastDAO",
]
