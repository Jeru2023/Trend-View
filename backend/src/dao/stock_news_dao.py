"""DAO for storing stock-specific news articles fetched from AkShare."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "stock_news_schema.sql"


class StockNewsDAO(PostgresDAOBase):
    """Persistence helper for AkShare stock news rows."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "stock_news_table", "stock_news")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            unique_idx=f"{self._table_name}_url_uniq_idx",
            stock_idx=f"{self._table_name}_stock_idx",
        )
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN IF NOT EXISTS normalized_url TEXT"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )
            cur.execute(
                sql.SQL("DROP INDEX IF EXISTS {schema}.{index}").format(
                    schema=sql.Identifier(self.config.schema),
                    index=sql.Identifier(f"{self._table_name}_url_uniq_idx"),
                )
            )
            cur.execute(
                sql.SQL(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {index} "
                    "ON {schema}.{table} (stock_code, COALESCE(normalized_url, ''), COALESCE(title, ''))"
                ).format(
                    index=sql.Identifier(f"{self._table_name}_url_uniq_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert_many(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            return 0
        columns = [
            "stock_code",
            "keyword",
            "title",
            "content",
            "source",
            "url",
            "normalized_url",
            "published_at",
            "raw_payload",
        ]
        values = []
        for record in records:
            values.append(
                [
                    record.get("stock_code"),
                    record.get("keyword"),
                    record.get("title"),
                    record.get("content"),
                    record.get("source"),
                    record.get("url"),
                    record.get("normalized_url"),
                    record.get("published_at"),
                    Json(record.get("raw_payload") or {}),
                ]
            )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                insert_query = sql.SQL(
                    """
                    INSERT INTO {schema}.{table} (
                        stock_code,
                        keyword,
                        title,
                        content,
                        source,
                        url,
                        normalized_url,
                        published_at,
                        raw_payload
                    ) VALUES %s
                    ON CONFLICT (stock_code, COALESCE(normalized_url, ''), COALESCE(title, ''))
                    DO UPDATE SET
                        keyword = EXCLUDED.keyword,
                        content = EXCLUDED.content,
                        source = EXCLUDED.source,
                        published_at = EXCLUDED.published_at,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = CURRENT_TIMESTAMP
                    """
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
                execute_values(cur, insert_query.as_string(conn), values)
            conn.commit()
        return len(records)

    def list_recent(self, stock_code: str, *, limit: int = 100) -> List[Dict[str, Any]]:
        if not stock_code:
            return []
        limit = max(1, min(int(limit), 200))
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT
                            id,
                            stock_code,
                            keyword,
                            title,
                            content,
                            source,
                            url,
                            published_at,
                            created_at,
                            updated_at
                        FROM {schema}.{table}
                        WHERE stock_code = %s
                        ORDER BY published_at DESC NULLS LAST, id DESC
                        LIMIT %s
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code, limit),
                )
                rows = cur.fetchall()
        columns = [
            "id",
            "stock_code",
            "keyword",
            "title",
            "content",
            "source",
            "url",
            "published_at",
            "created_at",
            "updated_at",
        ]
        return [{column: value for column, value in zip(columns, row)} for row in rows]

    def latest_published_at(self, stock_code: str) -> Optional[datetime]:
        if not stock_code:
            return None
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT MAX(published_at)
                        FROM {schema}.{table}
                        WHERE stock_code = %s
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (stock_code,),
                )
                row = cur.fetchone()
        return row[0] if row else None

    def list_since(
        self,
        stock_code: str,
        *,
        since: Optional[datetime],
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        if not stock_code:
            return []
        limit = max(1, min(int(limit), 200))
        params: List[object] = [stock_code]
        where_clause = "stock_code = %s"
        if since:
            where_clause += " AND published_at >= %s"
            params.append(since)
        params.extend([limit])
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        f"""
                        SELECT
                            id,
                            stock_code,
                            keyword,
                            title,
                            content,
                            source,
                            url,
                            published_at,
                            created_at,
                            updated_at
                        FROM {{schema}}.{{table}}
                        WHERE {where_clause}
                        ORDER BY published_at DESC NULLS LAST, id DESC
                        LIMIT %s
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    params,
                )
                rows = cur.fetchall()
        columns = [
            "id",
            "stock_code",
            "keyword",
            "title",
            "content",
            "source",
            "url",
            "published_at",
            "created_at",
            "updated_at",
        ]
        return [{column: value for column, value in zip(columns, row)} for row in rows]


__all__ = ["StockNewsDAO"]
