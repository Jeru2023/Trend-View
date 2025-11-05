"""DAO for unified news articles storage."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "news_articles_schema.sql"

NEWS_ARTICLE_FIELDS: tuple[str, ...] = (
    "article_id",
    "source",
    "source_item_id",
    "title",
    "summary",
    "content",
    "content_type",
    "published_at",
    "url",
    "language",
    "content_fetched",
    "content_fetched_at",
    "processing_status",
    "relevance_attempts",
    "impact_attempts",
    "last_error",
    "raw_payload",
)

DATE_COLUMNS: tuple[str, ...] = ("published_at", "content_fetched_at")


class NewsArticleDAO(PostgresDAOBase):
    """Persistence helper for unified news articles."""

    _relevance_status = "pending"
    _relevance_in_progress_status = "relevance_in_progress"
    _impact_ready_status = "ready_for_impact"
    _impact_in_progress_status = "impact_in_progress"
    _completed_status = "completed"
    _error_status = "error"

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "news_articles_table", "news_articles")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            table_source_item_idx=f"{self._table_name}_source_item_idx",
            table_status_idx=f"{self._table_name}_status_idx",
        )

    def upsert(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        with self.connect() as conn:
            self.ensure_table(conn)
            available_columns = [column for column in NEWS_ARTICLE_FIELDS if column in dataframe.columns]
            affected = self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe.loc[:, available_columns],
                columns=available_columns,
                conflict_keys=("article_id",),
                date_columns=DATE_COLUMNS,
            )
        return affected

    def acquire_for_relevance(self, *, limit: int = 20) -> List[Dict[str, object]]:
        return self._acquire_for_processing(
            current_status=self._relevance_status,
            next_status=self._relevance_in_progress_status,
            attempt_column="relevance_attempts",
            limit=limit,
        )

    def acquire_for_impact(self, *, limit: int = 20) -> List[Dict[str, object]]:
        return self._acquire_for_processing(
            current_status=self._impact_ready_status,
            next_status=self._impact_in_progress_status,
            attempt_column="impact_attempts",
            limit=limit,
        )

    def latest_published_at(self, source: str) -> Optional[datetime]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT MAX(published_at) FROM {schema}.{table} WHERE source = %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (source,),
                )
                row = cur.fetchone()
        if not row:
            return None
        latest = row[0]
        if isinstance(latest, datetime):
            return latest
        return None

    def list_missing_content(self, source: str, *, limit: int = 100) -> List[Dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT article_id, title, summary, url, published_at "
                        "FROM {schema}.{table} "
                        "WHERE source = %s AND (content_fetched IS DISTINCT FROM TRUE OR content IS NULL) "
                        "ORDER BY published_at DESC "
                        "LIMIT %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (source, limit),
                )
                rows = cur.fetchall()
        columns = ["article_id", "title", "summary", "url", "published_at"]
        return [{column: value for column, value in zip(columns, row)} for row in rows]

    def existing_article_ids(self, article_ids: Iterable[str]) -> set[str]:
        ids = [article_id for article_id in article_ids if article_id]
        if not ids:
            return set()
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT article_id FROM {schema}.{table} WHERE article_id = ANY(%s)"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (ids,),
                )
                rows = cur.fetchall()
        return {row[0] for row in rows}

    def existing_source_items(self, source: str, source_item_ids: Iterable[str]) -> set[str]:
        items = [item for item in source_item_ids if item]
        if not items:
            return set()
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COALESCE(source_item_id, url) "
                        "FROM {schema}.{table} "
                        "WHERE source = %s AND COALESCE(source_item_id, url) = ANY(%s)"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (source, items),
                )
                rows = cur.fetchall()
        return {row[0] for row in rows}

    def list_articles(
        self,
        *,
        source: Optional[str] = None,
        processing_status: Optional[Sequence[str]] = None,
        limit: int = 100,
    ) -> List[Dict[str, object]]:
        limit_value = max(1, min(int(limit), 500))
        conditions: List[sql.Composed] = []
        params: List[object] = []
        if source:
            conditions.append(sql.SQL("source = %s"))
            params.append(source)
        if processing_status:
            conditions.append(sql.SQL("processing_status = ANY(%s)"))
            params.append(list(processing_status))

        where_clause = sql.SQL("")
        if conditions:
            where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT article_id, source, source_item_id, title, summary, content, content_type, "
                        "published_at, url, language, content_fetched, content_fetched_at, processing_status, "
                        "relevance_attempts, impact_attempts, last_error "
                        "FROM {schema}.{table} "
                        "{where_clause} "
                        "ORDER BY published_at DESC, article_id DESC "
                        "LIMIT %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                        where_clause=where_clause,
                    ),
                    (*params, limit_value),
                )
                rows = cur.fetchall()

        columns = [
            "article_id",
            "source",
            "source_item_id",
            "title",
            "summary",
            "content",
            "content_type",
            "published_at",
            "url",
            "language",
            "content_fetched",
            "content_fetched_at",
            "processing_status",
            "relevance_attempts",
            "impact_attempts",
            "last_error",
        ]
        return [{column: value for column, value in zip(columns, row)} for row in rows]

    def stats(self, source: Optional[str] = None) -> Dict[str, object]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                if source:
                    cur.execute(
                        sql.SQL(
                            "SELECT COUNT(*) AS total, MAX(published_at) AS latest_published, "
                            "MAX(updated_at) AS latest_update "
                            "FROM {schema}.{table} WHERE source = %s"
                        ).format(
                            schema=sql.Identifier(self.config.schema),
                            table=sql.Identifier(self._table_name),
                        ),
                        (source,),
                    )
                else:
                    cur.execute(
                        sql.SQL(
                            "SELECT COUNT(*) AS total, MAX(published_at) AS latest_published, "
                            "MAX(updated_at) AS latest_update "
                            "FROM {schema}.{table}"
                        ).format(
                            schema=sql.Identifier(self.config.schema),
                            table=sql.Identifier(self._table_name),
                        )
                    )
                total, latest_published, latest_update = cur.fetchone()
        count = int(total or 0)
        return {
            "total": count,
            "count": count,
            "latest_published": latest_published,
            "published_at": latest_published,
            "latest_update": latest_update,
            "updated_at": latest_update,
        }

    def update_status(
        self,
        *,
        article_ids: Iterable[str],
        status: str,
        last_error: Optional[str] = None,
        content_fetched: Optional[bool] = None,
    ) -> None:
        ids = [article_id for article_id in article_ids]
        if not ids:
            return

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                assignments = [sql.SQL("processing_status = %s"), sql.SQL("updated_at = CURRENT_TIMESTAMP")]
                params: List[object] = [status]
                if last_error is not None:
                    assignments.append(sql.SQL("last_error = %s"))
                    params.append(last_error)
                if content_fetched is not None:
                    assignments.append(sql.SQL("content_fetched = %s"))
                    params.append(content_fetched)

                query = sql.SQL(
                    "UPDATE {schema}.{table} SET {assignments} WHERE article_id = ANY(%s)"
                ).format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                    assignments=sql.SQL(", ").join(assignments),
                )
                cur.execute(query, (*params, ids))

    def mark_content_fetched(self, article_id: str, *, fetched_at: datetime, content_available: bool) -> None:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "UPDATE {schema}.{table} SET content_fetched = %s, content_fetched_at = %s, updated_at = CURRENT_TIMESTAMP WHERE article_id = %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (content_available, fetched_at, article_id),
                )

    def _acquire_for_processing(
        self,
        *,
        current_status: str,
        next_status: str,
        attempt_column: str,
        limit: int,
    ) -> List[Dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT article_id, source, source_item_id, title, summary, content, content_type, published_at, url, language, raw_payload "
                        "FROM {schema}.{table} "
                        "WHERE processing_status = %s "
                        "ORDER BY published_at DESC, article_id DESC "
                        "LIMIT %s "
                        "FOR UPDATE SKIP LOCKED"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (current_status, limit),
                )
                rows = cur.fetchall()
                if not rows:
                    return []
                article_ids = [row[0] for row in rows]

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "UPDATE {schema}.{table} "
                        "SET processing_status = %s, {attempt} = {attempt} + 1, updated_at = CURRENT_TIMESTAMP "
                        "WHERE article_id = ANY(%s)"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                        attempt=sql.Identifier(attempt_column),
                    ),
                    (next_status, article_ids),
                )

            columns = [
                "article_id",
                "source",
                "source_item_id",
                "title",
                "summary",
                "content",
                "content_type",
                "published_at",
                "url",
                "language",
                "raw_payload",
            ]
            results: List[Dict[str, object]] = []
            for row in rows:
                record = {column: value for column, value in zip(columns, row)}
                results.append(record)
        return results


__all__ = ["NewsArticleDAO"]
