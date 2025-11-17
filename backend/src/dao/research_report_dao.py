"""DAO for research reports scraped from external sources."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "research_report_schema.sql"

REPORT_COLUMNS: Sequence[str] = (
    "ts_code",
    "symbol",
    "report_id",
    "title",
    "report_type",
    "publish_date",
    "org",
    "analysts",
    "detail_url",
    "content_html",
    "content_text",
)


class ResearchReportDAO(PostgresDAOBase):
    """Persistence helper for research reports."""

    _conflict_keys: Sequence[str] = ("report_id",)
    _date_columns: Sequence[str] = ("publish_date",)

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "research_report_table", "research_reports")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            index_ts_code_date=f"{self._table_name}_ts_code_date_idx",
        )

    def upsert(self, dataframe: pd.DataFrame, *, conn=None) -> int:
        if dataframe is None or dataframe.empty:
            return 0
        normalized = self._normalize_dataframe(dataframe, self._date_columns)
        if conn is None:
            with self.connect() as owned_conn:
                self.ensure_table(owned_conn)
                return self._write_dataframe(owned_conn, normalized)
        self.ensure_table(conn)
        return self._write_dataframe(conn, normalized)

    def _write_dataframe(self, conn, dataframe: pd.DataFrame) -> int:
        return self._upsert_dataframe(
            conn,
            schema=self.config.schema,
            table=self._table_name,
            dataframe=dataframe,
            columns=REPORT_COLUMNS,
            conflict_keys=self._conflict_keys,
            date_columns=self._date_columns,
        )

    def existing_report_ids(self, ts_code: str) -> List[str]:
        query = sql.SQL(
            """
            SELECT report_id
            FROM {schema}.{table}
            WHERE ts_code = %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (ts_code,))
                return [row[0] for row in cur.fetchall()]

    def latest_publish_date(self, ts_code: str) -> Optional[datetime]:
        query = sql.SQL(
            """
            SELECT MAX(publish_date)
            FROM {schema}.{table}
            WHERE ts_code = %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (ts_code,))
                result = cur.fetchone()
                return result[0] if result else None

    def list_reports(
        self,
        *,
        ts_code: str,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        query = sql.SQL(
            """
            SELECT id, ts_code, symbol, report_id, title, report_type, publish_date,
                   org, analysts, detail_url, created_at
            FROM {schema}.{table}
            WHERE ts_code = %s
            ORDER BY publish_date DESC NULLS LAST, id DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        count_query = sql.SQL(
            """
            SELECT COUNT(*)
            FROM {schema}.{table}
            WHERE ts_code = %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(count_query, (ts_code,))
                total = cur.fetchone()[0] or 0
                cur.execute(query, (ts_code, limit, offset))
                rows = cur.fetchall()
        columns = [
            "id",
            "ts_code",
            "symbol",
            "report_id",
            "title",
            "report_type",
            "publish_date",
            "org",
            "analysts",
            "detail_url",
            "created_at",
        ]
        return {"total": int(total), "items": [dict(zip(columns, row)) for row in rows]}

    def fetch_reports_since(
        self,
        *,
        ts_code: str,
        start_date: datetime,
        limit: int = 20,
    ) -> List[dict]:
        query = sql.SQL(
            """
            SELECT title, org, analysts, publish_date, detail_url, content_text
            FROM {schema}.{table}
            WHERE ts_code = %s AND publish_date >= %s AND content_text IS NOT NULL
            ORDER BY publish_date DESC NULLS LAST
            LIMIT %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (ts_code, start_date, limit))
                rows = cur.fetchall()
        columns = [
            "title",
            "org",
            "analysts",
            "publish_date",
            "detail_url",
            "content_text",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def fetch_reports_for_distillation(
        self,
        *,
        ts_code: str,
        start_date: datetime,
        limit: int,
    ) -> List[dict]:
        query = sql.SQL(
            """
            SELECT report_id, title, org, analysts, report_type, publish_date,
                   detail_url, content_text, distillation, distillation_model,
                   distillation_generated_at
            FROM {schema}.{table}
            WHERE ts_code = %s AND publish_date >= %s AND content_text IS NOT NULL
            ORDER BY publish_date DESC NULLS LAST
            LIMIT %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (ts_code, start_date, limit))
                rows = cur.fetchall()
        columns = [
            "report_id",
            "title",
            "org",
            "analysts",
            "report_type",
            "publish_date",
            "detail_url",
            "content_text",
            "distillation",
            "distillation_model",
            "distillation_generated_at",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def list_distilled_reports(
        self,
        *,
        ts_code: str,
        start_date: datetime,
        limit: int,
    ) -> List[dict]:
        query = sql.SQL(
            """
            SELECT report_id, title, org, report_type, publish_date, detail_url,
                   distillation, distillation_model, distillation_generated_at
            FROM {schema}.{table}
            WHERE ts_code = %s AND publish_date >= %s AND distillation IS NOT NULL
            ORDER BY publish_date DESC NULLS LAST
            LIMIT %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (ts_code, start_date, limit))
                rows = cur.fetchall()
        columns = [
            "report_id",
            "title",
            "org",
            "report_type",
            "publish_date",
            "detail_url",
            "distillation",
            "distillation_model",
            "distillation_generated_at",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def save_distillation(
        self,
        *,
        report_id: str,
        payload: dict,
        model: Optional[str] = None,
    ) -> None:
        query = sql.SQL(
            """
            UPDATE {schema}.{table}
            SET distillation = %s::jsonb,
                distillation_model = %s,
                distillation_generated_at = NOW()
            WHERE report_id = %s
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (json.dumps(payload, ensure_ascii=False), model, report_id))


__all__ = ["ResearchReportDAO", "REPORT_COLUMNS"]
