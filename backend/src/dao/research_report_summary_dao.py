"""DAO for storing aggregated research report summaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "research_report_summary_schema.sql"


class ResearchReportSummaryDAO(PostgresDAOBase):
    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "research_report_summary_table", "research_report_summaries")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )

    def upsert_summary(self, ts_code: str, payload: dict, model: Optional[str] = None) -> None:
        query = sql.SQL(
            """
            INSERT INTO {schema}.{table} (ts_code, summary, model, generated_at, updated_at)
            VALUES (%s, %s::jsonb, %s, NOW(), NOW())
            ON CONFLICT (ts_code)
            DO UPDATE SET
                summary = EXCLUDED.summary,
                model = EXCLUDED.model,
                generated_at = EXCLUDED.generated_at,
                updated_at = NOW()
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query, (ts_code, json.dumps(payload, ensure_ascii=False), model))

    def get_summary(self, ts_code: str) -> Optional[dict]:
        query = sql.SQL(
            """
            SELECT summary, model, generated_at
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
                row = cur.fetchone()
        if not row:
            return None
        summary, model, generated_at = row
        result = summary if isinstance(summary, dict) else summary
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                result = None
        if result is None:
            return None
        result["_meta"] = {
            "model": model,
            "generated_at": generated_at.isoformat() if generated_at else None,
        }
        return result


__all__ = ["ResearchReportSummaryDAO"]
