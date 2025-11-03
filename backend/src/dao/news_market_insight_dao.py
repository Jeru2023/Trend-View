"""DAO for aggregated market insight summaries."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from psycopg2 import sql

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "news_market_insights_schema.sql"


class NewsMarketInsightDAO(PostgresDAOBase):
    """Persistence helper for market insight summaries."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "news_market_insight_table", "news_market_insights")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
            table_generated_idx=f"{self._table_name}_generated_idx",
        )

    def insert_summary(self, payload: Dict[str, object]) -> str:
        summary_id = payload.get("summary_id") or uuid.uuid4().hex
        payload = dict(payload)
        payload["summary_id"] = summary_id

        dataframe = pd.DataFrame([payload])
        with self.connect() as conn:
            self.ensure_table(conn)
            self._upsert_dataframe(
                conn,
                schema=self.config.schema,
                table=self._table_name,
                dataframe=dataframe,
                columns=list(dataframe.columns),
                conflict_keys=("summary_id",),
                date_columns=("generated_at", "window_start", "window_end"),
            )
        return summary_id

    def latest_summary(self) -> Optional[Dict[str, object]]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT summary_id, generated_at, window_start, window_end, headline_count, summary_json, "
                        "raw_response, referenced_articles, prompt_tokens, completion_tokens, total_tokens, elapsed_ms, model_used "
                        "FROM {schema}.{table} ORDER BY generated_at DESC LIMIT 1"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                row = cur.fetchone()
        if not row:
            return None
        keys = [
            "summary_id",
            "generated_at",
            "window_start",
            "window_end",
            "headline_count",
            "summary_json",
            "raw_response",
            "referenced_articles",
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "elapsed_ms",
            "model_used",
        ]
        record = dict(zip(keys, row))
        record["referenced_articles"] = self._decode_json(record.get("referenced_articles"))
        record["summary_json"] = self._decode_json(record.get("summary_json"))
        return record

    def stats(self) -> Dict[str, object]:
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*) AS total, MAX(generated_at) AS latest FROM {schema}.{table}"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    )
                )
                row = cur.fetchone()
        total, latest = row if row else (0, None)
        return {"count": int(total or 0), "latest": latest}

    def list_summaries(self, limit: int = 20) -> List[Dict[str, object]]:
        limit_value = max(1, min(int(limit), 100))
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT summary_id, generated_at, window_start, window_end, headline_count, summary_json, "
                        "raw_response, referenced_articles, prompt_tokens, completion_tokens, total_tokens, elapsed_ms, model_used "
                        "FROM {schema}.{table} ORDER BY generated_at DESC LIMIT %s"
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (limit_value,),
                )
                rows = cur.fetchall()
        keys = [
            "summary_id",
            "generated_at",
            "window_start",
            "window_end",
            "headline_count",
            "summary_json",
            "raw_response",
            "referenced_articles",
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "elapsed_ms",
            "model_used",
        ]
        results = []
        for row in rows:
            record = dict(zip(keys, row))
            record["referenced_articles"] = self._decode_json(record.get("referenced_articles"))
            record["summary_json"] = self._decode_json(record.get("summary_json"))
            results.append(record)
        return results

    @staticmethod
    def _decode_json(value: Optional[str]) -> Optional[object]:
        if not value:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value


__all__ = ["NewsMarketInsightDAO"]
