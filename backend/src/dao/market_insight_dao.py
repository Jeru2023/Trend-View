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

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "market_insights_schema.sql"
STAGE_COLUMN_MAP = {
    "index_analysis": "index_stage",
    "fund_flow_analysis": "fund_stage",
    "sentiment_analysis": "sentiment_stage",
    "macro_analysis": "macro_stage",
    "news_analysis": "news_stage",
}


class MarketInsightDAO(PostgresDAOBase):
    """Persistence helper for market insight summaries."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "market_insight_table", "market_insights")
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
                        "raw_response, referenced_articles, prompt_tokens, completion_tokens, total_tokens, elapsed_ms, model_used, "
                        "index_stage, fund_stage, sentiment_stage, macro_stage, news_stage, comprehensive_stage "
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
            "index_stage",
            "fund_stage",
            "sentiment_stage",
            "macro_stage",
            "news_stage",
            "comprehensive_stage",
        ]
        record = dict(zip(keys, row))
        record["referenced_articles"] = self._decode_json(record.get("referenced_articles"))
        record["summary_json"] = self._decode_json(record.get("summary_json"))
        for column in (
            "index_stage",
            "fund_stage",
            "sentiment_stage",
            "macro_stage",
            "news_stage",
            "comprehensive_stage",
        ):
            record[column] = self._decode_json(record.get(column))
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
                        "raw_response, referenced_articles, prompt_tokens, completion_tokens, total_tokens, elapsed_ms, model_used, "
                        "index_stage, fund_stage, sentiment_stage, macro_stage, news_stage, comprehensive_stage "
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
            "index_stage",
            "fund_stage",
            "sentiment_stage",
            "macro_stage",
            "news_stage",
            "comprehensive_stage",
        ]
        results = []
        for row in rows:
            record = dict(zip(keys, row))
            record["referenced_articles"] = self._decode_json(record.get("referenced_articles"))
            record["summary_json"] = self._decode_json(record.get("summary_json"))
            for column in (
                "index_stage",
                "fund_stage",
                "sentiment_stage",
                "macro_stage",
                "news_stage",
                "comprehensive_stage",
            ):
                record[column] = self._decode_json(record.get(column))
            results.append(record)
        return results

    def update_stage(self, summary_id: str, stage_key: str, payload: Dict[str, object]) -> None:
        column = STAGE_COLUMN_MAP.get(stage_key)
        if column is None:
            raise ValueError(f"Unsupported stage key: {stage_key}")
        json_payload = json.dumps(payload, ensure_ascii=False)
        self._update_columns(summary_id, **{column: json_payload})

    def update_comprehensive(self, summary_id: str, payload: Dict[str, object]) -> None:
        json_payload = json.dumps(payload, ensure_ascii=False)
        self._update_columns(summary_id, comprehensive_stage=json_payload)

    def update_columns(self, summary_id: str, **columns) -> None:
        self._update_columns(summary_id, **columns)

    def _update_columns(self, summary_id: str, **columns) -> None:
        if not columns:
            return
        assignments = []
        values = []
        for name, value in columns.items():
            assignments.append(sql.SQL("{} = %s").format(sql.Identifier(name)))
            values.append(value)
        assignments.append(sql.SQL("updated_at = CURRENT_TIMESTAMP"))
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                query = sql.SQL("UPDATE {schema}.{table} SET {assignments} WHERE summary_id = %s").format(
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                    assignments=sql.SQL(", ").join(assignments),
                )
                cur.execute(query, (*values, summary_id))
            conn.commit()

    @staticmethod
    def _decode_json(value: Optional[str]) -> Optional[object]:
        if not value:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value


__all__ = ["MarketInsightDAO"]
