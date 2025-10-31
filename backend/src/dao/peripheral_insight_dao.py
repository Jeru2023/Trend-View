"""
DAO for storing aggregated peripheral market insights and LLM summaries.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from ..config.settings import PostgresSettings
from .base import PostgresDAOBase

SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "config" / "peripheral_insight_schema.sql"


class PeripheralInsightDAO(PostgresDAOBase):
    """Persistence helper for peripheral market insight snapshots."""

    def __init__(self, config: PostgresSettings, table_name: Optional[str] = None) -> None:
        super().__init__(config=config)
        self._table_name = table_name or getattr(config, "peripheral_insight_table", "peripheral_insights")
        self._schema_sql_template = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    def ensure_table(self, conn: PGConnection) -> None:
        self._execute_schema_template(
            conn,
            self._schema_sql_template,
            schema=self.config.schema,
            table=self._table_name,
        )
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (generated_at DESC)"
                ).format(
                    index=sql.Identifier(f"{self._table_name}_generated_at_idx"),
                    schema=sql.Identifier(self.config.schema),
                    table=sql.Identifier(self._table_name),
                )
            )

    def upsert_snapshot(
        self,
        snapshot_date: date,
        generated_at: datetime,
        metrics: Dict[str, object],
        summary: Optional[str],
        raw_response: Optional[str],
        model: Optional[str],
    ) -> None:
        payload = json.dumps(metrics, ensure_ascii=False)
        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.{table} (
                            snapshot_date,
                            generated_at,
                            metrics,
                            summary,
                            raw_response,
                            model
                        )
                        VALUES (%s, %s, %s::jsonb, %s, %s, %s)
                        ON CONFLICT (snapshot_date)
                        DO UPDATE
                        SET generated_at = EXCLUDED.generated_at,
                            metrics = EXCLUDED.metrics,
                            summary = EXCLUDED.summary,
                            raw_response = EXCLUDED.raw_response,
                            model = EXCLUDED.model,
                            updated_at = NOW()
                        """
                    ).format(
                        schema=sql.Identifier(self.config.schema),
                        table=sql.Identifier(self._table_name),
                    ),
                    (
                        snapshot_date,
                        generated_at,
                        payload,
                        summary,
                        raw_response,
                        model,
                    ),
                )

    def fetch_latest(self) -> Optional[Dict[str, object]]:
        query = sql.SQL(
            """
            SELECT snapshot_date,
                   generated_at,
                   metrics,
                   summary,
                   raw_response,
                   model,
                   created_at,
                   updated_at
            FROM {schema}.{table}
            ORDER BY snapshot_date DESC, generated_at DESC
            LIMIT 1
            """
        ).format(
            schema=sql.Identifier(self.config.schema),
            table=sql.Identifier(self._table_name),
        )

        with self.connect() as conn:
            self.ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
        if not row:
            return None

        (
            snapshot_date,
            generated_at,
            metrics,
            summary,
            raw_response,
            model,
            created_at,
            updated_at,
        ) = row
        return {
            "snapshot_date": snapshot_date,
            "generated_at": generated_at,
            "metrics": metrics,
            "summary": summary,
            "raw_response": raw_response,
            "model": model,
            "created_at": created_at,
            "updated_at": updated_at,
        }

    def stats(self) -> Dict[str, Optional[datetime]]:
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


__all__ = ["PeripheralInsightDAO"]
